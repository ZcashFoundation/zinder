use std::{ffi::OsString, fs, path::PathBuf};

use zinder_core::{Network, NetworkUpgradeActivationsFingerprint};

use crate::{
    BoundedRocksDbOpen, RocksDbIoMode, RocksDbOpenRole, RocksDbResourceBudget, open_bounded_rocksdb,
};

use super::{
    CanonicalStoreBuildError, CanonicalStoreBuildPlan, CanonicalStoreError, CanonicalStoreWorkload,
    block_load::{
        CANONICAL_BLOCK_SST_TARGET_LOGICAL_BYTES, CanonicalBlockLoadEvidence,
        CanonicalBlockSstConfig, CanonicalBuildBlock, CanonicalStagedSstEvidence,
        CanonicalTrustedFreshBlockEvidence, REVERSE_INDEX_SORT_MEMORY_BYTES,
        canonical_block_families_are_empty, ingest_canonical_block_ssts,
        validate_source_tip_checkpoint, write_canonical_block_ssts,
    },
    rocksdb::{
        canonical_column_family_descriptors, canonical_data_options, canonical_store_path,
        create_fresh_directory, initialize_store_identity, validate_resource_budget,
    },
    subtree_load::{
        CanonicalBuildSubtreeRoot, CanonicalSubtreeRootLoadEvidence,
        load_complete_subtree_root_prefix, load_subtree_roots, required_subtree_root_ranges,
    },
};

/// Exclusive owner of one fresh, unpublished canonical build.
///
/// A builder cannot open an existing path. Any crash residue or partially
/// populated family requires deletion of the whole build before retrying.
pub struct RocksDbCanonicalBuilder {
    pub(super) store_path: PathBuf,
    pub(super) bounded_open: BoundedRocksDbOpen,
    pub(super) resource_budget: RocksDbResourceBudget,
    pub(super) network: Network,
    pub(super) workload: CanonicalStoreWorkload,
    pub(super) build_plan: CanonicalStoreBuildPlan,
    pub(super) canonical_block_evidence: Option<CanonicalBlockLoadEvidence>,
    pub(super) canonical_block_staged_ssts: Option<Vec<CanonicalStagedSstEvidence>>,
    pub(super) trusted_fresh_block_evidence: Option<CanonicalTrustedFreshBlockEvidence>,
    pub(super) subtree_root_evidence: Option<CanonicalSubtreeRootLoadEvidence>,
    pub(super) trusted_fresh_subtree_family_evidence:
        Option<super::construction_manifest::CanonicalConstructionFamilyEvidence>,
    pub(super) confirmed_source_tip_checkpoint: Option<zinder_core::CommitmentTreeCheckpoint>,
    pub(super) cursor_auth_key: [u8; 32],
}

impl RocksDbCanonicalBuilder {
    /// Creates an unpublished canonical store at a path that does not exist.
    ///
    /// Identity, schema version, network, workload, raw-blob retention, source
    /// range, and cursor authentication material are synced before any data
    /// family is created.
    pub fn create_fresh(
        path: impl AsRef<std::path::Path>,
        workload: CanonicalStoreWorkload,
        build_plan: CanonicalStoreBuildPlan,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<Self, CanonicalStoreError> {
        let path = path.as_ref();
        let network = build_plan.network();
        validate_resource_budget(resource_budget)?;
        let mut cursor_auth_key = [0; 32];
        getrandom::fill(&mut cursor_auth_key)
            .map_err(|source| CanonicalStoreError::EntropyUnavailable { source })?;
        create_fresh_directory(path)?;
        let store_path = canonical_store_path(path)?;
        initialize_store_identity(path, workload, &build_plan, cursor_auth_key)?;
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Primary { path },
            resource_budget,
            canonical_column_family_descriptors,
        )
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "create builder",
            source,
        })?;
        Ok(Self {
            store_path,
            bounded_open,
            resource_budget,
            network,
            workload,
            build_plan,
            canonical_block_evidence: None,
            canonical_block_staged_ssts: None,
            trusted_fresh_block_evidence: None,
            subtree_root_evidence: None,
            trusted_fresh_subtree_family_evidence: None,
            confirmed_source_tip_checkpoint: None,
            cursor_auth_key,
        })
    }

    /// Discards the deterministic block-load staging sibling for a build that
    /// the caller has already proved is unpublished.
    ///
    /// This operation must not be used to infer whether a build is safe to
    /// discard. Runtime admission owns that decision; the store owns the
    /// staging-path contract and filesystem error mapping.
    pub fn discard_unpublished_block_load_staging(
        store_path: impl AsRef<std::path::Path>,
    ) -> Result<bool, CanonicalStoreError> {
        let staging_path = canonical_block_staging_path(store_path.as_ref());
        match fs::remove_dir_all(&staging_path) {
            Ok(()) => Ok(true),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(source) => Err(CanonicalStoreError::PathUnavailable {
                path: staging_path,
                source,
            }),
        }
    }

    /// Returns the immutable network persisted by this build.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.network
    }

    /// Returns the immutable consumer workload persisted by this build.
    #[must_use]
    pub const fn workload(&self) -> CanonicalStoreWorkload {
        self.workload
    }

    /// Returns the immutable raw-blob retention persisted by this build.
    #[must_use]
    pub const fn raw_blob_retention(&self) -> crate::RawBlobRetention {
        self.build_plan.raw_blob_retention()
    }

    /// Returns the immutable activation-table identity persisted by this build.
    #[must_use]
    pub const fn network_upgrade_activations_fingerprint(
        &self,
    ) -> NetworkUpgradeActivationsFingerprint {
        self.build_plan.network_upgrade_activations_fingerprint()
    }

    /// Returns the exact predecessor-to-tip source range persisted by this build.
    #[must_use]
    pub const fn build_plan(&self) -> &CanonicalStoreBuildPlan {
        &self.build_plan
    }

    /// Bulk-loads every source-derived canonical block family.
    ///
    /// All seven families are completely staged before any column family is
    /// ingested. Reverse indexes use fixed-record bounded sort runs and a
    /// capped merge fan-in; raw transaction bytes remain source-position keyed
    /// and never enter the random-key sort.
    /// The returned evidence describes the prepared rows accepted by `RocksDB`;
    /// READY publication additionally requires cache-bypassing family readback.
    pub fn bulk_load_blocks<SourceError>(
        &mut self,
        blocks: impl IntoIterator<Item = Result<CanonicalBuildBlock, SourceError>>,
    ) -> Result<CanonicalBlockLoadEvidence, CanonicalStoreBuildError<SourceError>> {
        self.bulk_load_blocks_with_limits(
            blocks,
            CANONICAL_BLOCK_SST_TARGET_LOGICAL_BYTES,
            REVERSE_INDEX_SORT_MEMORY_BYTES,
        )
    }

    pub(super) fn bulk_load_blocks_with_limits<SourceError>(
        &mut self,
        blocks: impl IntoIterator<Item = Result<CanonicalBuildBlock, SourceError>>,
        sst_target_logical_bytes: u64,
        reverse_index_sort_memory_bytes: usize,
    ) -> Result<CanonicalBlockLoadEvidence, CanonicalStoreBuildError<SourceError>> {
        if self.canonical_block_evidence.is_some()
            || !canonical_block_families_are_empty(&self.bounded_open.db)?
        {
            return Err(CanonicalStoreError::BlockLoadAlreadyLoaded.into());
        }
        let staging_path = canonical_block_staging_path(&self.store_path);
        let staging = FreshCanonicalBlockStaging::create(staging_path)?;
        let sst_options =
            canonical_data_options(&self.bounded_open.block_cache, self.resource_budget);
        let prepared = write_canonical_block_ssts(
            CanonicalBlockSstConfig {
                staging_path: staging.path(),
                options: &sst_options,
                workload: self.workload,
                build_plan: &self.build_plan,
                sst_target_logical_bytes,
                reverse_index_sort_memory_bytes,
            },
            blocks,
        )?;
        validate_block_load_range(&self.build_plan, &prepared.evidence)?;
        let ingested = ingest_canonical_block_ssts(&self.bounded_open.db, prepared)?;
        staging.remove()?;
        self.canonical_block_evidence = Some(ingested.evidence);
        self.canonical_block_staged_ssts = Some(ingested.staged_ssts);
        self.trusted_fresh_block_evidence = Some(ingested.trusted_fresh_evidence);
        Ok(ingested.evidence)
    }

    /// Returns the exact non-empty source ranges required after the block load.
    pub fn required_subtree_root_ranges(
        &self,
    ) -> Result<Vec<zinder_core::SubtreeRootRange>, CanonicalStoreError> {
        let block_evidence = self
            .canonical_block_evidence
            .as_ref()
            .ok_or(CanonicalStoreError::CanonicalBlocksNotLoaded)?;
        required_subtree_root_ranges(
            self.build_plan.history_predecessor().tip_metadata(),
            block_evidence.tip_metadata,
        )
    }

    /// Atomically loads every source-authenticated completed subtree root.
    pub fn load_subtree_roots(
        &mut self,
        subtree_roots: impl IntoIterator<Item = CanonicalBuildSubtreeRoot>,
    ) -> Result<CanonicalSubtreeRootLoadEvidence, CanonicalStoreError> {
        if self.subtree_root_evidence.is_some() {
            return Err(CanonicalStoreError::SubtreeRootLoadAlreadyLoaded);
        }
        let block_evidence = self
            .canonical_block_evidence
            .as_ref()
            .ok_or(CanonicalStoreError::CanonicalBlocksNotLoaded)?;
        let loaded = load_subtree_roots(
            &self.bounded_open.db,
            &self.build_plan,
            block_evidence,
            subtree_roots,
        )?;
        self.subtree_root_evidence = Some(loaded.evidence);
        self.trusted_fresh_subtree_family_evidence = Some(loaded.family_evidence);
        Ok(loaded.evidence)
    }

    /// Loads a caller-authenticated complete subtree-root prefix for a fresh build.
    ///
    /// Every protocol must cover exactly index zero through the completed count
    /// at the fixed tip. Roots completed before retained history carry their
    /// exact source-authenticated block identity; the store preserves that
    /// identity but does not claim to authenticate a block header it does not
    /// retain. Completion blocks inside retained history are checked against
    /// the retained canonical header before any rows are written.
    pub fn load_complete_subtree_root_prefix(
        &mut self,
        subtree_roots: impl IntoIterator<Item = zinder_core::SubtreeRootArtifact>,
    ) -> Result<CanonicalSubtreeRootLoadEvidence, CanonicalStoreError> {
        if self.subtree_root_evidence.is_some() {
            return Err(CanonicalStoreError::SubtreeRootLoadAlreadyLoaded);
        }
        let block_evidence = self
            .canonical_block_evidence
            .as_ref()
            .ok_or(CanonicalStoreError::CanonicalBlocksNotLoaded)?;
        let loaded = load_complete_subtree_root_prefix(
            &self.bounded_open.db,
            &self.build_plan,
            block_evidence,
            subtree_roots,
        )?;
        self.subtree_root_evidence = Some(loaded.evidence);
        self.trusted_fresh_subtree_family_evidence = Some(loaded.family_evidence);
        Ok(loaded.evidence)
    }

    /// Confirms that the persisted exact-tip frontier matches a final source observation.
    pub fn confirm_source_tip_checkpoint(
        &mut self,
        source_checkpoint: &zinder_core::CommitmentTreeCheckpoint,
    ) -> Result<(), CanonicalStoreError> {
        let block_evidence = self
            .canonical_block_evidence
            .as_ref()
            .ok_or(CanonicalStoreError::CanonicalBlocksNotLoaded)?;
        validate_source_tip_checkpoint(
            &self.bounded_open.db,
            &self.build_plan,
            block_evidence,
            source_checkpoint,
        )?;
        self.confirmed_source_tip_checkpoint = Some(source_checkpoint.clone());
        Ok(())
    }

    /// Returns whether the exact fixed tip was authenticated against the source.
    #[must_use]
    pub const fn is_source_tip_checkpoint_confirmed(&self) -> bool {
        self.confirmed_source_tip_checkpoint.is_some()
    }

    /// Returns the filesystem I/O mode selected by the bounded `RocksDB` open.
    #[must_use]
    pub const fn io_mode(&self) -> RocksDbIoMode {
        self.bounded_open.io_mode
    }
}

fn validate_block_load_range(
    build_plan: &CanonicalStoreBuildPlan,
    evidence: &CanonicalBlockLoadEvidence,
) -> Result<(), CanonicalStoreError> {
    let expected_first_height = build_plan.history_bounds().first_available_height();
    if evidence.first_height != expected_first_height {
        return Err(CanonicalStoreError::block_load_sequence(format!(
            "expected first available height {}, observed {}",
            expected_first_height.value(),
            evidence.first_height.value()
        )));
    }
    if evidence.first_parent_hash != build_plan.history_predecessor().block_id.hash {
        return Err(CanonicalStoreError::block_load_sequence(format!(
            "block {} parent does not match the persisted history predecessor",
            evidence.first_height.value()
        )));
    }
    let expected_tip = build_plan.build_tip();
    if evidence.tip_height != expected_tip.height || evidence.tip_hash != expected_tip.hash {
        return Err(CanonicalStoreError::block_load_sequence(format!(
            "expected build tip {expected_tip:?}, observed height {} hash {:?}",
            evidence.tip_height.value(),
            evidence.tip_hash
        )));
    }
    Ok(())
}

pub(super) fn canonical_block_staging_path(store_path: &std::path::Path) -> PathBuf {
    let mut staging_path = OsString::from(store_path.as_os_str());
    staging_path.push(".block-load-staging");
    PathBuf::from(staging_path)
}

struct FreshCanonicalBlockStaging {
    path: PathBuf,
    remove_on_drop: bool,
}

impl FreshCanonicalBlockStaging {
    fn create(path: PathBuf) -> Result<Self, CanonicalStoreError> {
        match fs::create_dir(&path) {
            Ok(()) => Ok(Self {
                path,
                remove_on_drop: true,
            }),
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(CanonicalStoreError::BlockLoadStagingExists { path })
            }
            Err(source) => Err(CanonicalStoreError::PathUnavailable { path, source }),
        }
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }

    fn remove(mut self) -> Result<(), CanonicalStoreError> {
        fs::remove_dir_all(&self.path).map_err(|source| CanonicalStoreError::PathUnavailable {
            path: self.path.clone(),
            source,
        })?;
        self.remove_on_drop = false;
        Ok(())
    }
}

impl Drop for FreshCanonicalBlockStaging {
    fn drop(&mut self) {
        if self.remove_on_drop {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        num::NonZeroU32,
        path::Path,
        process::{Command, Stdio},
    };

    use metrics_exporter_prometheus::PrometheusBuilder;
    use rust_rocksdb::DB;
    use tempfile::TempDir;
    use zinder_core::{
        BlockFinalNoteCommitmentRoots, BlockHash, BlockHeaderArtifact, BlockHeight, BlockId,
        CanonicalBlockFacts, CanonicalBlockFactsDigestVersion, CanonicalBlockFactsSequenceDigest,
        CanonicalBlockFactsSequenceDigestBuilder, CanonicalBlockFactsSequenceDigestVersion,
        CanonicalBlockReplayEnvelope, CanonicalBlockReplayFormatVersion, CanonicalHistoryBounds,
        CanonicalTransactionFacts, ChainEpochId, ChainTipMetadata, CompactBlockArtifact,
        CompactTransaction, CompactTransactionData, ConsensusBranchId,
        DisplacedBlockArchiveCoverage, FinalNoteCommitmentRoot, LockTime, NetworkUpgradeActivation,
        NetworkUpgradeActivations, PrivacyShape, SerializedBytesDigest, TransactionBlobArtifact,
        TransactionComponentCounts, TransactionId, TransactionIntrinsicValueBalances,
        TransactionLocation, TransactionPublicFacts, TransactionVersion,
        TransparentAddressScriptHash, TransparentOutputFact, UnixTimestampMillis,
        UnsupportedSection, encode_canonical_block_replay,
    };

    use super::*;
    use crate::canonical_store::{
        block_load::{canonical_block_families_are_empty, validate_compact_block_payload},
        block_replay::{BLOCK_REPLAY_COLUMN_FAMILY, validate_persisted_block_replays},
        construction_manifest::CANONICAL_CONSTRUCTION_MANIFEST_FILE_NAME,
        control::{decode_store_control, encode_ready_store_control},
        displaced_archive::{
            encode_test_archive_record, encode_test_archive_state, encode_test_event_context,
            encode_test_hash_pointer_rows, encode_test_order_key,
        },
        publication::{encode_live_chain_epoch, encode_live_event},
        rocksdb::{
            BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY,
            CANONICAL_DATA_COLUMN_FAMILIES, CHAIN_EPOCH_COLUMN_FAMILY, CHAIN_EVENT_COLUMN_FAMILY,
            COMPACT_BLOCK_COLUMN_FAMILY, DISPLACED_BLOCK_FACTS_COLUMN_FAMILY, STORE_CONTROL_KEY,
        },
    };
    use crate::{
        CanonicalEventFence, CanonicalReorgPolicy, CanonicalStoreBuildState, RawBlobRetention,
        RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    };

    const PUBLICATION_FAILPOINT_ENV: &str = "ZINDER_TEST_CANONICAL_PUBLICATION_FAILPOINT";
    const PUBLICATION_STORE_PATH_ENV: &str = "ZINDER_TEST_CANONICAL_PUBLICATION_STORE_PATH";
    const LIVE_COMMIT_FAILPOINT_ENV: &str = "ZINDER_TEST_CANONICAL_LIVE_COMMIT_FAILPOINT";
    const LIVE_COMMIT_STORE_PATH_ENV: &str = "ZINDER_TEST_CANONICAL_LIVE_COMMIT_STORE_PATH";
    const ACTIVATION_SCHEDULE_FAILPOINT_ENV: &str =
        "ZINDER_TEST_CANONICAL_ACTIVATION_SCHEDULE_FAILPOINT";
    const ACTIVATION_SCHEDULE_STORE_PATH_ENV: &str =
        "ZINDER_TEST_CANONICAL_ACTIVATION_SCHEDULE_STORE_PATH";

    #[test]
    fn builder_refuses_every_existing_path() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let error = RocksDbCanonicalBuilder::create_fresh(
            temporary.path(),
            CanonicalStoreWorkload::Wallet,
            complete_build_plan()?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("existing path should be rejected")?;
        assert!(matches!(error, CanonicalStoreError::PathNotFresh { .. }));
        Ok(())
    }

    #[test]
    fn unpublished_block_load_staging_discard_preserves_build_directory()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical.building");
        let marker_path = store_path.join("CURRENT");
        let block_load_staging_path = canonical_block_staging_path(&store_path);
        std::fs::create_dir(&store_path)?;
        std::fs::write(&marker_path, b"MANIFEST-000001\n")?;
        std::fs::create_dir(&block_load_staging_path)?;
        std::fs::write(block_load_staging_path.join("partial.sst"), b"partial")?;

        assert!(RocksDbCanonicalBuilder::discard_unpublished_block_load_staging(&store_path)?);
        assert!(store_path.exists());
        assert_eq!(std::fs::read(marker_path)?, b"MANIFEST-000001\n");
        assert!(!block_load_staging_path.exists());
        assert!(!RocksDbCanonicalBuilder::discard_unpublished_block_load_staging(&store_path)?);
        Ok(())
    }

    #[test]
    fn bulk_load_blocks_stages_and_ingests_every_wallet_block_family()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let mut store = create_building_store(&store_path, CanonicalHistoryBounds::complete())?;
        let blocks = vec![
            Ok::<_, std::io::Error>(canonical_build_block(
                BlockHeight::new(1),
                [1; 32],
                Network::ZcashTestnet.genesis_hash().as_bytes(),
            )),
            Ok(canonical_build_block(BlockHeight::new(2), [2; 32], [1; 32])),
        ];
        let mut blocks = blocks;
        if let Some(Ok(tip)) = blocks.last_mut() {
            add_tree_state_checkpoint(tip)?;
        }

        let evidence = store.bulk_load_blocks(blocks)?;

        assert_eq!(evidence.block_count, 2);
        assert_eq!(evidence.block_header_count, 2);
        assert_eq!(evidence.block_hash_index_count, 2);
        assert_eq!(evidence.block_replay_count, 2);
        assert_eq!(evidence.compact_block_count, 2);
        assert_eq!(evidence.transaction_location_count, 0);
        assert_eq!(evidence.transaction_blob_count, 0);
        assert_eq!(evidence.block_blob_count, 0);
        assert_eq!(evidence.tip_metadata, ChainTipMetadata::new(0, 0, 0));
        assert_eq!(evidence.tree_state_checkpoint_count, 2);
        Ok(())
    }

    #[test]
    fn subtree_root_load_requires_blocks_and_is_atomic_and_single_use()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let mut store = create_building_store(&store_path, CanonicalHistoryBounds::complete())?;
        assert!(matches!(
            store.required_subtree_root_ranges(),
            Err(CanonicalStoreError::CanonicalBlocksNotLoaded)
        ));

        let first = canonical_build_block(
            BlockHeight::new(1),
            [1; 32],
            Network::ZcashTestnet.genesis_hash().as_bytes(),
        );
        let mut tip = canonical_build_block(BlockHeight::new(2), [2; 32], [1; 32]);
        add_tree_state_checkpoint(&mut tip)?;
        store.bulk_load_blocks([Ok::<_, std::io::Error>(first), Ok(tip)])?;
        assert!(store.required_subtree_root_ranges()?.is_empty());

        let unexpected_root = CanonicalBuildSubtreeRoot {
            protocol: zinder_core::ShieldedProtocol::Sapling,
            subtree_index: zinder_core::SubtreeRootIndex::new(0),
            root_hash: zinder_core::SubtreeRootHash::from_bytes([7; 32]),
            completing_block_height: BlockHeight::new(1),
        };
        assert!(matches!(
            store.load_subtree_roots([unexpected_root]),
            Err(CanonicalStoreError::SubtreeRootSequenceInvalid { .. })
        ));

        let evidence = store.load_subtree_roots(std::iter::empty())?;
        assert_eq!(evidence.subtree_root_count, 0);
        assert_eq!(evidence.subtree_root_logical_bytes, 0);
        let wrong_tip = zinder_core::CommitmentTreeCheckpoint::new(
            BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([9; 32])),
            2,
            zinder_core::CommitmentTreeFrontiers::default(),
        );
        assert!(matches!(
            store.confirm_source_tip_checkpoint(&wrong_tip),
            Err(CanonicalStoreError::SourceTipCheckpointMismatch { .. })
        ));
        assert!(!store.is_source_tip_checkpoint_confirmed());
        let source_tip = zinder_core::CommitmentTreeCheckpoint::new(
            BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32])),
            2,
            crate::canonical_store::test_checkpoint_frontiers(
                &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
                BlockHeight::new(2),
            ),
        );
        store.confirm_source_tip_checkpoint(&source_tip)?;
        assert!(store.is_source_tip_checkpoint_confirmed());
        assert!(matches!(
            store.load_subtree_roots(std::iter::empty()),
            Err(CanonicalStoreError::SubtreeRootLoadAlreadyLoaded)
        ));
        Ok(())
    }

    #[test]
    fn publication_requires_every_authenticated_build_phase()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = create_building_store(&store_path, CanonicalHistoryBounds::complete())?;

        let error = store
            .prepare_cold_certified_publication()
            .err()
            .ok_or("an empty BUILDING store must not validate")?;

        assert!(matches!(
            error,
            CanonicalStoreError::PublicationRefused { .. }
        ));
        let error = RocksDbCanonicalStore::open_ready(
            &store_path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("failed validation must leave the store BUILDING")?;
        assert!(matches!(error, CanonicalStoreError::StoreNotReady { .. }));
        Ok(())
    }

    #[test]
    fn explorer_publication_waits_for_required_daily_family_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store = RocksDbCanonicalBuilder::create_fresh(
            temporary.path().join("explorer"),
            CanonicalStoreWorkload::Explorer,
            complete_build_plan()?,
            RocksDbResourceBudget::for_local_tests(),
        )?;

        let error = store
            .prepare_cold_certified_publication()
            .err()
            .ok_or("explorer READY must require daily value-pool evidence")?;

        assert!(error.to_string().contains("daily value-pool evidence"));
        Ok(())
    }

    #[test]
    fn cold_validation_publishes_and_reopens_exact_baseline()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = complete_loaded_builder(&store_path)?;

        let validated = store.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let published = validated.publish_baseline(publication)?;

        assert_ready_construction_manifest_binding(&store_path, &published)?;
        let manifest =
            fs::read_to_string(store_path.join("canonical-construction-manifest.v4.json"))?;
        assert!(manifest.contains("\"evidence_provenance\":\"cold-certification\""));

        assert_eq!(published.ready_evidence().visible_epoch.value(), 1);
        assert_eq!(published.ready_evidence().visible_block_count, 2);
        assert_eq!(
            published.sequence_checkpoint().through(),
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32]))
        );
        assert_eq!(published.sequence_checkpoint().retained_block_count(), 1);
        assert_eq!(published.displaced_block_count()?, 0);
        assert_eq!(published.displaced_block_archive_coverage()?, None);
        assert_eq!(
            published.ready_evidence().visible_tip.height,
            BlockHeight::new(2)
        );
        drop(published);

        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let mismatch = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(101)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("reorg-window mismatch must fail closed")?;
        assert!(
            mismatch
                .to_string()
                .contains("persisted reorg window 100 does not equal requested reorg window 101")
        );

        let reopened = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(reopened.ready_evidence().visible_epoch.value(), 1);
        assert_eq!(reopened.reorg_policy().reorg_window_blocks(), 100);
        assert_eq!(
            reopened.ready_evidence().visible_tip.hash,
            BlockHash::from_bytes([2; 32])
        );
        let replayed_blocks = reopened
            .scan_canonical_replay()?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(replayed_blocks.len(), 2);
        assert_eq!(
            replayed_blocks[0].facts().block_header.block_hash,
            BlockHash::from_bytes([1; 32])
        );
        assert_eq!(
            replayed_blocks[1].facts().block_header.block_hash,
            BlockHash::from_bytes([2; 32])
        );
        Ok(())
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one end-to-end proof keeps publication, fail-closed readmission, batch reads, and secondary admission together"
    )]
    fn wallet_all_retention_survives_publication_reopen_and_secondary_batch_reads()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let build_tip = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32]));
        let build_plan = CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            build_tip,
            RawBlobRetention::All,
            CanonicalReorgPolicy::new(100)?,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            &store_path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let first = canonical_build_block_with_block_blob(
            BlockHeight::new(1),
            [1; 32],
            Network::ZcashTestnet.genesis_hash().as_bytes(),
            vec![11],
        );
        let mut second =
            canonical_build_block_with_block_blob(BlockHeight::new(2), [2; 32], [1; 32], vec![22]);
        add_tree_state_checkpoint(&mut second)?;
        builder.bulk_load_blocks([
            Ok::<_, std::io::Error>(first),
            Ok::<_, std::io::Error>(second),
        ])?;
        builder.load_subtree_roots(std::iter::empty())?;
        builder.confirm_source_tip_checkpoint(&zinder_core::CommitmentTreeCheckpoint::new(
            build_tip,
            2,
            crate::canonical_store::test_checkpoint_frontiers(&activations, BlockHeight::new(2)),
        ))?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let published = validated.publish_baseline(publication)?;
        assert_eq!(published.raw_blob_retention(), RawBlobRetention::All);
        assert_eq!(
            published
                .block_blob_at(BlockHeight::new(1))?
                .ok_or("published block blob must be retained")?
                .raw_block_bytes,
            vec![11]
        );
        drop(published);

        let mismatch = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("raw blob retention mismatch must fail closed")?;
        assert!(mismatch.to_string().contains(
            "persisted raw blob retention all does not equal requested raw blob retention transactions"
        ));

        let reopened = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::All,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(
            reopened
                .block_blobs_in_range(zinder_core::BlockHeightRange::inclusive(
                    BlockHeight::new(1),
                    BlockHeight::new(2),
                ))?
                .into_iter()
                .map(|blob| blob.map(|blob| blob.raw_block_bytes))
                .collect::<Vec<_>>(),
            vec![Some(vec![11]), Some(vec![22])]
        );
        let secondary = RocksDbCanonicalSecondary::open_ready(
            &store_path,
            temporary.path().join("canonical-secondary"),
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::All,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(secondary.raw_blob_retention(), RawBlobRetention::All);
        assert_eq!(
            secondary
                .block_blob_at(BlockHeight::new(2))?
                .ok_or("secondary block blob must be retained")?
                .raw_block_bytes,
            vec![22]
        );
        Ok(())
    }

    #[test]
    fn compact_block_range_refuses_missing_interior_payload_or_header()
    -> Result<(), Box<dyn std::error::Error>> {
        for (family_name, expected_error) in [
            (
                COMPACT_BLOCK_COLUMN_FAMILY,
                "compact block at height 2 is absent",
            ),
            (BLOCK_HEADER_COLUMN_FAMILY, "compact block header is absent"),
        ] {
            let temporary = TempDir::new()?;
            let store_path = temporary.path().join("canonical");
            let store = published_loaded_store(&store_path, BlockHeight::new(3))?;
            let range =
                zinder_core::BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(3));

            let before = store.compact_blocks_in_range(range)?;
            assert_eq!(
                before
                    .iter()
                    .map(|block| block.height().value())
                    .collect::<Vec<_>>(),
                [1, 2, 3]
            );

            let family = store
                .bounded_open
                .db
                .cf_handle(family_name)
                .ok_or("compact-block test family is absent")?;
            store.bounded_open.db.delete_cf(
                &family,
                crate::canonical_store::block_load::encode_block_position(BlockHeight::new(2)),
            )?;

            let error = store
                .compact_blocks_in_range(range)
                .err()
                .ok_or("an interior compact-block gap must fail closed")?;
            assert!(
                error.to_string().contains(expected_error),
                "unexpected missing-row error: {error}"
            );
        }
        Ok(())
    }

    #[test]
    fn trusted_fresh_writer_publishes_without_cold_family_certification()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let builder = complete_loaded_builder(&store_path)?;
        let recorder = PrometheusBuilder::new().build_recorder();
        let recorder_handle = recorder.handle();

        let published = metrics::with_local_recorder(&recorder, || {
            let validated = builder.prepare_trusted_fresh_publication()?;
            let publication =
                validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
                    BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                    zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
                ))?;
            validated.publish_baseline(publication)
        })?;

        assert_ready_construction_manifest_binding(&store_path, &published)?;
        assert!(
            !recorder_handle
                .render()
                .contains("zinder_store_canonical_publication_family_scan")
        );
        let manifest =
            fs::read_to_string(store_path.join("canonical-construction-manifest.v4.json"))?;
        assert!(manifest.contains("\"evidence_provenance\":\"trusted-fresh-writer\""));
        assert_eq!(published.ready_evidence().visible_block_count, 2);
        assert_eq!(
            published.ready_evidence().visible_tip.height,
            BlockHeight::new(2)
        );
        let missing_subtree_root = published.subtree_roots(zinder_core::SubtreeRootRange::new(
            zinder_core::ShieldedProtocol::Sapling,
            zinder_core::SubtreeRootIndex::new(0),
            std::num::NonZeroU32::new(1).ok_or("subtree range must be nonzero")?,
        ));
        assert!(missing_subtree_root.is_err());
        Ok(())
    }

    #[test]
    fn ready_admission_fails_closed_on_missing_or_tampered_construction_manifest()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let validated =
            complete_loaded_builder(&store_path)?.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let published = validated.publish_baseline(publication)?;
        let binding = published.ready_evidence().construction_manifest_sha256;
        drop(published);

        let manifest_path = store_path.join("canonical-construction-manifest.v4.json");
        std::fs::remove_file(&manifest_path)?;
        std::fs::write(
            store_path.join("canonical-construction-manifest.v1.json"),
            b"{}",
        )?;
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let missing = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("READY admission must reject a missing construction manifest")?;
        assert!(
            missing
                .to_string()
                .contains("canonical-construction-manifest.v4.json")
        );

        std::fs::write(&manifest_path, b"{}")?;
        let tampered = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("READY admission must reject a tampered construction manifest")?;
        assert!(tampered.to_string().contains("construction manifest"));
        let secondary_tampered = RocksDbCanonicalSecondary::open_ready(
            &store_path,
            temporary.path().join("canonical-secondary"),
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("secondary admission must reject a tampered construction manifest")?;
        assert!(
            secondary_tampered
                .to_string()
                .contains("construction manifest")
        );
        assert_ne!(binding, [0; 32]);
        Ok(())
    }

    #[test]
    fn displaced_archive_public_reads_derive_facts_and_preserve_exact_newest_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = published_store_with_displaced_archive(&store_path)?;
        for family_name in [CHAIN_EVENT_COLUMN_FAMILY, CHAIN_EPOCH_COLUMN_FAMILY] {
            let family = store
                .bounded_open
                .db
                .cf_handle(family_name)
                .ok_or("canonical transition family absent")?;
            store
                .bounded_open
                .db
                .delete_cf(&family, 2_u64.to_be_bytes())?;
        }
        store.bounded_open.db.flush_wal(true)?;
        let coverage = DisplacedBlockArchiveCoverage {
            activation_event_sequence: 2,
            activation_epoch: ChainEpochId::new(2),
            activated_at: UnixTimestampMillis::new(1_750_000_000_002),
        };

        assert_eq!(store.displaced_block_count()?, 2);
        assert_eq!(store.displaced_block_archive_coverage()?, Some(coverage));

        let page = store.displaced_block_page(None, NonZeroU32::new(1).ok_or("zero limit")?)?;
        assert_eq!(page.blocks.len(), 1);
        assert!(page.has_more);
        let newest = &page.blocks[0];
        assert_eq!(newest.header.height, BlockHeight::new(2));
        assert_eq!(newest.block_hash, BlockHash::from_bytes([22; 32]));
        assert_eq!(
            newest.transaction_ids,
            vec![TransactionId::from_bytes([9; 32])]
        );
        assert_eq!(newest.coinbase_outputs.len(), 1);
        assert_eq!(newest.coinbase_outputs[0].output_index, 0);
        assert_eq!(newest.coinbase_outputs[0].value_zat, 42);
        assert_eq!(newest.coinbase_outputs[0].script_pub_key, vec![0x51]);
        assert_eq!(newest.raw_block_bytes, Some(vec![6, 7, 8]));
        assert_eq!(
            newest
                .final_note_commitment_roots
                .ok_or("final roots absent")?
                .sapling,
            Some(FinalNoteCommitmentRoot::from_bytes([31; 32]))
        );

        let cursor = page.next_cursor.ok_or("lookahead page cursor absent")?;
        let older =
            store.displaced_block_page(Some(&cursor), NonZeroU32::new(1).ok_or("zero limit")?)?;
        assert_eq!(older.blocks.len(), 1);
        assert_eq!(older.blocks[0].header.height, BlockHeight::new(1));
        assert!(!older.has_more);
        assert!(older.next_cursor.is_none());

        assert_eq!(
            store
                .displaced_block_by_hash(BlockHash::from_bytes([11; 32]))?
                .ok_or("hash detail absent")?
                .header
                .height,
            BlockHeight::new(1)
        );
        let event = store.displaced_blocks_for_event(2, NonZeroU32::new(1).ok_or("zero limit")?)?;
        assert_eq!(event.len(), 1);
        assert_eq!(event[0].header.height, BlockHeight::new(2));
        assert!(
            store
                .newest_displaced_blocks(NonZeroU32::new(4_097).ok_or("zero oversized limit")?)
                .is_err()
        );
        Ok(())
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "the corruption matrix keeps independent fail-closed archive mutations together"
    )]
    fn displaced_archive_public_reads_fail_closed_on_pointer_and_raw_corruption()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = published_store_with_displaced_archive(&store_path)?;
        let archive_family = store
            .bounded_open
            .db
            .cf_handle(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
            .ok_or("archive family absent")?;
        let (pointer_key, original_pointer) =
            encode_test_hash_pointer_rows(2, BlockHeight::new(1), BlockHash::from_bytes([11; 32]));
        let (_, wrong_pointer) =
            encode_test_hash_pointer_rows(2, BlockHeight::new(2), BlockHash::from_bytes([11; 32]));
        store
            .bounded_open
            .db
            .put_cf(&archive_family, &pointer_key, wrong_pointer)?;
        let pointer_error = store
            .displaced_block_by_hash(BlockHash::from_bytes([11; 32]))
            .err()
            .ok_or("mislinked hash pointer must fail")?;
        assert!(pointer_error.to_string().contains("pointer"));
        store
            .bounded_open
            .db
            .put_cf(&archive_family, &pointer_key, original_pointer)?;

        let event_range =
            zinder_core::BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2));
        let (context_key, original_context) = encode_test_event_context(
            2,
            event_range,
            ChainEpochId::new(2),
            UnixTimestampMillis::new(1_750_000_000_002),
            2,
        );
        store
            .bounded_open
            .db
            .delete_cf(&archive_family, &context_key)?;
        let context_error = store
            .displaced_block_archive_coverage()
            .err()
            .ok_or("missing event context must fail")?;
        assert!(context_error.to_string().contains("context row is absent"));
        store
            .bounded_open
            .db
            .put_cf(&archive_family, &context_key, &original_context)?;

        let (_, swapped_context) = encode_test_event_context(
            3,
            event_range,
            ChainEpochId::new(3),
            UnixTimestampMillis::new(1_750_000_000_002),
            2,
        );
        store
            .bounded_open
            .db
            .put_cf(&archive_family, &context_key, swapped_context)?;
        let swapped_error = store
            .displaced_block_archive_coverage()
            .err()
            .ok_or("context moved under a different sequence must fail")?;
        assert!(swapped_error.to_string().contains("matching nonzero"));
        store
            .bounded_open
            .db
            .put_cf(&archive_family, &context_key, &original_context)?;

        let coverage = DisplacedBlockArchiveCoverage {
            activation_event_sequence: 2,
            activation_epoch: ChainEpochId::new(2),
            activated_at: UnixTimestampMillis::new(1_750_000_000_002),
        };
        let original_state = encode_test_archive_state(coverage, 2);
        store.bounded_open.db.put_cf(
            &archive_family,
            [0x00],
            encode_test_archive_state(coverage, 1),
        )?;
        let state_error = store
            .displaced_block_count()
            .err()
            .ok_or("state count below activation rows must fail")?;
        assert!(state_error.to_string().contains("smaller"));
        store
            .bounded_open
            .db
            .put_cf(&archive_family, [0x00], original_state)?;

        let first_key =
            encode_test_order_key(2, BlockHeight::new(1), BlockHash::from_bytes([11; 32]));
        store
            .bounded_open
            .db
            .delete_cf(&archive_family, &first_key)?;
        let range_error = store
            .displaced_blocks_for_event(2, NonZeroU32::new(2).ok_or("zero limit")?)
            .err()
            .ok_or("missing exact event row must fail")?;
        assert!(range_error.to_string().contains("exactly cover"));
        store.bounded_open.db.put_cf(
            &archive_family,
            first_key,
            encode_test_archive_record(
                displaced_archive_block_one()?,
                ChainEpochId::new(2),
                UnixTimestampMillis::new(1_750_000_000_002),
                None,
                None,
            )?,
        )?;

        let extra_replay =
            displaced_archive_coinbase_replay(BlockHeight::new(3), [33; 32], [22; 32])?;
        let extra_key =
            encode_test_order_key(2, BlockHeight::new(3), BlockHash::from_bytes([33; 32]));
        store.bounded_open.db.put_cf(
            &archive_family,
            &extra_key,
            encode_test_archive_record(
                extra_replay,
                ChainEpochId::new(2),
                UnixTimestampMillis::new(1_750_000_000_002),
                None,
                None,
            )?,
        )?;
        let extra_error = store
            .displaced_blocks_for_event(2, NonZeroU32::new(2).ok_or("zero limit")?)
            .err()
            .ok_or("extra event row above the reverted range must fail")?;
        assert!(extra_error.to_string().contains("canonical reorg event"));
        store
            .bounded_open
            .db
            .delete_cf(&archive_family, extra_key)?;

        let (raw_mismatch_replay, _) = displaced_archive_block_two()?;
        let roots = displaced_archive_roots();
        let second_key =
            encode_test_order_key(2, BlockHeight::new(2), BlockHash::from_bytes([22; 32]));
        store.bounded_open.db.put_cf(
            &archive_family,
            &second_key,
            encode_test_archive_record(
                raw_mismatch_replay,
                ChainEpochId::new(2),
                UnixTimestampMillis::new(1_750_000_000_002),
                Some(vec![0]),
                Some(roots),
            )?,
        )?;
        let raw_error = store
            .newest_displaced_blocks(NonZeroU32::new(2).ok_or("zero limit")?)
            .err()
            .ok_or("raw digest mismatch must fail")?;
        assert!(raw_error.to_string().contains("raw block bytes"));
        let (second_replay, second_raw_block_bytes) = displaced_archive_block_two()?;
        store.bounded_open.db.put_cf(
            &archive_family,
            second_key,
            encode_test_archive_record(
                second_replay,
                ChainEpochId::new(2),
                UnixTimestampMillis::new(1_750_000_000_002),
                Some(second_raw_block_bytes),
                Some(roots),
            )?,
        )?;

        overwrite_second_archive_replay(
            &store,
            replay(BlockHeight::new(2), [22; 32], [11; 32]).into_bytes(),
        )?;
        let zero_coinbase_error = store
            .newest_displaced_blocks(NonZeroU32::new(1).ok_or("zero limit")?)
            .err()
            .ok_or("archive replay without coinbase must fail")?;
        assert!(
            zero_coinbase_error
                .to_string()
                .contains("no first coinbase")
        );

        overwrite_second_archive_replay(&store, displaced_archive_late_coinbase_replay()?)?;
        let late_coinbase_error = store
            .newest_displaced_blocks(NonZeroU32::new(1).ok_or("zero limit")?)
            .err()
            .ok_or("archive replay with a late coinbase must fail")?;
        assert!(late_coinbase_error.to_string().contains("tx[0]"));
        Ok(())
    }

    #[test]
    fn ready_store_reads_history_predecessor_without_a_retained_header()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let validated =
            complete_loaded_builder(&store_path)?.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let store = validated.publish_baseline(publication)?;
        let predecessor = store.build_plan().history_predecessor();

        assert_eq!(store.block_header_at(predecessor.block_id.height)?, None);
        assert_eq!(
            store.tree_state_checkpoint_at_or_before(BlockHeight::new(1))?,
            Some(predecessor.clone())
        );
        Ok(())
    }

    #[test]
    fn ready_store_atomically_appends_one_live_canonical_epoch_and_reopens()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let validated =
            complete_loaded_builder(&store_path)?.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let mut store = validated.publish_baseline(publication)?;
        let mut live_block = canonical_build_block(BlockHeight::new(3), [3; 32], [2; 32]);
        add_tree_state_checkpoint(&mut live_block)?;
        let expected_fence = store.event_fence();
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;

        let (next_store, outcome) = store.commit_live_append(
            crate::CanonicalLiveAppend::new(
                expected_fence,
                live_block,
                Vec::new(),
                BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                zinder_core::UnixTimestampMillis::new(1_750_000_001_000),
            ),
            &activations,
        )?;
        store = next_store;

        assert_eq!(outcome.chain_epoch_id(), zinder_core::ChainEpochId::new(2));
        assert_eq!(outcome.chain_event_sequence(), 2);
        assert_eq!(outcome.visible_tip().height, BlockHeight::new(3));
        assert_eq!(outcome.sequence_digest().block_count(), 3);
        assert_ne!(
            outcome.sequence_digest().as_bytes(),
            expected_fence.sequence_digest().as_bytes()
        );
        drop(store);

        let reopened = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(
            reopened.ready_evidence().visible_epoch,
            outcome.chain_epoch_id()
        );
        assert_eq!(canonical_replay_count(&reopened)?, 3);
        let expected_fence = reopened.event_fence();
        let mut block_four = canonical_build_block(BlockHeight::new(4), [4; 32], [3; 32]);
        add_tree_state_checkpoint(&mut block_four)?;
        let (store, second_outcome) = reopened.commit_live_append(
            crate::CanonicalLiveAppend::new(
                expected_fence,
                block_four,
                Vec::new(),
                BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                zinder_core::UnixTimestampMillis::new(1_750_000_002_000),
            ),
            &activations,
        )?;
        assert_eq!(second_outcome.chain_epoch_id(), ChainEpochId::new(3));
        assert_eq!(second_outcome.sequence_digest().block_count(), 4);
        drop(store);
        let reopened = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(canonical_replay_count(&reopened)?, 4);
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one durable store lifecycle verifies schedule admission across append, checkpoint, and secondary convergence"
    )]
    fn activation_schedule_admission_is_future_only_journaled_and_survives_append()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let validated =
            complete_loaded_builder_with_reorg_window(&store_path, 100, BlockHeight::new(10))?
                .prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let mut store = validated.publish_baseline(publication)?;
        let baseline =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let mut unprovable_entries = baseline.activations().to_vec();
        unprovable_entries[0].name = "Changed historical upgrade".to_owned();
        let unprovable = NetworkUpgradeActivations::new(Network::ZcashTestnet, unprovable_entries)?;
        let error = store
            .admit_network_upgrade_schedule(&unprovable)
            .err()
            .ok_or("legacy schedule without a matching prefix proof must be rejected")?;
        assert!(error.to_string().contains("exact prefix"));
        assert_eq!(
            store
                .network_upgrade_schedule()
                .map(crate::CanonicalActivationSchedule::revision),
            None
        );

        let mut extended_entries = baseline.activations().to_vec();
        extended_entries.push(NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(99),
            activation_height: BlockHeight::new(11),
            name: "Future".to_owned(),
        });
        extended_entries.push(NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(100),
            activation_height: BlockHeight::new(12),
            name: "Future after append".to_owned(),
        });
        let extended = NetworkUpgradeActivations::new(Network::ZcashTestnet, extended_entries)?;

        store.admit_network_upgrade_schedule(&extended)?;
        assert_eq!(
            store
                .network_upgrade_schedule()
                .map(crate::CanonicalActivationSchedule::revision),
            Some(1)
        );
        let mut secondary = RocksDbCanonicalSecondary::open_ready(
            &store_path,
            temporary.path().join("canonical-secondary"),
            &baseline,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(
            secondary
                .network_upgrade_schedule()
                .map(crate::CanonicalActivationSchedule::revision),
            Some(1)
        );
        assert_eq!(secondary.network_upgrade_activations(), Some(&extended));
        let expected_fence = store.event_fence();
        let mut block = canonical_build_block(BlockHeight::new(11), [11; 32], [10; 32]);
        add_tree_state_checkpoint(&mut block)?;
        let (store, _) = store.commit_live_append(
            crate::CanonicalLiveAppend::new(
                expected_fence,
                block,
                Vec::new(),
                BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                UnixTimestampMillis::new(1_750_000_001_000),
            ),
            &extended,
        )?;
        drop(store);

        let mut store = RocksDbCanonicalStore::open_ready(
            &store_path,
            &extended,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(
            store
                .network_upgrade_schedule()
                .map(crate::CanonicalActivationSchedule::revision),
            Some(1)
        );
        assert_eq!(store.network_upgrade_activations(), Some(&extended));
        let mut changed_at_new_tip = extended.activations().to_vec();
        let boundary_activation = changed_at_new_tip
            .iter_mut()
            .find(|activation| activation.branch_id == ConsensusBranchId::new(99))
            .ok_or("height-eleven activation must remain in the schedule")?;
        boundary_activation.name = "Changed after reaching activation height".to_owned();
        let changed_at_new_tip =
            NetworkUpgradeActivations::new(Network::ZcashTestnet, changed_at_new_tip)?;
        let error = store
            .admit_network_upgrade_schedule(&changed_at_new_tip)
            .err()
            .ok_or("a schedule change at the locked tip must be refused")?;
        assert!(error.to_string().contains("at or below canonical tip"));
        let checkpoint_path = temporary.path().join("canonical-checkpoint");
        let checkpoint_admission = store.create_owner_checkpoint_physical(&checkpoint_path)?;
        let checkpoint_evidence = RocksDbCanonicalStore::cold_admit_owner_checkpoint(
            &checkpoint_path,
            &checkpoint_admission,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let checkpoint_schedule = checkpoint_evidence
            .activation_schedule
            .as_ref()
            .ok_or("cold checkpoint must expose its admitted schedule")?;
        assert_eq!(checkpoint_schedule.revision(), 1);
        assert_eq!(checkpoint_schedule.activations(), &extended);

        let mut revised_entries = extended.activations().to_vec();
        let revised_future = revised_entries
            .iter_mut()
            .find(|activation| activation.branch_id == ConsensusBranchId::new(100))
            .ok_or("extended schedule should have a future activation")?;
        revised_future.activation_height = BlockHeight::new(13);
        revised_future.name = "Future revised".to_owned();
        let revised = NetworkUpgradeActivations::new(Network::ZcashTestnet, revised_entries)?;
        store.admit_network_upgrade_schedule(&revised)?;
        assert_eq!(
            store
                .network_upgrade_schedule()
                .map(crate::CanonicalActivationSchedule::revision),
            Some(2)
        );
        secondary.try_catch_up()?;
        assert_eq!(
            secondary
                .network_upgrade_schedule()
                .map(crate::CanonicalActivationSchedule::revision),
            Some(2)
        );
        assert_eq!(secondary.network_upgrade_activations(), Some(&revised));
        let old_checkpoint_evidence = RocksDbCanonicalStore::cold_admit_owner_checkpoint(
            &checkpoint_path,
            &checkpoint_admission,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(
            old_checkpoint_evidence
                .activation_schedule
                .as_ref()
                .map(crate::CanonicalActivationSchedule::revision),
            Some(1)
        );

        let mut historical_entries = revised.activations().to_vec();
        historical_entries[0].name = "Changed history".to_owned();
        let historical_edit =
            NetworkUpgradeActivations::new(Network::ZcashTestnet, historical_entries)?;
        let error = store
            .admit_network_upgrade_schedule(&historical_edit)
            .err()
            .ok_or("historical activation edit must be rejected")?;
        assert!(error.to_string().contains("at or below canonical tip"));
        drop(store);

        let reopened = RocksDbCanonicalStore::open_ready(
            &store_path,
            &revised,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(
            reopened
                .network_upgrade_schedule()
                .map(crate::CanonicalActivationSchedule::revision),
            Some(2)
        );
        assert_eq!(reopened.network_upgrade_activations(), Some(&revised));
        Ok(())
    }

    #[test]
    fn activation_schedule_crash_recovers_before_or_after_atomic_record_and_control_write()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let baseline =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let mut extended_entries = baseline.activations().to_vec();
        extended_entries.push(NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(99),
            activation_height: BlockHeight::new(11),
            name: "Future".to_owned(),
        });
        let extended = NetworkUpgradeActivations::new(Network::ZcashTestnet, extended_entries)?;
        drop(published_loaded_store(&store_path, BlockHeight::new(2))?);

        run_activation_schedule_crash_child(&store_path, "before_atomic_write")?;
        let before = RocksDbCanonicalStore::open_ready(
            &store_path,
            &baseline,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(
            before
                .network_upgrade_schedule()
                .map(crate::CanonicalActivationSchedule::revision),
            None
        );
        drop(before);

        run_activation_schedule_crash_child(&store_path, "after_atomic_write")?;
        let after = RocksDbCanonicalStore::open_ready(
            &store_path,
            &baseline,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(
            after
                .network_upgrade_schedule()
                .map(crate::CanonicalActivationSchedule::revision),
            Some(1)
        );
        assert_eq!(after.network_upgrade_activations(), Some(&extended));
        Ok(())
    }

    #[test]
    fn settled_sequence_checkpoint_advances_with_window_two_and_resumes_after_reopen()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let validated =
            complete_loaded_builder_with_reorg_window(&store_path, 2, BlockHeight::new(2))?
                .prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let mut store = validated.publish_baseline(publication)?;
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;

        for (height, settled_height) in [(3_u32, 1_u32), (4, 2), (5, 3)] {
            let height_byte = u8::try_from(height)?;
            let parent_byte = u8::try_from(height.saturating_sub(1))?;
            let mut block = canonical_build_block(
                BlockHeight::new(height),
                [height_byte; 32],
                [parent_byte; 32],
            );
            add_tree_state_checkpoint(&mut block)?;
            let expected_fence = store.event_fence();
            let (next, _) = store.commit_live_append(
                crate::CanonicalLiveAppend::new(
                    expected_fence,
                    block,
                    Vec::new(),
                    BlockId::new(
                        BlockHeight::new(settled_height),
                        BlockHash::from_bytes([u8::try_from(settled_height)?; 32]),
                    ),
                    zinder_core::UnixTimestampMillis::new(1_750_000_000_000 + u64::from(height)),
                ),
                &activations,
            )?;
            store = next;
        }
        assert_eq!(
            store.sequence_checkpoint().through(),
            BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([3; 32]))
        );
        assert_eq!(store.sequence_checkpoint().retained_block_count(), 3);
        drop(store);
        assert_reopened_window_two_checkpoint(&store_path, &activations)?;
        Ok(())
    }

    #[test]
    fn live_append_rejects_a_hash_change_at_the_unchanged_settled_height()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let validated =
            complete_loaded_builder_with_reorg_window(&store_path, 2, BlockHeight::new(2))?
                .prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let store = validated.publish_baseline(publication)?;
        let expected_fence = store.event_fence();
        let mut block = canonical_build_block(BlockHeight::new(3), [3; 32], [2; 32]);
        add_tree_state_checkpoint(&mut block)?;
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;

        let error = store
            .commit_live_append(
                crate::CanonicalLiveAppend::new(
                    expected_fence,
                    block,
                    Vec::new(),
                    BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([9; 32])),
                    zinder_core::UnixTimestampMillis::new(1_750_000_000_003),
                ),
                &activations,
            )
            .err()
            .ok_or("unchanged settled height with a different hash must fail")?;
        assert!(error.to_string().contains("retain its canonical hash"));

        let reopened = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(2)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(reopened.event_fence(), expected_fence);
        Ok(())
    }

    #[test]
    fn baseline_rejects_settlement_beyond_the_reorg_window_without_rebuilding()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let validated =
            complete_loaded_builder_with_reorg_window(&store_path, 1, BlockHeight::new(3))?
                .prepare_cold_certified_publication()?;

        let error = validated
            .prepare_baseline(crate::CanonicalBaselinePublication::new(
                BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
            ))
            .err()
            .ok_or("baseline settlement beyond the reorg window must fail")?;

        assert!(error.to_string().contains("reorg window is 1"));
        Ok(())
    }

    #[test]
    fn checkpointed_history_sequence_checkpoint_counts_only_retained_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let predecessor = BlockId::new(BlockHeight::new(99), BlockHash::from_bytes([9; 32]));
        let mut builder = create_building_store(
            &store_path,
            CanonicalHistoryBounds::checkpointed(predecessor)?,
        )?;
        let mut block = canonical_build_block(BlockHeight::new(100), [10; 32], [9; 32]);
        add_tree_state_checkpoint(&mut block)?;
        let expected_replay_bytes = u64::try_from(block.replay_envelope.as_bytes().len())?;
        builder.bulk_load_blocks([Ok::<_, std::io::Error>(block)])?;
        builder.load_subtree_roots(std::iter::empty())?;
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        builder.confirm_source_tip_checkpoint(&zinder_core::CommitmentTreeCheckpoint::new(
            BlockId::new(BlockHeight::new(100), BlockHash::from_bytes([10; 32])),
            100,
            crate::canonical_store::test_checkpoint_frontiers(&activations, BlockHeight::new(100)),
        ))?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(100), BlockHash::from_bytes([10; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let store = validated.publish_baseline(publication)?;

        assert_eq!(store.sequence_checkpoint().retained_block_count(), 1);
        assert_eq!(
            store.sequence_checkpoint().logical_replay_bytes(),
            expected_replay_bytes
        );
        Ok(())
    }

    #[test]
    fn ready_open_bounded_proof_rejects_every_checkpoint_field_corruption()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        for corruption in 0_u8..4 {
            let store_path = temporary.path().join(format!("canonical-{corruption}"));
            let validated =
                complete_loaded_builder_with_reorg_window(&store_path, 2, BlockHeight::new(2))?
                    .prepare_cold_certified_publication()?;
            let publication =
                validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
                    BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                    zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
                ))?;
            drop(validated.publish_baseline(publication)?);

            rewrite_corrupted_sequence_checkpoint(&store_path, corruption)?;

            assert!(
                RocksDbCanonicalStore::open_ready(
                    &store_path,
                    &activations,
                    CanonicalStoreWorkload::Wallet,
                    RawBlobRetention::Transactions,
                    CanonicalReorgPolicy::new(2)?,
                    RocksDbResourceBudget::for_local_tests(),
                )
                .is_err(),
                "checkpoint corruption {corruption} must fail admission"
            );
        }
        Ok(())
    }

    #[test]
    fn ready_store_append_anchor_survives_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let validated =
            complete_loaded_builder(&store_path)?.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let store = validated.publish_baseline(publication)?;
        let expected_anchor = store.append_anchor()?;
        assert_eq!(expected_anchor.event_fence(), store.event_fence());
        assert_eq!(
            expected_anchor.tip_checkpoint().block_id,
            store.event_fence().visible_tip()
        );
        drop(store);

        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let reopened = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(reopened.append_anchor()?, expected_anchor);
        Ok(())
    }

    #[test]
    fn ready_store_rejects_incomplete_live_wallet_artifacts_without_advancing()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = published_loaded_store(&store_path, BlockHeight::new(2))?;
        let expected_fence = store.event_fence();
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let error = store
            .commit_live_append(
                crate::CanonicalLiveAppend::new(
                    expected_fence,
                    canonical_build_block(BlockHeight::new(3), [3; 32], [2; 32]),
                    Vec::new(),
                    BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                    zinder_core::UnixTimestampMillis::new(1_750_000_001_000),
                ),
                &activations,
            )
            .err()
            .ok_or("a live tip without its tree state must be rejected")?;
        assert!(matches!(
            error,
            CanonicalStoreError::BlockLoadSequenceInvalid { .. }
        ));

        let store = RocksDbCanonicalStore::open_ready(
            &store_path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let mut block_three = canonical_build_block(BlockHeight::new(3), [3; 32], [2; 32]);
        add_tree_state_checkpoint(&mut block_three)?;
        let expected_fence = store.event_fence();
        let unexpected_root = CanonicalBuildSubtreeRoot {
            protocol: zinder_core::ShieldedProtocol::Sapling,
            subtree_index: zinder_core::SubtreeRootIndex::new(0),
            root_hash: zinder_core::SubtreeRootHash::from_bytes([7; 32]),
            completing_block_height: BlockHeight::new(3),
        };
        let error = store
            .commit_live_append(
                crate::CanonicalLiveAppend::new(
                    expected_fence,
                    block_three,
                    vec![unexpected_root],
                    BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                    zinder_core::UnixTimestampMillis::new(1_750_000_001_000),
                ),
                &activations,
            )
            .err()
            .ok_or("an unexpected live subtree root must be rejected")?;
        assert!(matches!(
            error,
            CanonicalStoreError::LiveCommitRefused { .. }
        ));

        let reopened = RocksDbCanonicalStore::open_ready(
            &store_path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(
            reopened.ready_evidence().visible_epoch,
            ChainEpochId::new(1)
        );
        assert_eq!(
            reopened
                .scan_canonical_replay()?
                .collect::<Result<Vec<_>, _>>()?
                .len(),
            2
        );
        Ok(())
    }

    #[test]
    fn ready_store_rejects_disconnected_live_append_without_advancing()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let validated =
            complete_loaded_builder(&store_path)?.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let store = validated.publish_baseline(publication)?;
        let expected_fence = store.event_fence();
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;

        let mut disconnected_block = canonical_build_block(BlockHeight::new(3), [3; 32], [9; 32]);
        add_tree_state_checkpoint(&mut disconnected_block)?;
        let error = store
            .commit_live_append(
                crate::CanonicalLiveAppend::new(
                    expected_fence,
                    disconnected_block,
                    Vec::new(),
                    BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                    zinder_core::UnixTimestampMillis::new(1_750_000_001_000),
                ),
                &activations,
            )
            .err()
            .ok_or("a disconnected live block must be rejected")?;

        assert!(matches!(
            error,
            CanonicalStoreError::LiveCommitRefused { .. }
        ));
        let reopened = RocksDbCanonicalStore::open_ready(
            &store_path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(
            reopened.ready_evidence().visible_epoch,
            ChainEpochId::new(1)
        );
        assert_eq!(
            reopened
                .scan_canonical_replay()?
                .collect::<Result<Vec<_>, _>>()?
                .len(),
            2
        );
        Ok(())
    }

    #[test]
    fn ready_store_rejects_stale_live_fence_without_advancing()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let validated =
            complete_loaded_builder(&store_path)?.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let mut store = validated.publish_baseline(publication)?;
        let stale_fence = store.event_fence();
        let mut block_three = canonical_build_block(BlockHeight::new(3), [3; 32], [2; 32]);
        add_tree_state_checkpoint(&mut block_three)?;
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let (next_store, _) = store.commit_live_append(
            crate::CanonicalLiveAppend::new(
                stale_fence,
                block_three,
                Vec::new(),
                BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                zinder_core::UnixTimestampMillis::new(1_750_000_001_000),
            ),
            &activations,
        )?;
        store = next_store;

        let error = store
            .commit_live_append(
                crate::CanonicalLiveAppend::new(
                    stale_fence,
                    canonical_build_block(BlockHeight::new(4), [4; 32], [3; 32]),
                    Vec::new(),
                    BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                    zinder_core::UnixTimestampMillis::new(1_750_000_002_000),
                ),
                &activations,
            )
            .err()
            .ok_or("a stale canonical event fence must be rejected")?;

        assert!(matches!(
            error,
            CanonicalStoreError::LiveCommitRefused { .. }
        ));
        let reopened = RocksDbCanonicalStore::open_ready(
            &store_path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(
            reopened
                .scan_canonical_replay()?
                .collect::<Result<Vec<_>, _>>()?
                .len(),
            3
        );
        Ok(())
    }

    #[test]
    fn ready_store_rejects_live_checkpoint_not_derived_from_the_persisted_frontier()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let validated =
            complete_loaded_builder(&store_path)?.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let store = validated.publish_baseline(publication)?;
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let mut block = canonical_build_block(BlockHeight::new(3), [3; 32], [2; 32]);
        block.tree_state_checkpoint = Some(zinder_core::CommitmentTreeCheckpoint::new(
            BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([3; 32])),
            3,
            zinder_core::CommitmentTreeFrontiers::default(),
        ));
        let expected_fence = store.event_fence();

        let error = store
            .commit_live_append(
                crate::CanonicalLiveAppend::new(
                    expected_fence,
                    block,
                    Vec::new(),
                    BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                    zinder_core::UnixTimestampMillis::new(1_750_000_001_000),
                ),
                &activations,
            )
            .err()
            .ok_or("an invented live frontier must be rejected")?;

        assert!(matches!(
            error,
            CanonicalStoreError::LiveCommitRefused { .. }
        ));
        let reopened = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(
            reopened.event_fence().chain_epoch_id(),
            ChainEpochId::new(1)
        );
        Ok(())
    }

    #[test]
    fn live_commit_crash_child_process() -> Result<(), Box<dyn std::error::Error>> {
        let Some(store_path) = env::var_os(LIVE_COMMIT_STORE_PATH_ENV) else {
            return Ok(());
        };
        let store_path = Path::new(&store_path);
        let validated =
            complete_loaded_builder(store_path)?.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let store = validated.publish_baseline(publication)?;
        let mut block = canonical_build_block(BlockHeight::new(3), [3; 32], [2; 32]);
        add_tree_state_checkpoint(&mut block)?;
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let expected_fence = store.event_fence();
        let _ = store.commit_live_append(
            crate::CanonicalLiveAppend::new(
                expected_fence,
                block,
                Vec::new(),
                BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                zinder_core::UnixTimestampMillis::new(1_750_000_001_000),
            ),
            &activations,
        )?;
        Err("live-commit failpoint did not abort the child process".into())
    }

    #[test]
    fn activation_schedule_crash_child_process() -> Result<(), Box<dyn std::error::Error>> {
        let Some(store_path) = env::var_os(ACTIVATION_SCHEDULE_STORE_PATH_ENV) else {
            return Ok(());
        };
        let baseline =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let mut entries = baseline.activations().to_vec();
        entries.push(NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(99),
            activation_height: BlockHeight::new(11),
            name: "Future".to_owned(),
        });
        let extended = NetworkUpgradeActivations::new(Network::ZcashTestnet, entries)?;
        let _store = RocksDbCanonicalStore::open_ready_with_network_upgrade_schedule(
            Path::new(&store_path),
            &extended,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        Err("activation-schedule failpoint did not abort the child process".into())
    }

    #[test]
    fn live_commit_crashes_reopen_at_one_complete_atomic_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let before_path = temporary.path().join("before-atomic-write");
        run_live_commit_crash_child(&before_path, "before_atomic_write")?;
        let before = RocksDbCanonicalStore::open_ready(
            &before_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(before.event_fence().chain_epoch_id(), ChainEpochId::new(1));
        drop(before);

        let after_path = temporary.path().join("after-atomic-write");
        run_live_commit_crash_child(&after_path, "after_atomic_write")?;
        let after = RocksDbCanonicalStore::open_ready(
            &after_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(after.event_fence().chain_epoch_id(), ChainEpochId::new(2));
        assert_eq!(after.event_fence().chain_event_sequence(), 2);
        assert_eq!(
            after.event_fence().visible_tip().height,
            BlockHeight::new(3)
        );
        Ok(())
    }

    #[test]
    fn cold_validation_rejects_replay_corruption_after_bulk_load()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = complete_loaded_builder(&store_path)?;
        let replay_family = store
            .bounded_open
            .db
            .cf_handle(BLOCK_REPLAY_COLUMN_FAMILY)
            .ok_or("block replay family should exist")?;
        store.bounded_open.db.put_cf(
            &replay_family,
            zinder_core::wire::encode_height_key_ascending(BlockHeight::new(1)),
            [0_u8],
        )?;
        drop(replay_family);

        let error = store
            .prepare_cold_certified_publication()
            .err()
            .ok_or("cold publication must reject replay corruption")?;

        assert!(matches!(
            error,
            CanonicalStoreError::BlockReplayInvalid { height: 1, .. }
        ));
        let error = RocksDbCanonicalStore::open_ready(
            &store_path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("corrupt BUILDING store must not open READY")?;
        assert!(matches!(error, CanonicalStoreError::StoreNotReady { .. }));
        Ok(())
    }
    #[test]
    fn publication_crash_child_process() -> Result<(), Box<dyn std::error::Error>> {
        let Some(store_path) = env::var_os(PUBLICATION_STORE_PATH_ENV) else {
            return Ok(());
        };
        let store_path = Path::new(&store_path);
        let validated =
            complete_loaded_builder(store_path)?.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let _published = validated.publish_baseline(publication)?;
        Err("publication failpoint did not abort the child process".into())
    }

    #[test]
    fn publication_crashes_preserve_the_atomic_build_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        for failpoint in [
            "after_flush",
            "after_builder_drop",
            "after_cold_validation",
            "after_construction_manifest",
            "before_atomic_write",
        ] {
            let store_path = temporary.path().join(failpoint);
            run_publication_crash_child(&store_path, failpoint)?;
            assert_building_without_baseline(&store_path)?;
            if failpoint == "after_construction_manifest"
                && !store_path
                    .join(CANONICAL_CONSTRUCTION_MANIFEST_FILE_NAME)
                    .is_file()
            {
                return Err("sidecar must be durable before the READY transition".into());
            }
        }

        let committed_path = temporary.path().join("after_atomic_write");
        run_publication_crash_child(&committed_path, "after_atomic_write")?;
        let reopened = RocksDbCanonicalStore::open_ready(
            &committed_path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(reopened.ready_evidence().visible_epoch.value(), 1);
        assert_eq!(reopened.ready_evidence().visible_event_sequence, 1);
        Ok(())
    }

    #[test]
    fn ready_open_rejects_missing_atomic_baseline_member() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = complete_loaded_builder(&store_path)?;
        let validated = store.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let published = validated.publish_baseline(publication)?;
        drop(published);

        let column_families = DB::list_cf(&rust_rocksdb::Options::default(), &store_path)?;
        assert_eq!(
            column_families.len(),
            CANONICAL_DATA_COLUMN_FAMILIES.len() + 1
        );
        let db = DB::open_cf(
            &rust_rocksdb::Options::default(),
            &store_path,
            &column_families,
        )?;
        let event_family = db
            .cf_handle(CHAIN_EVENT_COLUMN_FAMILY)
            .ok_or("chain event family must exist")?;
        db.delete_cf(&event_family, 1_u64.to_be_bytes())?;
        drop(event_family);
        db.flush_wal(true)?;
        drop(db);

        let error = RocksDbCanonicalStore::open_ready(
            &store_path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("READY without event 1 must fail admission")?;
        assert!(matches!(
            error,
            CanonicalStoreError::PublicationRefused { .. }
        ));
        Ok(())
    }

    #[test]
    fn ready_open_rejects_wrong_first_retained_block_hash() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let validated =
            complete_loaded_builder(&store_path)?.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        drop(validated.publish_baseline(publication)?);

        let column_families = DB::list_cf(&rust_rocksdb::Options::default(), &store_path)?;
        let db = DB::open_cf(
            &rust_rocksdb::Options::default(),
            &store_path,
            &column_families,
        )?;
        let encoded_control = db
            .get(STORE_CONTROL_KEY)?
            .ok_or("READY control must exist")?;
        let decoded_control = decode_store_control(&store_path, &encoded_control)?;
        let CanonicalStoreBuildState::Ready(mut wrong_evidence) = decoded_control.build_state
        else {
            return Err("published control must be READY".into());
        };
        wrong_evidence.first_retained_block =
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([9; 32]));
        db.put(
            STORE_CONTROL_KEY,
            encode_ready_store_control(
                decoded_control.workload,
                &decoded_control.build_plan,
                decoded_control.cursor_auth_key,
                &wrong_evidence,
            )?,
        )?;
        db.flush_wal(true)?;
        drop(db);

        let error = RocksDbCanonicalStore::open_ready(
            &store_path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("wrong first retained block hash must fail admission")?;
        assert!(error.to_string().contains("first retained block"));
        Ok(())
    }

    #[test]
    fn invalid_baseline_input_can_be_corrected_without_rebuilding()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = complete_loaded_builder(&store_path)?;

        let validated = store.prepare_cold_certified_publication()?;
        let error = validated
            .prepare_baseline(crate::CanonicalBaselinePublication::new(
                BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([9; 32])),
                zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
            ))
            .err()
            .ok_or("a settled tip with the wrong hash must be rejected")?;

        assert!(matches!(
            error,
            CanonicalStoreError::PublicationRefused { .. }
        ));
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let published = validated.publish_baseline(publication)?;
        drop(published);
        RocksDbCanonicalStore::open_ready(
            &store_path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        Ok(())
    }

    #[test]
    fn bulk_load_blocks_reports_exact_per_family_logical_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let build_plan = CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            RawBlobRetention::All,
            CanonicalReorgPolicy::new(100)?,
        )?;
        let mut store = RocksDbCanonicalBuilder::create_fresh(
            &store_path,
            CanonicalStoreWorkload::Explorer,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let mut block = canonical_build_block_with_raw_blobs(
            BlockHeight::new(1),
            [1; 32],
            Network::ZcashTestnet.genesis_hash().as_bytes(),
        );
        add_tree_state_checkpoint(&mut block)?;
        let expected_block_replay_logical_bytes =
            4_u64 + u64::try_from(block.replay_envelope.as_bytes().len())?;
        let expected_compact_block_logical_bytes = 4_u64
            + u64::try_from(zinder_proto::wire::encode_compact_block(&block.compact_block).len())?;

        let evidence = store.bulk_load_blocks(vec![Ok::<_, std::io::Error>(block)])?;

        assert_eq!(evidence.block_header_logical_bytes, 4 + 184);
        assert_eq!(evidence.block_hash_index_logical_bytes, 32 + 4);
        assert_eq!(
            evidence.block_replay_logical_bytes,
            expected_block_replay_logical_bytes
        );
        assert_eq!(
            evidence.compact_block_logical_bytes,
            expected_compact_block_logical_bytes
        );
        assert_eq!(evidence.transaction_location_logical_bytes, 32 + 40);
        assert_eq!(evidence.transaction_blob_logical_bytes, 8 + 5);
        assert_eq!(evidence.block_blob_logical_bytes, 4 + 3);
        assert!(evidence.tree_state_checkpoint_logical_bytes > 0);
        let checked_family_logical_bytes = [
            evidence.block_header_logical_bytes,
            evidence.block_hash_index_logical_bytes,
            evidence.block_replay_logical_bytes,
            evidence.compact_block_logical_bytes,
            evidence.transaction_location_logical_bytes,
            evidence.transaction_blob_logical_bytes,
            evidence.block_blob_logical_bytes,
            evidence.tree_state_checkpoint_logical_bytes,
            evidence.block_final_note_commitment_roots_logical_bytes,
        ]
        .into_iter()
        .try_fold(0_u64, u64::checked_add)
        .ok_or("family logical byte sum must fit in u64")?;
        assert_eq!(evidence.logical_bytes, checked_family_logical_bytes);
        assert_eq!(evidence.transaction_count, 1);
        assert_eq!(evidence.transaction_location_count, 1);
        assert_eq!(evidence.transaction_blob_count, 1);
        assert_eq!(evidence.block_blob_count, 1);
        Ok(())
    }

    #[test]
    fn no_blob_bulk_load_counts_semantic_locations_independently_from_optional_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let build_plan = CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            RawBlobRetention::None,
            CanonicalReorgPolicy::new(100)?,
        )?;
        let mut store = RocksDbCanonicalBuilder::create_fresh(
            &store_path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let mut block = canonical_build_block_with_raw_blobs(
            BlockHeight::new(1),
            [1; 32],
            Network::ZcashTestnet.genesis_hash().as_bytes(),
        );
        block.transaction_blobs.clear();
        block.block_blob = None;
        add_tree_state_checkpoint(&mut block)?;

        let evidence = store.bulk_load_blocks([Ok::<_, std::io::Error>(block)])?;

        assert_eq!(evidence.transaction_count, 1);
        assert_eq!(evidence.transaction_location_count, 1);
        assert_eq!(evidence.transaction_blob_count, 0);
        assert_eq!(evidence.block_blob_count, 0);
        assert_eq!(evidence.transaction_location_logical_bytes, 32 + 40);
        assert_eq!(evidence.transaction_blob_logical_bytes, 0);
        assert_eq!(evidence.block_blob_logical_bytes, 0);
        Ok(())
    }

    #[test]
    fn bulk_load_blocks_merges_tiny_reverse_index_runs_in_key_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let build_plan = CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([2; 32])),
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
        )?;
        let mut store = RocksDbCanonicalBuilder::create_fresh(
            &store_path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let blocks = vec![
            Ok::<_, std::io::Error>(canonical_build_block(
                BlockHeight::new(1),
                [3; 32],
                Network::ZcashTestnet.genesis_hash().as_bytes(),
            )),
            Ok(canonical_build_block(BlockHeight::new(2), [1; 32], [3; 32])),
            Ok(canonical_build_block(BlockHeight::new(3), [2; 32], [1; 32])),
        ];
        let mut blocks = blocks;
        if let Some(Ok(tip)) = blocks.last_mut() {
            add_tree_state_checkpoint(tip)?;
        }

        let evidence = store.bulk_load_blocks_with_limits(blocks, 1, 72)?;

        assert_eq!(evidence.block_hash_index_count, 3);
        assert!(evidence.sst_file_count > 7);
        let block_hash_index = store
            .bounded_open
            .db
            .cf_handle(BLOCK_HASH_INDEX_COLUMN_FAMILY)
            .ok_or("block hash index must exist")?;
        let keys = store
            .bounded_open
            .db
            .iterator_cf(&block_hash_index, rust_rocksdb::IteratorMode::Start)
            .map(|row| row.map(|(key, _)| key[0]))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(keys, vec![1, 2, 3]);
        Ok(())
    }

    #[test]
    fn bulk_load_blocks_source_error_leaves_every_family_empty()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let mut store = create_building_store(&store_path, CanonicalHistoryBounds::complete())?;
        let blocks = vec![
            Ok(canonical_build_block(
                BlockHeight::new(1),
                [1; 32],
                Network::ZcashTestnet.genesis_hash().as_bytes(),
            )),
            Err(std::io::Error::other("source stopped")),
        ];

        let error = store
            .bulk_load_blocks(blocks)
            .err()
            .ok_or("source failure must abort every family")?;

        assert!(matches!(error, CanonicalStoreBuildError::Source { .. }));
        assert!(canonical_block_families_are_empty(&store.bounded_open.db)?);
        assert!(!canonical_block_staging_path(&store.store_path).exists());
        Ok(())
    }

    #[test]
    fn bulk_load_blocks_rejects_workload_and_compact_payload_mismatches()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let wallet_path = temporary.path().join("wallet");
        let mut wallet = create_building_store(&wallet_path, CanonicalHistoryBounds::complete())?;
        let mut wallet_block = canonical_build_block(
            BlockHeight::new(1),
            [1; 32],
            Network::ZcashTestnet.genesis_hash().as_bytes(),
        );
        wallet_block.block_blob = Some(zinder_core::BlockBlobArtifact::new(
            wallet_block.facts.block_header.height,
            wallet_block.facts.block_header.block_hash,
            wallet_block.facts.block_header.parent_hash,
            [],
        ));
        let error = wallet
            .bulk_load_blocks(vec![Ok::<_, std::io::Error>(wallet_block)])
            .err()
            .ok_or("wallet raw block must be rejected")?;
        assert!(matches!(
            error,
            CanonicalStoreBuildError::Store(CanonicalStoreError::BlockLoadSequenceInvalid { .. })
        ));
        assert!(canonical_block_families_are_empty(&wallet.bounded_open.db)?);

        let compact_path = temporary.path().join("compact");
        let mut compact_store =
            create_building_store(&compact_path, CanonicalHistoryBounds::complete())?;
        let mut compact_block = canonical_build_block(
            BlockHeight::new(1),
            [1; 32],
            Network::ZcashTestnet.genesis_hash().as_bytes(),
        );
        compact_block.compact_block = CompactBlockArtifact::empty(
            zinder_core::BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xff; 32])),
            Network::ZcashTestnet.genesis_hash(),
            0,
            zinder_core::CompactChainMetadata {
                sapling_commitment_tree_size: 0,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        );
        let error = compact_store
            .bulk_load_blocks(vec![Ok::<_, std::io::Error>(compact_block)])
            .err()
            .ok_or("divergent compact payload must be rejected")?;
        assert!(matches!(
            error,
            CanonicalStoreBuildError::Store(CanonicalStoreError::BlockLoadSequenceInvalid { .. })
        ));
        assert!(canonical_block_families_are_empty(
            &compact_store.bounded_open.db
        )?);
        Ok(())
    }

    #[test]
    fn compact_payload_validation_rejects_parent_time_index_and_transaction_id_mismatches()
    -> Result<(), Box<dyn std::error::Error>> {
        let height = BlockHeight::new(1);
        let block_hash = [1; 32];
        let parent_hash = Network::ZcashTestnet.genesis_hash().as_bytes();

        for (parent, time) in [
            (BlockHash::from_bytes([99; 32]), height.value()),
            (BlockHash::from_bytes(parent_hash), height.value() + 1),
        ] {
            let mut block = canonical_build_block(height, block_hash, parent_hash);
            block.compact_block = CompactBlockArtifact::empty(
                BlockId::new(height, BlockHash::from_bytes(block_hash)),
                parent,
                time,
                zinder_core::CompactChainMetadata {
                    sapling_commitment_tree_size: 0,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            );
            assert!(validate_compact_block_payload(&block).is_err());
        }

        for (index, transaction_id) in [
            (1, TransactionId::from_bytes([9; 32])),
            (0, TransactionId::from_bytes([10; 32])),
        ] {
            let mut block = canonical_build_block_with_raw_blobs(height, block_hash, parent_hash);
            block.compact_block = CompactBlockArtifact::new(
                BlockId::new(height, BlockHash::from_bytes(block_hash)),
                BlockHash::from_bytes(parent_hash),
                height.value(),
                vec![CompactTransaction {
                    index,
                    transaction_id,
                    data: CompactTransactionData::default(),
                }],
                zinder_core::CompactChainMetadata {
                    sapling_commitment_tree_size: 0,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            )?;
            assert!(validate_compact_block_payload(&block).is_err());
        }
        Ok(())
    }

    #[test]
    fn bulk_load_blocks_rejects_missing_tip_checkpoint_and_wallet_roots()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let tip = BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32]));

        let missing_checkpoint_path = temporary.path().join("missing-checkpoint");
        let mut missing_checkpoint_store = RocksDbCanonicalBuilder::create_fresh(
            &missing_checkpoint_path,
            CanonicalStoreWorkload::Wallet,
            CanonicalStoreBuildPlan::complete(
                &activations,
                0,
                tip,
                RawBlobRetention::Transactions,
                CanonicalReorgPolicy::new(100)?,
            )?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let missing_checkpoint = canonical_build_block(
            BlockHeight::new(1),
            [1; 32],
            Network::ZcashTestnet.genesis_hash().as_bytes(),
        );
        let error = missing_checkpoint_store
            .bulk_load_blocks(vec![Ok::<_, std::io::Error>(missing_checkpoint)])
            .err()
            .ok_or("tip without a tree-state checkpoint must be rejected")?;
        assert!(matches!(
            error,
            CanonicalStoreBuildError::Store(CanonicalStoreError::BlockLoadSequenceInvalid { .. })
        ));

        let wallet_roots_path = temporary.path().join("wallet-roots");
        let mut wallet_roots_store = RocksDbCanonicalBuilder::create_fresh(
            &wallet_roots_path,
            CanonicalStoreWorkload::Wallet,
            CanonicalStoreBuildPlan::complete(
                &activations,
                0,
                tip,
                RawBlobRetention::Transactions,
                CanonicalReorgPolicy::new(100)?,
            )?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let mut wallet_roots = canonical_build_block(
            BlockHeight::new(1),
            [1; 32],
            Network::ZcashTestnet.genesis_hash().as_bytes(),
        );
        add_tree_state_checkpoint(&mut wallet_roots)?;
        wallet_roots.block_final_note_commitment_roots =
            Some(zinder_core::BlockFinalNoteCommitmentRoots::unavailable(
                BlockHeight::new(1),
                BlockHash::from_bytes([1; 32]),
            ));
        let error = wallet_roots_store
            .bulk_load_blocks(vec![Ok::<_, std::io::Error>(wallet_roots)])
            .err()
            .ok_or("wallet workload roots must be rejected")?;
        assert!(matches!(
            error,
            CanonicalStoreBuildError::Store(CanonicalStoreError::BlockLoadSequenceInvalid { .. })
        ));
        Ok(())
    }

    #[test]
    fn persisted_readback_rejects_miskeyed_replay() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = create_building_store(&store_path, CanonicalHistoryBounds::complete())?;
        let block_replay = store
            .bounded_open
            .db
            .cf_handle(BLOCK_REPLAY_COLUMN_FAMILY)
            .ok_or("block replay family should exist")?;
        let replay = replay(BlockHeight::new(1), [1; 32], [0; 32]);
        store.bounded_open.db.put_cf(
            &block_replay,
            zinder_core::wire::encode_height_key_ascending(BlockHeight::new(2)),
            replay.as_bytes(),
        )?;

        let error = validate_persisted_block_replays(&store.bounded_open.db)
            .err()
            .ok_or("a miskeyed replay must be rejected")?;
        assert!(matches!(
            error,
            CanonicalStoreError::BlockReplayInvalid { height: 2, .. }
        ));
        Ok(())
    }

    #[test]
    fn persisted_readback_rejects_malformed_height_key() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = create_building_store(&store_path, CanonicalHistoryBounds::complete())?;
        let block_replay = store
            .bounded_open
            .db
            .cf_handle(BLOCK_REPLAY_COLUMN_FAMILY)
            .ok_or("block replay family should exist")?;
        let replay = replay(BlockHeight::new(1), [1; 32], [0; 32]);
        store
            .bounded_open
            .db
            .put_cf(&block_replay, b"bad", replay.as_bytes())?;

        let error = validate_persisted_block_replays(&store.bounded_open.db)
            .err()
            .ok_or("a malformed replay key must be rejected")?;
        assert!(matches!(
            error,
            CanonicalStoreError::BlockReplayKeyInvalid { .. }
        ));
        Ok(())
    }

    #[test]
    fn persisted_readback_rejects_non_version_one_replay() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = create_building_store(&store_path, CanonicalHistoryBounds::complete())?;
        let block_replay = store
            .bounded_open
            .db
            .cf_handle(BLOCK_REPLAY_COLUMN_FAMILY)
            .ok_or("block replay family should exist")?;
        let replay = replay(BlockHeight::new(1), [1; 32], [0; 32]);
        let mut encoded = replay.into_bytes();
        if !encoded.starts_with(&[0x08, 0x01]) {
            return Err("the version-1 replay fixture has an unexpected envelope prefix".into());
        }
        encoded[1] = 0x02;
        store.bounded_open.db.put_cf(
            &block_replay,
            zinder_core::wire::encode_height_key_ascending(BlockHeight::new(1)),
            encoded,
        )?;

        let error = validate_persisted_block_replays(&store.bounded_open.db)
            .err()
            .ok_or("a non-version-one replay must be rejected")?;
        assert!(matches!(
            error,
            CanonicalStoreError::BlockReplayInvalid { height: 1, .. }
        ));
        Ok(())
    }

    fn create_building_store(
        path: &Path,
        history_bounds: CanonicalHistoryBounds,
    ) -> Result<RocksDbCanonicalBuilder, Box<dyn std::error::Error>> {
        let build_plan = match history_bounds.preceding_checkpoint() {
            None => complete_build_plan()?,
            Some(checkpoint) => {
                let activations = crate::canonical_store::test_network_upgrade_activations(
                    Network::ZcashTestnet,
                )?;
                CanonicalStoreBuildPlan::checkpointed(
                    &activations,
                    zinder_core::CommitmentTreeCheckpoint::new(
                        checkpoint,
                        0,
                        crate::canonical_store::test_checkpoint_frontiers(
                            &activations,
                            checkpoint.height,
                        ),
                    ),
                    BlockId::new(BlockHeight::new(100), BlockHash::from_bytes([10; 32])),
                    RawBlobRetention::Transactions,
                    CanonicalReorgPolicy::new(100)?,
                )?
            }
        };
        Ok(RocksDbCanonicalBuilder::create_fresh(
            path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?)
    }

    fn assert_ready_construction_manifest_binding(
        store_path: &Path,
        store: &RocksDbCanonicalStore,
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            store.ready_evidence().construction_manifest_version,
            crate::CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION
        );
        assert!(
            store
                .ready_evidence()
                .construction_manifest_sha256
                .iter()
                .any(|byte| *byte != 0)
        );
        assert!(
            store_path
                .join("canonical-construction-manifest.v4.json")
                .is_file()
        );
        let manifest_binding =
            RocksDbCanonicalStore::read_construction_manifest_binding(store_path)?;
        assert_eq!(
            manifest_binding.version,
            crate::CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION
        );
        assert_eq!(
            manifest_binding.sha256,
            store.ready_evidence().construction_manifest_sha256
        );
        Ok(())
    }

    fn complete_loaded_builder(
        path: &Path,
    ) -> Result<RocksDbCanonicalBuilder, Box<dyn std::error::Error>> {
        complete_loaded_builder_with_reorg_window(path, 100, BlockHeight::new(2))
    }

    fn published_loaded_store(
        path: &Path,
        build_tip_height: BlockHeight,
    ) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
        let builder = complete_loaded_builder_with_reorg_window(path, 100, build_tip_height)?;
        let settled_height = BlockHeight::new(
            build_tip_height
                .value()
                .checked_sub(1)
                .ok_or("published test store requires a non-genesis tip")?,
        );
        let settled_hash_byte = u8::try_from(settled_height.value())?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(
                settled_height,
                BlockHash::from_bytes([settled_hash_byte; 32]),
            ),
            zinder_core::UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        Ok(validated.publish_baseline(publication)?)
    }

    fn complete_loaded_builder_with_reorg_window(
        path: &Path,
        reorg_window_blocks: u32,
        build_tip_height: BlockHeight,
    ) -> Result<RocksDbCanonicalBuilder, Box<dyn std::error::Error>> {
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let build_tip_byte = u8::try_from(build_tip_height.value())?;
        let build_plan = CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            BlockId::new(
                build_tip_height,
                BlockHash::from_bytes([build_tip_byte; 32]),
            ),
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(reorg_window_blocks)?,
        )?;
        let mut store = RocksDbCanonicalBuilder::create_fresh(
            path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let mut blocks = Vec::new();
        for height in 1..=build_tip_height.value() {
            let block_hash = u8::try_from(height)?;
            let parent_hash = if height == 1 {
                Network::ZcashTestnet.genesis_hash().as_bytes()
            } else {
                [u8::try_from(height - 1)?; 32]
            };
            let mut block =
                canonical_build_block(BlockHeight::new(height), [block_hash; 32], parent_hash);
            if height == build_tip_height.value() {
                add_tree_state_checkpoint(&mut block)?;
            }
            blocks.push(Ok::<_, std::io::Error>(block));
        }
        store.bulk_load_blocks(blocks)?;
        store.load_subtree_roots(std::iter::empty())?;
        store.confirm_source_tip_checkpoint(&zinder_core::CommitmentTreeCheckpoint::new(
            BlockId::new(
                build_tip_height,
                BlockHash::from_bytes([build_tip_byte; 32]),
            ),
            build_tip_height.value(),
            crate::canonical_store::test_checkpoint_frontiers(&activations, build_tip_height),
        ))?;
        Ok(store)
    }

    fn run_publication_crash_child(
        store_path: &Path,
        failpoint: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let status = Command::new(env::current_exe()?)
            .arg("--exact")
            .arg("canonical_store::builder::tests::publication_crash_child_process")
            .arg("--nocapture")
            .env(PUBLICATION_STORE_PATH_ENV, store_path)
            .env(PUBLICATION_FAILPOINT_ENV, failpoint)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        if status.success() {
            return Err(format!("publication failpoint {failpoint} did not crash").into());
        }
        Ok(())
    }

    fn run_live_commit_crash_child(
        store_path: &Path,
        failpoint: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let status = Command::new(env::current_exe()?)
            .arg("--exact")
            .arg("canonical_store::builder::tests::live_commit_crash_child_process")
            .arg("--nocapture")
            .env(LIVE_COMMIT_STORE_PATH_ENV, store_path)
            .env(LIVE_COMMIT_FAILPOINT_ENV, failpoint)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        if status.success() {
            return Err(format!("live-commit failpoint {failpoint} did not crash").into());
        }
        Ok(())
    }

    fn run_activation_schedule_crash_child(
        store_path: &Path,
        failpoint: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let status = Command::new(env::current_exe()?)
            .arg("--exact")
            .arg("canonical_store::builder::tests::activation_schedule_crash_child_process")
            .arg("--nocapture")
            .env(ACTIVATION_SCHEDULE_STORE_PATH_ENV, store_path)
            .env(ACTIVATION_SCHEDULE_FAILPOINT_ENV, failpoint)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        if status.success() {
            return Err(format!("activation-schedule failpoint {failpoint} did not crash").into());
        }
        Ok(())
    }

    fn assert_building_without_baseline(
        store_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let error = RocksDbCanonicalStore::open_ready(
            store_path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("pre-publication crash must not open READY")?;
        if !matches!(error, CanonicalStoreError::StoreNotReady { .. }) {
            return Err(error.into());
        }
        let column_families = DB::list_cf(&rust_rocksdb::Options::default(), store_path)?;
        let db = DB::open_cf_for_read_only(
            &rust_rocksdb::Options::default(),
            store_path,
            &column_families,
            false,
        )?;
        for name in [CHAIN_EPOCH_COLUMN_FAMILY, CHAIN_EVENT_COLUMN_FAMILY] {
            let family = db.cf_handle(name).ok_or("baseline family must exist")?;
            if db
                .iterator_cf(&family, rust_rocksdb::IteratorMode::Start)
                .next()
                .is_some()
            {
                return Err(format!("{name} must remain empty before atomic publication").into());
            }
        }
        Ok(())
    }

    fn complete_build_plan() -> Result<CanonicalStoreBuildPlan, Box<dyn std::error::Error>> {
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        Ok(CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32])),
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
        )?)
    }

    fn canonical_replay_count(
        store: &RocksDbCanonicalStore,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        Ok(store
            .scan_canonical_replay()?
            .collect::<Result<Vec<_>, _>>()?
            .len())
    }

    fn assert_reopened_window_two_checkpoint(
        store_path: &Path,
        activations: &zinder_core::NetworkUpgradeActivations,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reopened = RocksDbCanonicalStore::open_ready(
            store_path,
            activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(2)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let checkpoint = reopened.sequence_checkpoint();
        let replayed = reopened
            .scan_canonical_replay()?
            .collect::<Result<Vec<_>, _>>()?;
        let mut serial_prefix = CanonicalBlockFactsSequenceDigestBuilder::new(
            CanonicalBlockFactsSequenceDigestVersion::V1,
        );
        let mut prefix_bytes = 0_u64;
        for replay in replayed.iter().take(3) {
            serial_prefix.try_append(replay.reference_digest())?;
            let encoded = encode_canonical_block_replay(
                replay.facts(),
                replay.format_version(),
                replay.reference_digest().version(),
            );
            prefix_bytes = prefix_bytes
                .checked_add(u64::try_from(encoded.as_bytes().len())?)
                .ok_or("serial prefix bytes overflow")?;
        }
        assert_eq!(checkpoint.sequence_digest(), serial_prefix.finish());
        assert_eq!(checkpoint.logical_replay_bytes(), prefix_bytes);

        let mut resumed = CanonicalBlockFactsSequenceDigestBuilder::resume_from_prefix(
            checkpoint.sequence_digest(),
        );
        for replay in replayed.iter().skip(3) {
            resumed.try_append(replay.reference_digest())?;
        }
        assert_eq!(resumed.finish(), reopened.event_fence().sequence_digest());
        Ok(())
    }

    fn rewrite_corrupted_sequence_checkpoint(
        store_path: &Path,
        corruption: u8,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let column_families = DB::list_cf(&rust_rocksdb::Options::default(), store_path)?;
        let db = DB::open_cf(
            &rust_rocksdb::Options::default(),
            store_path,
            &column_families,
        )?;
        let encoded = db
            .get(STORE_CONTROL_KEY)?
            .ok_or("READY control is absent")?;
        let decoded = decode_store_control(store_path, &encoded)?;
        let CanonicalStoreBuildState::Ready(mut evidence) = decoded.build_state else {
            return Err("published control must be READY".into());
        };
        evidence.sequence_checkpoint =
            corrupted_sequence_checkpoint(evidence.sequence_checkpoint, corruption);
        db.put(
            STORE_CONTROL_KEY,
            encode_ready_store_control(
                decoded.workload,
                &decoded.build_plan,
                decoded.cursor_auth_key,
                &evidence,
            )?,
        )?;
        db.flush_wal(true)?;
        Ok(())
    }

    fn corrupted_sequence_checkpoint(
        checkpoint: crate::CanonicalSequenceCheckpoint,
        corruption: u8,
    ) -> crate::CanonicalSequenceCheckpoint {
        let (through, retained_count, digest, logical_bytes) = match corruption {
            0 => (
                BlockId::new(checkpoint.through().height, BlockHash::from_bytes([9; 32])),
                checkpoint.retained_block_count(),
                checkpoint.sequence_digest(),
                checkpoint.logical_replay_bytes(),
            ),
            1 => {
                let count = checkpoint.retained_block_count() + 1;
                (
                    checkpoint.through(),
                    count,
                    CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                        checkpoint.sequence_digest().version(),
                        count,
                        checkpoint.sequence_digest().as_bytes(),
                    ),
                    checkpoint.logical_replay_bytes(),
                )
            }
            2 => (
                checkpoint.through(),
                checkpoint.retained_block_count(),
                CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                    checkpoint.sequence_digest().version(),
                    checkpoint.retained_block_count(),
                    [9; 32],
                ),
                checkpoint.logical_replay_bytes(),
            ),
            _ => (
                checkpoint.through(),
                checkpoint.retained_block_count(),
                checkpoint.sequence_digest(),
                checkpoint.logical_replay_bytes() + 1,
            ),
        };
        crate::CanonicalSequenceCheckpoint::from_admitted_parts(
            through,
            retained_count,
            digest,
            logical_bytes,
        )
    }

    fn replay(
        height: BlockHeight,
        block_hash: [u8; 32],
        parent_hash: [u8; 32],
    ) -> CanonicalBlockReplayEnvelope {
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
        encode_canonical_block_replay(
            &facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        )
    }

    fn canonical_build_block(
        height: BlockHeight,
        block_hash: [u8; 32],
        parent_hash: [u8; 32],
    ) -> crate::CanonicalBuildBlock {
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
        crate::CanonicalBuildBlock {
            compact_block: CompactBlockArtifact::empty(
                zinder_core::BlockId::new(height, facts.block_header.block_hash),
                facts.block_header.parent_hash,
                height.value(),
                zinder_core::CompactChainMetadata {
                    sapling_commitment_tree_size: 0,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            ),
            replay_envelope,
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            tree_state_checkpoint: None,
            block_final_note_commitment_roots: None,
            transaction_blobs: Vec::new(),
            block_blob: None,
            facts,
        }
    }

    fn canonical_build_block_with_raw_blobs(
        height: BlockHeight,
        block_hash: [u8; 32],
        parent_hash: [u8; 32],
    ) -> crate::CanonicalBuildBlock {
        let mut block = canonical_build_block(height, block_hash, parent_hash);
        let raw_transaction_bytes = vec![1, 2, 3, 4, 5];
        let raw_block_bytes = vec![6, 7, 8];
        let transaction_id = TransactionId::from_bytes([9; 32]);
        block.facts.transactions.push(CanonicalTransactionFacts {
            public_facts: TransactionPublicFacts {
                transaction_id,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::Unsupported {
                    effective_version: 0,
                    version_group_id: None,
                },
                consensus_branch_id: None,
                lock_time: LockTime::Unlocked,
                expiry_height: None,
                size_bytes: u32::try_from(raw_transaction_bytes.len()).unwrap_or(u32::MAX),
                counts: TransactionComponentCounts::EMPTY,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                privacy_shape: PrivacyShape::Unclassified,
                is_coinbase: false,
                unsupported_sections: vec![UnsupportedSection::FutureVersionHeader],
            },
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                &raw_transaction_bytes,
            ),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs: Vec::new(),
            transparent_outputs: Vec::new(),
        });
        block.facts.serialized_bytes_digest =
            SerializedBytesDigest::from_serialized_bytes(&raw_block_bytes);
        block.transaction_blobs.push(TransactionBlobArtifact::new(
            TransactionLocation::new(
                transaction_id,
                height,
                block.facts.block_header.block_hash,
                0,
            ),
            raw_transaction_bytes,
        ));
        block.block_blob = Some(zinder_core::BlockBlobArtifact::new(
            height,
            block.facts.block_header.block_hash,
            block.facts.block_header.parent_hash,
            raw_block_bytes,
        ));
        block.replay_envelope = encode_canonical_block_replay(
            &block.facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        );
        block
    }

    fn canonical_build_block_with_block_blob(
        height: BlockHeight,
        block_hash: [u8; 32],
        parent_hash: [u8; 32],
        raw_block_bytes: Vec<u8>,
    ) -> crate::CanonicalBuildBlock {
        let mut block = canonical_build_block(height, block_hash, parent_hash);
        block.facts.serialized_bytes_digest =
            SerializedBytesDigest::from_serialized_bytes(&raw_block_bytes);
        block.block_blob = Some(zinder_core::BlockBlobArtifact::new(
            height,
            block.facts.block_header.block_hash,
            block.facts.block_header.parent_hash,
            raw_block_bytes,
        ));
        block.replay_envelope = encode_canonical_block_replay(
            &block.facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        );
        block
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the exact archive fixture writes every versioned row explicitly"
    )]
    fn published_store_with_displaced_archive(
        store_path: &Path,
    ) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
        let validated =
            complete_loaded_builder(store_path)?.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(crate::CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
            UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        let store = validated.publish_baseline(publication)?;
        let epoch = ChainEpochId::new(2);
        let displaced_at = UnixTimestampMillis::new(1_750_000_000_002);
        let event_range =
            zinder_core::BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2));
        let epoch_family = store
            .bounded_open
            .db
            .cf_handle(CHAIN_EPOCH_COLUMN_FAMILY)
            .ok_or("chain epoch family absent")?;
        let event_family = store
            .bounded_open
            .db
            .cf_handle(CHAIN_EVENT_COLUMN_FAMILY)
            .ok_or("chain event family absent")?;
        let archive_family = store
            .bounded_open
            .db
            .cf_handle(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
            .ok_or("archive family absent")?;
        store.bounded_open.db.put_cf(
            &epoch_family,
            epoch.value().to_be_bytes(),
            encode_live_chain_epoch(
                BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([32; 32])),
                BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([31; 32])),
                ChainTipMetadata::new(7, 8, 9),
                displaced_at,
            ),
        )?;
        store.bounded_open.db.put_cf(
            &event_family,
            epoch.value().to_be_bytes(),
            encode_live_event(
                epoch,
                ChainEpochId::new(1),
                Some(event_range),
                event_range,
                CanonicalEventFence::from_persisted_event(
                    epoch,
                    epoch.value(),
                    BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([32; 32])),
                    2,
                    [9; 32],
                ),
            ),
        )?;
        let coverage = DisplacedBlockArchiveCoverage {
            activation_event_sequence: 2,
            activation_epoch: epoch,
            activated_at: displaced_at,
        };
        store.bounded_open.db.put_cf(
            &archive_family,
            [0x00],
            encode_test_archive_state(coverage, 2),
        )?;
        let (event_context_key, event_context) =
            encode_test_event_context(2, event_range, epoch, displaced_at, 2);
        store
            .bounded_open
            .db
            .put_cf(&archive_family, event_context_key, event_context)?;

        let first_replay = displaced_archive_block_one()?;
        let first_key =
            encode_test_order_key(2, BlockHeight::new(1), BlockHash::from_bytes([11; 32]));
        store.bounded_open.db.put_cf(
            &archive_family,
            first_key,
            encode_test_archive_record(first_replay, epoch, displaced_at, None, None)?,
        )?;
        let (first_pointer_key, first_pointer) =
            encode_test_hash_pointer_rows(2, BlockHeight::new(1), BlockHash::from_bytes([11; 32]));
        store
            .bounded_open
            .db
            .put_cf(&archive_family, first_pointer_key, first_pointer)?;

        let (second_replay, raw_block_bytes) = displaced_archive_block_two()?;
        let second_key =
            encode_test_order_key(2, BlockHeight::new(2), BlockHash::from_bytes([22; 32]));
        store.bounded_open.db.put_cf(
            &archive_family,
            second_key,
            encode_test_archive_record(
                second_replay,
                epoch,
                displaced_at,
                Some(raw_block_bytes),
                Some(displaced_archive_roots()),
            )?,
        )?;
        let (second_pointer_key, second_pointer) =
            encode_test_hash_pointer_rows(2, BlockHeight::new(2), BlockHash::from_bytes([22; 32]));
        store
            .bounded_open
            .db
            .put_cf(&archive_family, second_pointer_key, second_pointer)?;
        store.bounded_open.db.flush_wal(true)?;
        drop(archive_family);
        drop(event_family);
        drop(epoch_family);
        Ok(store)
    }

    fn displaced_archive_block_two() -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error>> {
        let mut block =
            canonical_build_block_with_raw_blobs(BlockHeight::new(2), [22; 32], [11; 32]);
        let transaction = block
            .facts
            .transactions
            .first_mut()
            .ok_or("archive transaction fixture absent")?;
        transaction.public_facts.is_coinbase = true;
        transaction
            .transparent_outputs
            .push(TransparentOutputFact::new(
                0,
                42,
                [0x51],
                TransparentAddressScriptHash::of_script_pub_key(&[0x51]),
            ));
        let replay = encode_canonical_block_replay(
            &block.facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        )
        .into_bytes();
        let raw_block_bytes = block
            .block_blob
            .ok_or("archive raw block fixture absent")?
            .raw_block_bytes;
        Ok((replay, raw_block_bytes))
    }

    fn displaced_archive_block_one() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        displaced_archive_coinbase_replay(BlockHeight::new(1), [11; 32], [0; 32])
    }

    fn displaced_archive_coinbase_replay(
        height: BlockHeight,
        block_hash: [u8; 32],
        parent_hash: [u8; 32],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut block = canonical_build_block_with_raw_blobs(height, block_hash, parent_hash);
        block
            .facts
            .transactions
            .first_mut()
            .ok_or("archive transaction fixture absent")?
            .public_facts
            .is_coinbase = true;
        Ok(encode_canonical_block_replay(
            &block.facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        )
        .into_bytes())
    }

    fn displaced_archive_late_coinbase_replay() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut block =
            canonical_build_block_with_raw_blobs(BlockHeight::new(2), [22; 32], [11; 32]);
        let mut late_coinbase = block
            .facts
            .transactions
            .first()
            .cloned()
            .ok_or("archive transaction fixture absent")?;
        late_coinbase.public_facts.transaction_id = TransactionId::from_bytes([10; 32]);
        late_coinbase.public_facts.is_coinbase = true;
        block.facts.transactions.push(late_coinbase);
        Ok(encode_canonical_block_replay(
            &block.facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        )
        .into_bytes())
    }

    fn overwrite_second_archive_replay(
        store: &RocksDbCanonicalStore,
        replay_bytes: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let archive_family = store
            .bounded_open
            .db
            .cf_handle(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)
            .ok_or("archive family absent")?;
        store.bounded_open.db.put_cf(
            &archive_family,
            encode_test_order_key(2, BlockHeight::new(2), BlockHash::from_bytes([22; 32])),
            encode_test_archive_record(
                replay_bytes,
                ChainEpochId::new(2),
                UnixTimestampMillis::new(1_750_000_000_002),
                None,
                None,
            )?,
        )?;
        Ok(())
    }

    fn displaced_archive_roots() -> BlockFinalNoteCommitmentRoots {
        BlockFinalNoteCommitmentRoots::new(
            BlockHeight::new(2),
            BlockHash::from_bytes([22; 32]),
            Some(FinalNoteCommitmentRoot::from_bytes([31; 32])),
            None,
            Some(FinalNoteCommitmentRoot::from_bytes([33; 32])),
        )
    }

    fn add_tree_state_checkpoint(
        block: &mut crate::CanonicalBuildBlock,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let header = &block.facts.block_header;
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        block.tree_state_checkpoint = Some(zinder_core::CommitmentTreeCheckpoint::new(
            BlockId::new(header.height, header.block_hash),
            u32::try_from(header.block_time)?,
            crate::canonical_store::test_checkpoint_frontiers(&activations, header.height),
        ));
        Ok(())
    }
}
