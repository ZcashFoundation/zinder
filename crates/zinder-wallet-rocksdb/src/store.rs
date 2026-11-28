//! Exact `RocksDB` wallet layout, admission, and reads.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fs,
    mem::size_of,
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    ops::Range,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use zinder_rocksdb_bulk_load::{SortedVariableValues, VariableValueSorter};

use rust_rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, BoundColumnFamily, Cache, ColumnFamilyDescriptor,
    DBCompressionType, DEFAULT_COLUMN_FAMILY_NAME, Direction, FlushOptions,
    IngestExternalFileOptions, IteratorMode, Options, ReadOptions, WriteBatch, WriteOptions,
    checkpoint::Checkpoint,
};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, CanonicalBlockFactsSequenceDigest, Network,
    TransparentAddressScriptHash, TransparentOutPoint, TransparentUtxoSetCommitment,
    UnixTimestampMillis, ValidatedCanonicalBlockReplay,
};
use zinder_store::{
    BoundedRocksDbOpen, CanonicalEventFence, CanonicalRetainedEvent, CanonicalStoreError,
    RocksDbIoMode, RocksDbOpenRole, RocksDbResourceBudget, build_block_based_table_factory,
    keep_blocks_of_dropped_table_files, open_bounded_rocksdb,
};
use zinder_wallet_projection::{
    StoredWalletSpentOutput, StoredWalletUnspentOutput, WALLET_PROJECTION_SCHEMA_VERSION,
    WALLET_PROJECTION_STORE_IDENTITY, WALLET_STORE_CONTROL_KEY, WalletAddressBalance,
    WalletAddressTransaction, WalletAddressTransactionHistoryPage, WalletAddressTransactionKey,
    WalletAddressUnspentOutputKey, WalletAddressUnspentOutputsPage, WalletBlockHash,
    WalletCanonicalSourceIdentity, WalletOutpointKey, WalletProjectionBuildLease,
    WalletProjectionBuildLeaseRequest, WalletProjectionBuildOwner, WalletProjectionBuildPlan,
    WalletProjectionBuildState, WalletProjectionDigestBuilder, WalletProjectionReadyEvidence,
    WalletProjectionRowFamily, WalletReorgUndo, WalletSpentOutput, WalletStoreControlRecord,
    WalletStoredTransactionPosition, WalletUnspentOutput, WalletUtxoSetSummary,
};

use crate::{
    RocksDbWalletError, WalletBuildLeaseHeartbeat, WalletColdReopenPhaseDurations,
    WalletColdValidationPhaseDurations, WalletTransitionFootprint, build_lock::WalletBuildLock,
    projection_load::PreparedWalletProjectionLoad,
};

#[cfg(test)]
use zinder_testkit::process_crash::crash_point;

#[cfg(not(test))]
const fn crash_point(_name: &str) {}

/// Exact clean wallet-store schema supported by this adapter.
pub const WALLET_ROCKSDB_SCHEMA_VERSION: u16 = WALLET_PROJECTION_SCHEMA_VERSION;

/// Cold-admitted identity and READY evidence for an owner-created wallet checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletOwnerCheckpointEvidence {
    /// Exact `RocksDB` database identity captured from the owner before the
    /// physical checkpoint was created and re-read from the cold checkpoint.
    ///
    /// This prevents a same-plan database swap between physical creation and
    /// cold admission from being mistaken for the owner's immutable copy.
    pub database_identity: Vec<u8>,
    /// Exact physical store identity admitted from the checkpoint.
    pub store_identity: &'static [u8],
    /// Exact physical schema admitted from the checkpoint.
    pub schema_version: u16,
    /// Immutable network admitted from the checkpoint.
    pub network: Network,
    /// Persisted READY evidence read from the cold-opened checkpoint.
    pub ready_evidence: WalletProjectionReadyEvidence,
}

/// Immutable context captured by a wallet owner during physical checkpoint
/// creation.
///
/// The value grants no filesystem access and can only cold-admit a checkpoint
/// whose database identity matches the source primary observed before the
/// checkpoint call.
#[derive(Clone, Debug)]
pub struct WalletOwnerCheckpointAdmission {
    network: Network,
    database_identity: Vec<u8>,
}

/// Explicit bounded resources for one non-serving wallet recovery admission.
///
/// The caller owns `staging_path`; admission creates and removes only its
/// sorter workspaces below that directory. A successful admission returns
/// immutable evidence and never returns a query-serving or mutable store
/// handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletRecoveryAdmissionConfig<'staging> {
    /// Bounded resources applied while the checkpoint is opened.
    pub resource_budget: RocksDbResourceBudget,
    /// Existing private directory used for bounded external-sort workspaces.
    pub staging_path: &'staging Path,
    /// Accounted memory ceiling applied independently to each validation sorter.
    pub max_sort_memory_bytes_per_sorter: u64,
    /// Temporary-run byte ceiling applied independently to each validation sorter.
    pub max_temporary_file_bytes_per_sorter: u64,
    /// Accounted memory ceiling for reconstructing retained reorg-undo effects.
    pub max_accounted_reorg_undo_bytes: u64,
}

/// Durable BUILDING wallet store and its lease-management boundary.
///
/// The value owns no open database handle. Each mutation cold-opens the
/// singleton control record, synchronously replaces it, and closes again, so
/// lease ownership survives process restarts and no caller can retain an
/// untracked mutable database handle.
#[derive(Clone, Debug)]
pub struct RocksDbWalletBuildStore {
    store_path: std::path::PathBuf,
    resource_budget: RocksDbResourceBudget,
    network: Network,
}

pub(crate) const TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY: &str = "transparent_unspent_output";
pub(crate) const TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY: &str =
    "transparent_unspent_output_by_address";
pub(crate) const TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY: &str = "transparent_spent_output";
pub(crate) const TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY: &str =
    "transparent_address_transaction";
pub(crate) const TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY: &str = "transparent_address_balance";
pub(crate) const REORG_UNDO_COLUMN_FAMILY: &str = "reorg_undo";
pub(crate) const BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY: &str = "block_hash_by_height";

pub(crate) const WALLET_DATA_COLUMN_FAMILIES: [(&str, WalletProjectionRowFamily); 7] = [
    (
        TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
        WalletProjectionRowFamily::TransparentUnspentOutput,
    ),
    (
        TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
        WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
    ),
    (
        TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
        WalletProjectionRowFamily::TransparentSpentOutput,
    ),
    (
        TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
        WalletProjectionRowFamily::TransparentAddressTransaction,
    ),
    (
        TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
        WalletProjectionRowFamily::TransparentAddressBalance,
    ),
    (
        REORG_UNDO_COLUMN_FAMILY,
        WalletProjectionRowFamily::ReorgUndo,
    ),
    (
        BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY,
        WalletProjectionRowFamily::BlockHashByHeight,
    ),
];
/// Bloom-equivalent bits per key of the point-lookup filters, about a 1%
/// false-positive rate.
const POINT_LOOKUP_FILTER_BITS_PER_KEY: f64 = 10.0;
/// Flushed and L0 files keep Bloom filters, which build faster; files below L0,
/// including bulk-built ones, use Ribbon, which needs about 30% less memory.
const POINT_LOOKUP_FILTER_BLOOM_BEFORE_LEVEL: i32 = 1;
const ADDRESS_UNSPENT_KEY_BYTES: usize = 72;
const ADDRESS_TRANSACTION_KEY_BYTES: usize = 40;

/// A fresh BUILDING wallet store that cannot be admitted by query processes.
pub(crate) struct RocksDbWalletBuilder {
    bounded_open: BoundedRocksDbOpen,
    store_path: std::path::PathBuf,
    resource_budget: RocksDbResourceBudget,
    control: WalletStoreControlRecord,
    lease: WalletProjectionBuildLease,
}

/// A cold-reopened BUILDING store whose rows have not yet been validated.
pub(crate) struct ColdRocksDbWalletBuild {
    bounded_open: BoundedRocksDbOpen,
    control: WalletStoreControlRecord,
    lease: WalletProjectionBuildLease,
}

/// A cold-validated BUILDING store carrying the evidence it may publish.
pub(crate) struct ValidatedRocksDbWalletBuild {
    bounded_open: BoundedRocksDbOpen,
    control: WalletStoreControlRecord,
    lease: WalletProjectionBuildLease,
    ready_evidence: WalletProjectionReadyEvidence,
    validation_evidence: WalletColdValidationEvidence,
}

/// Explicit bounded resources for one cold digest and row-count readback.
#[derive(Clone, Copy)]
pub(crate) struct WalletColdValidationConfig {
    pub(crate) max_accounted_reorg_undo_bytes: u64,
    /// Requested hash-prefix key ranges the primary output and
    /// address-transaction families are each read back in, each on its own
    /// thread.
    pub(crate) family_scan_ranges: NonZeroU32,
    /// Fewest rows each requested scan range must cover; smaller stores use
    /// fewer ranges.
    pub(crate) min_rows_per_scan_range: NonZeroU64,
}

/// Fewest rows a production cold-validation scan range covers.
///
/// A family with fewer rows than this is read on one thread.
pub(crate) const MIN_ROWS_PER_SCAN_RANGE: NonZeroU64 = match NonZeroU64::new(1 << 16) {
    Some(rows) => rows,
    None => NonZeroU64::MIN,
};

/// Explicit bounded resources for one independent cold semantic re-derivation.
#[derive(Clone, Copy)]
pub(crate) struct WalletSemanticValidationConfig<'staging> {
    pub(crate) staging_path: &'staging Path,
    pub(crate) max_sort_memory_bytes_per_sorter: u64,
    pub(crate) max_temporary_file_bytes_per_sorter: u64,
    pub(crate) max_accounted_reorg_undo_bytes: u64,
}

/// Bounded work evidence from cold digest and row-count readback.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WalletColdValidationEvidence {
    pub(crate) peak_accounted_reorg_undo_bytes: u64,
    pub(crate) max_accounted_reorg_undo_bytes: u64,
    pub(crate) random_read_count: u64,
    pub(crate) phase_durations: WalletColdValidationPhaseDurations,
}

const BLOCK_HASH_CACHE_CHUNK_SHIFT: u32 = 7;
const BLOCK_HASH_CACHE_CHUNK_LEN: usize = 1 << BLOCK_HASH_CACHE_CHUNK_SHIFT;

#[derive(Clone, Debug)]
struct BlockHashChunk {
    present: u128,
    hashes: [[u8; 32]; BLOCK_HASH_CACHE_CHUNK_LEN],
}

impl BlockHashChunk {
    const fn empty() -> Self {
        Self {
            present: 0,
            hashes: [[0_u8; 32]; BLOCK_HASH_CACHE_CHUNK_LEN],
        }
    }

    const fn chunk_id(height: BlockHeight) -> u32 {
        height.value() >> BLOCK_HASH_CACHE_CHUNK_SHIFT
    }

    fn offset(height: BlockHeight) -> usize {
        usize::try_from(height.value() & 0x7f).unwrap_or(0)
    }

    fn get(&self, height: BlockHeight) -> Option<BlockHash> {
        let offset = Self::offset(height);
        if (self.present & (1_u128 << offset)) == 0 {
            return None;
        }
        Some(BlockHash::from_bytes(self.hashes[offset]))
    }

    fn insert(&mut self, height: BlockHeight, hash: BlockHash) {
        let offset = Self::offset(height);
        self.present |= 1_u128 << offset;
        self.hashes[offset] = hash.as_bytes();
    }
}

#[derive(Debug, Default)]
struct BlockHashChunkCacheState {
    chunks: HashMap<u32, Box<BlockHashChunk>>,
    fifo: VecDeque<u32>,
}

/// Bounded FIFO-evicting chunk cache of `block_hash_by_height` rows.
#[derive(Debug)]
pub(crate) struct BlockHashChunkCache {
    max_chunks: usize,
    state: parking_lot::RwLock<BlockHashChunkCacheState>,
}

impl BlockHashChunkCache {
    pub(crate) fn for_bounded_open(bounded_open: &BoundedRocksDbOpen) -> Self {
        let chunk_bytes = BLOCK_HASH_CACHE_CHUNK_LEN * 32 + size_of::<u128>();
        let cache_capacity = bounded_open.block_cache.get_capacity();
        let budget_bytes = (cache_capacity / 4).clamp(1024 * 1024, 128 * 1024 * 1024);
        let max_chunks = (budget_bytes / chunk_bytes).max(16);
        Self {
            max_chunks,
            state: parking_lot::RwLock::new(BlockHashChunkCacheState::default()),
        }
    }

    #[cfg(test)]
    fn with_max_chunks(max_chunks: usize) -> Self {
        Self {
            max_chunks: max_chunks.max(1),
            state: parking_lot::RwLock::new(BlockHashChunkCacheState::default()),
        }
    }

    pub(crate) fn invalidate_from(&self, min_height: BlockHeight) {
        let first_invalid_chunk = BlockHashChunk::chunk_id(min_height);
        let mut guard = self.state.write();
        guard
            .chunks
            .retain(|chunk_id, _| *chunk_id < first_invalid_chunk);
        guard
            .fifo
            .retain(|chunk_id| *chunk_id < first_invalid_chunk);
    }

    fn insert_chunk(&self, chunk_id: u32, chunk: Box<BlockHashChunk>) {
        let mut guard = self.state.write();
        if let Some(existing) = guard.chunks.get_mut(&chunk_id) {
            *existing = chunk;
            return;
        }
        while guard.chunks.len() >= self.max_chunks {
            let Some(oldest_id) = guard.fifo.pop_front() else {
                guard.chunks.clear();
                break;
            };
            guard.chunks.remove(&oldest_id);
        }
        guard.chunks.insert(chunk_id, chunk);
        guard.fifo.push_back(chunk_id);
    }
}

fn lookup_cached_block_hash<'cache>(
    cache: &'cache BlockHashChunkCacheState,
    last_chunk: &mut Option<(u32, &'cache BlockHashChunk)>,
    height: BlockHeight,
) -> Option<BlockHash> {
    let chunk_id = BlockHashChunk::chunk_id(height);
    if let Some((cached_id, chunk)) = *last_chunk
        && cached_id == chunk_id
    {
        return chunk.get(height);
    }
    let chunk = cache.chunks.get(&chunk_id)?;
    *last_chunk = Some((chunk_id, chunk.as_ref()));
    chunk.get(height)
}

/// One admitted READY wallet `RocksDB` store.
///
/// The private database handle prevents consumers from bypassing the wallet
/// row codecs or mutating a store after admission.
pub struct RocksDbWalletStore {
    pub(crate) bounded_open: BoundedRocksDbOpen,
    pub(crate) control: WalletStoreControlRecord,
    pub(crate) ready_evidence: WalletProjectionReadyEvidence,
    pub(crate) block_hash_cache: BlockHashChunkCache,
    /// Writer generation of the READY publication whose durable undo suffix
    /// this handle verified, by reading every retained row or by publishing an
    /// append planned on a verified suffix. `None` until the handle's first
    /// transition reads the whole suffix.
    pub(crate) verified_undo_suffix_generation: Option<u64>,
    pub(crate) last_transition_footprint: Option<WalletTransitionFootprint>,
}

/// A cold-admitted READY wallet held only for canonical following.
///
/// Unlike [`RocksDbWalletStore`], this type intentionally cannot serve query
/// reads. It exposes the persisted source evidence and atomic following
/// transitions, then requires an exact source match through
/// [`Self::into_ready_store`] before it can become a query-serving store.
pub struct RocksDbWalletFollowingStore {
    store: RocksDbWalletStore,
}

impl RocksDbWalletBuildStore {
    /// Creates a fresh, non-queryable BUILDING store without granting ownership.
    ///
    /// A caller must subsequently acquire a lease before writing or promoting
    /// projection state. Existing stores, including older pre-release control
    /// layouts, are refused rather than migrated. The store is initialized in a
    /// sibling directory and renamed into place after its BUILDING control
    /// record is durable, so an interrupted creation never leaves the wallet
    /// path holding a store without one.
    pub fn create_fresh(
        path: impl AsRef<Path>,
        network: Network,
        target_source: WalletCanonicalSourceIdentity,
        supported_reorg_depth: u32,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<Self, RocksDbWalletError> {
        validate_resource_budget(resource_budget)?;
        let path = path.as_ref();
        if path.symlink_metadata().is_ok() {
            return Err(RocksDbWalletError::PathNotFresh {
                path: path.to_path_buf(),
            });
        }
        let initialization_path = crate::build::store_initialization_path(path);
        create_fresh_directory(&initialization_path)?;
        crash_point("wallet_projection.build_store.after_initialization_directory");
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Primary {
                path: &initialization_path,
            },
            resource_budget,
            wallet_column_family_descriptors,
        )
        .map_err(|source| RocksDbWalletError::rocksdb("fresh build-store open", source))?;
        let control = WalletStoreControlRecord {
            network,
            supported_reorg_depth,
            writer_generation: 0,
            build_lease: None,
            build_state: WalletProjectionBuildState::Building(
                WalletProjectionBuildPlan::complete_history(target_source.source_position()),
            ),
        };
        write_control_sync(&bounded_open, &control)?;
        drop(bounded_open);
        crash_point("wallet_projection.build_store.before_wallet_path_rename");
        fs::rename(&initialization_path, path).map_err(|source| {
            RocksDbWalletError::PathUnavailable {
                path: path.to_path_buf(),
                source,
            }
        })?;
        let store_path =
            fs::canonicalize(path).map_err(|source| RocksDbWalletError::PathUnavailable {
                path: path.to_path_buf(),
                source,
            })?;
        Ok(Self {
            store_path,
            resource_budget,
            network,
        })
    }

    /// Reopens an existing BUILDING store after exact schema and network admission.
    pub fn open(
        path: impl AsRef<Path>,
        expected_network: Network,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<Self, RocksDbWalletError> {
        validate_resource_budget(resource_budget)?;
        let path = path.as_ref();
        let store_path =
            fs::canonicalize(path).map_err(|source| RocksDbWalletError::PathUnavailable {
                path: path.to_path_buf(),
                source,
            })?;
        require_exact_column_families(&store_path)?;
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::ExistingPrimary { path: &store_path },
            resource_budget,
            wallet_column_family_descriptors,
        )
        .map_err(|source| RocksDbWalletError::rocksdb("build-store reopen", source))?;
        let control = decode_only_control(&bounded_open)?;
        require_building_control(&control, &store_path, expected_network)?;
        drop(bounded_open);
        Ok(Self {
            store_path,
            resource_budget,
            network: expected_network,
        })
    }

    /// Acquires exclusive durable ownership of the BUILDING store.
    ///
    /// A live lease held by a different owner is never shared or stolen: it
    /// refuses the request until it expires. The owner of a live lease may
    /// reclaim it immediately, which is how a restarted builder recovers
    /// after a crash. Every acquisition, including a reclaim, persists a
    /// strictly higher generation, so a capability held by an earlier
    /// incarnation can no longer renew, release, or publish.
    pub fn try_acquire_lease(
        &self,
        request: WalletProjectionBuildLeaseRequest,
        now: UnixTimestampMillis,
    ) -> Result<WalletProjectionBuildLease, RocksDbWalletError> {
        let (bounded_open, control) = self.open_building_control()?;
        validate_lease_request(&control, request, now)?;
        require_no_live_lease_of_another_owner(&control, request.owner(), now)?;
        let generation = control
            .writer_generation
            .checked_add(1)
            .ok_or(RocksDbWalletError::WalletProjectionBuildLeaseGenerationOverflow)?;
        let lease = WalletProjectionBuildLease::from_request(request, generation, self.network);
        let next_control = WalletStoreControlRecord {
            writer_generation: generation,
            build_lease: Some(lease),
            ..control
        };
        write_control_sync(&bounded_open, &next_control)?;
        let persisted = decode_only_control(&bounded_open)?;
        if persisted != next_control {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "projection build lease differs after synchronous acquisition",
            });
        }
        Ok(lease)
    }

    /// Extends an active lease owned by the supplied durable capability.
    pub fn renew_lease(
        &self,
        lease: WalletProjectionBuildLease,
        expires_at: UnixTimestampMillis,
        now: UnixTimestampMillis,
    ) -> Result<WalletProjectionBuildLease, RocksDbWalletError> {
        let (bounded_open, control) = self.open_building_control()?;
        let persisted = authorize_active_lease(&control, lease, now)?;
        if expires_at <= now {
            return Err(RocksDbWalletError::WalletProjectionBuildLeaseExpiryNotFuture);
        }
        if expires_at <= persisted.expires_at() {
            return Err(RocksDbWalletError::WalletProjectionBuildLeaseRenewalNotExtended);
        }
        let renewed = persisted.renewed(expires_at);
        let next_control = WalletStoreControlRecord {
            build_lease: Some(renewed),
            ..control
        };
        write_control_sync(&bounded_open, &next_control)?;
        let persisted_control = decode_only_control(&bounded_open)?;
        if persisted_control != next_control {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "projection build lease differs after synchronous renewal",
            });
        }
        Ok(renewed)
    }

    /// Releases an active lease without changing the BUILDING plan or generation.
    pub fn release_lease(
        &self,
        lease: WalletProjectionBuildLease,
        now: UnixTimestampMillis,
    ) -> Result<(), RocksDbWalletError> {
        let (bounded_open, control) = self.open_building_control()?;
        let _persisted = authorize_active_lease(&control, lease, now)?;
        let next_control = WalletStoreControlRecord {
            build_lease: None,
            ..control
        };
        write_control_sync(&bounded_open, &next_control)?;
        let persisted = decode_only_control(&bounded_open)?;
        if persisted != next_control {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "projection build lease differs after synchronous release",
            });
        }
        Ok(())
    }

    /// Deletes an exact admitted BUILDING store and its deterministic owned staging path.
    ///
    /// READY stores are refused. This operation deliberately provides the only
    /// recovery path for an abandoned pre-release build, rather than allowing
    /// callers to raw-delete ambiguous wallet paths.
    ///
    /// `owner` is the caller's build-owner identity. A live lease held by a
    /// different owner refuses the discard until it expires; the lease's own
    /// owner, typically a builder restarted after a crash, may discard
    /// immediately. A build of the same path still running in another process
    /// refuses the discard whatever its owner.
    pub fn discard_unpublished(
        self,
        owner: WalletProjectionBuildOwner,
        now: UnixTimestampMillis,
    ) -> Result<(), RocksDbWalletError> {
        let _build_lock = WalletBuildLock::acquire(&self.store_path)?;
        let (bounded_open, control) = self.open_building_control()?;
        require_building_control(&control, &self.store_path, self.network)?;
        require_no_live_lease_of_another_owner(&control, owner, now)?;
        drop(bounded_open);

        let staging_path = crate::build::projection_staging_path(&self.store_path);
        if staging_path.exists() {
            fs::remove_dir_all(&staging_path).map_err(|source| {
                RocksDbWalletError::PathUnavailable {
                    path: staging_path,
                    source,
                }
            })?;
        }
        fs::remove_dir_all(&self.store_path).map_err(|source| RocksDbWalletError::PathUnavailable {
            path: self.store_path,
            source,
        })
    }

    fn open_building_control(
        &self,
    ) -> Result<(BoundedRocksDbOpen, WalletStoreControlRecord), RocksDbWalletError> {
        require_exact_column_families(&self.store_path)?;
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::ExistingPrimary {
                path: &self.store_path,
            },
            self.resource_budget,
            wallet_column_family_descriptors,
        )
        .map_err(|source| RocksDbWalletError::rocksdb("build lease open", source))?;
        let control = decode_only_control(&bounded_open)?;
        require_building_control(&control, &self.store_path, self.network)?;
        Ok((bounded_open, control))
    }
}

impl RocksDbWalletBuilder {
    /// Creates a fresh store and durably publishes its BUILDING plan.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "consuming the build-store capability makes one builder initialization an ownership boundary"
    )]
    pub(crate) fn create_fresh(
        build_store: RocksDbWalletBuildStore,
        lease_request: WalletProjectionBuildLeaseRequest,
        now: UnixTimestampMillis,
    ) -> Result<Self, RocksDbWalletError> {
        let lease = build_store.try_acquire_lease(lease_request, now)?;
        let store_path = build_store.store_path.clone();
        let resource_budget = build_store.resource_budget;
        let bounded_open = match open_bounded_rocksdb(
            RocksDbOpenRole::ExistingPrimary { path: &store_path },
            resource_budget,
            wallet_column_family_descriptors,
        )
        .map_err(|source| RocksDbWalletError::rocksdb("fresh leased build open", source))
        {
            Ok(bounded_open) => bounded_open,
            Err(open_error) => {
                let release_result = build_store.release_lease(lease, now);
                return match release_result {
                    Ok(())
                    | Err(
                        RocksDbWalletError::WalletProjectionBuildLeaseExpired { .. }
                        | RocksDbWalletError::WalletProjectionBuildLeaseMissing
                        | RocksDbWalletError::WalletProjectionBuildLeaseOwnerMismatch { .. }
                        | RocksDbWalletError::WalletProjectionBuildLeaseGenerationMismatch { .. }
                        | RocksDbWalletError::WalletProjectionBuildLeaseCanonicalAnchorMismatch {
                            ..
                        },
                    ) => Err(open_error),
                    Err(cleanup_error) => Err(RocksDbWalletError::BuildLeaseCleanup {
                        build_error: Box::new(open_error),
                        cleanup_error: Box::new(cleanup_error),
                    }),
                };
            }
        };
        let control = decode_only_control(&bounded_open)?;
        Ok(Self {
            bounded_open,
            store_path,
            resource_budget,
            control,
            lease,
        })
    }

    /// Returns column-family options identical to those used by the BUILDING store.
    pub(crate) fn column_family_options(&self) -> WalletColumnFamilyOptions {
        WalletColumnFamilyOptions::new(&self.bounded_open.block_cache, self.resource_budget)
    }

    #[cfg(test)]
    pub(crate) const fn test_bounded_open(&self) -> &BoundedRocksDbOpen {
        &self.bounded_open
    }

    pub(crate) const fn lease(&self) -> WalletProjectionBuildLease {
        self.lease
    }

    pub(crate) fn heartbeat(
        &mut self,
        heartbeat: WalletBuildLeaseHeartbeat,
    ) -> Result<(), RocksDbWalletError> {
        apply_build_lease_heartbeat(
            &self.bounded_open,
            &mut self.control,
            &mut self.lease,
            heartbeat,
        )
    }

    /// Ingests seven externally prepared SST families while the store is BUILDING.
    pub(crate) fn ingest_projection_ssts(
        &self,
        prepared: &mut PreparedWalletProjectionLoad,
    ) -> Result<(), RocksDbWalletError> {
        let WalletProjectionBuildState::Building(plan) = &self.control.build_state else {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "projection SST ingestion requires a BUILDING control record",
            });
        };
        if prepared.network != self.control.network
            || prepared.supported_reorg_depth != self.control.supported_reorg_depth
            || prepared.tip != plan.target_source_position.tip
        {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "prepared projection differs from the BUILDING plan",
            });
        }
        for family in std::mem::take(&mut prepared.families) {
            if family.paths.is_empty() {
                continue;
            }
            let column_family = column_family(&self.bounded_open, family.name)?;
            let mut options = IngestExternalFileOptions::default();
            options.set_move_files(true);
            options.set_snapshot_consistency(true);
            options.set_allow_global_seqno(false);
            options.set_allow_blocking_flush(false);
            self.bounded_open
                .db
                .ingest_external_file_cf_opts(&column_family, &options, family.paths)
                .map_err(|source| {
                    RocksDbWalletError::rocksdb("wallet projection external SST ingestion", source)
                })?;
        }
        Ok(())
    }

    /// Flushes, closes, and cold-reopens a complete BUILDING store.
    pub(crate) fn reopen_for_validation(
        self,
    ) -> Result<(ColdRocksDbWalletBuild, WalletColdReopenPhaseDurations), RocksDbWalletError> {
        let close_started = Instant::now();
        flush_complete_build(&self.bounded_open)?;
        let database_identity = self
            .bounded_open
            .db
            .get_db_identity()
            .map_err(|source| RocksDbWalletError::rocksdb("database identity read", source))?;
        let store_path = self.store_path.clone();
        let resource_budget = self.resource_budget;
        let expected_control = self.control.clone();
        let lease = self.lease;
        drop(self);
        let flush_and_close = close_started.elapsed();

        let reopen_started = Instant::now();
        require_exact_column_families(&store_path)?;
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::ExistingPrimary { path: &store_path },
            resource_budget,
            wallet_column_family_descriptors,
        )
        .map_err(|source| RocksDbWalletError::rocksdb("cold publication reopen", source))?;
        let reopened_identity = bounded_open.db.get_db_identity().map_err(|source| {
            RocksDbWalletError::rocksdb("reopened database identity read", source)
        })?;
        if reopened_identity != database_identity {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "database identity changed before cold publication validation",
            });
        }
        let reopened_control = decode_only_control(&bounded_open)?;
        if reopened_control != expected_control {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "BUILDING control changed before cold publication validation",
            });
        }
        Ok((
            ColdRocksDbWalletBuild {
                bounded_open,
                control: reopened_control,
                lease,
            },
            WalletColdReopenPhaseDurations {
                flush_and_close,
                cold_reopen: reopen_started.elapsed(),
            },
        ))
    }
}

impl ColdRocksDbWalletBuild {
    /// Reads every durable row back against the construction evidence and binds
    /// the only publishable READY evidence.
    ///
    /// The readback recomputes the order-independent accumulator and the six
    /// family row counts from the reopened store and requires them to equal the
    /// values emitted while the SST files were written. It also re-checks every
    /// invariant that a streaming scan can establish without sorting a family a
    /// second time.
    pub(crate) fn validate_rows(
        self,
        ready_evidence: WalletProjectionReadyEvidence,
        config: WalletColdValidationConfig,
    ) -> Result<ValidatedRocksDbWalletBuild, RocksDbWalletError> {
        let WalletProjectionBuildState::Building(plan) = &self.control.build_state else {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "cold validation requires a BUILDING control record",
            });
        };
        if ready_evidence.source_position != plan.target_source_position {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "READY source position differs from the BUILDING target",
            });
        }
        let validation_evidence = validate_ready_rows_by_readback(
            &self.bounded_open,
            self.control.network,
            &ready_evidence,
            config,
        )?;
        Ok(ValidatedRocksDbWalletBuild {
            bounded_open: self.bounded_open,
            control: self.control,
            lease: self.lease,
            ready_evidence,
            validation_evidence,
        })
    }
}

impl ValidatedRocksDbWalletBuild {
    /// Returns the bounded work observed before this typestate was admitted.
    pub(crate) const fn validation_evidence(&self) -> WalletColdValidationEvidence {
        self.validation_evidence
    }

    pub(crate) const fn lease(&self) -> WalletProjectionBuildLease {
        self.lease
    }

    pub(crate) fn heartbeat(
        &mut self,
        heartbeat: WalletBuildLeaseHeartbeat,
    ) -> Result<(), RocksDbWalletError> {
        apply_build_lease_heartbeat(
            &self.bounded_open,
            &mut self.control,
            &mut self.lease,
            heartbeat,
        )
    }

    /// Atomically replaces a lease-authorized BUILDING control with cold-validated READY evidence.
    pub(crate) fn publish_ready_at(
        self,
        now: UnixTimestampMillis,
    ) -> Result<RocksDbWalletStore, RocksDbWalletError> {
        let persisted_control = decode_only_control(&self.bounded_open)?;
        let persisted_lease = authorize_active_lease(&persisted_control, self.lease, now)?;
        if persisted_lease.pinned_canonical_anchor()
            != WalletCanonicalSourceIdentity::from_ready_evidence(&self.ready_evidence)
        {
            return Err(
                RocksDbWalletError::WalletProjectionBuildLeaseCanonicalAnchorMismatch {
                    reason: "READY evidence differs from the pinned canonical anchor",
                },
            );
        }
        let ready_evidence = self.ready_evidence;
        let ready_control = WalletStoreControlRecord {
            network: self.control.network,
            supported_reorg_depth: self.control.supported_reorg_depth,
            writer_generation: self.control.writer_generation,
            build_lease: None,
            build_state: WalletProjectionBuildState::Ready(ready_evidence.clone()),
        };
        write_control_sync(&self.bounded_open, &ready_control)?;
        let persisted_control = decode_only_control(&self.bounded_open)?;
        if persisted_control != ready_control {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "READY control differs after its synchronous publication",
            });
        }
        let block_hash_cache = BlockHashChunkCache::for_bounded_open(&self.bounded_open);
        Ok(RocksDbWalletStore {
            bounded_open: self.bounded_open,
            control: ready_control,
            ready_evidence,
            block_hash_cache,
            verified_undo_suffix_generation: None,
            last_transition_footprint: None,
        })
    }
}

impl RocksDbWalletStore {
    /// Opens an existing READY store after exact schema and control admission.
    pub fn open_ready(
        path: impl AsRef<Path>,
        expected_network: Network,
        expected_source: WalletCanonicalSourceIdentity,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<Self, RocksDbWalletError> {
        Self::open_ready_for_following(path, expected_network, resource_budget)?
            .into_ready_store(expected_source)
    }

    /// Cold-opens READY state for canonical following without serving it.
    ///
    /// This path performs the same exact on-disk schema, singleton-control,
    /// network, READY lifecycle, and serialized-accumulator decoding as
    /// [`Self::open_ready`], but deliberately does not compare the persisted
    /// source fence with a potentially newer canonical writer. It does not
    /// rescan the row families; full row-to-accumulator validation remains the
    /// bounded BUILDING-to-READY publication check. The first transition on
    /// the returned handle reads and decodes every retained undo row before
    /// it publishes, and a missing, corrupt, or disconnected row fails it.
    ///
    /// The returned [`RocksDbWalletFollowingStore`] exposes no query methods;
    /// callers must converge it through retained canonical events and then
    /// call [`RocksDbWalletFollowingStore::into_ready_store`] with an exact
    /// source identity before serving queries.
    pub fn open_ready_for_following(
        path: impl AsRef<Path>,
        expected_network: Network,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<RocksDbWalletFollowingStore, RocksDbWalletError> {
        validate_resource_budget(resource_budget)?;
        let path = path.as_ref();
        let store_path =
            fs::canonicalize(path).map_err(|source| RocksDbWalletError::PathUnavailable {
                path: path.to_path_buf(),
                source,
            })?;
        require_exact_column_families(&store_path)?;
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::ExistingPrimary { path: &store_path },
            resource_budget,
            wallet_column_family_descriptors,
        )
        .map_err(|source| RocksDbWalletError::rocksdb("ready open", source))?;
        require_exact_column_families(&store_path)?;
        let control = decode_only_control(&bounded_open)?;
        if control.network != expected_network {
            return Err(RocksDbWalletError::NetworkMismatch {
                expected: expected_network,
                observed: control.network,
            });
        }
        let WalletProjectionBuildState::Ready(ready_evidence) = &control.build_state else {
            return Err(RocksDbWalletError::StoreNotReady { path: store_path });
        };
        let block_hash_cache = BlockHashChunkCache::for_bounded_open(&bounded_open);
        Ok(RocksDbWalletFollowingStore {
            store: Self {
                bounded_open,
                ready_evidence: ready_evidence.clone(),
                control,
                block_hash_cache,
                verified_undo_suffix_generation: None,
                last_transition_footprint: None,
            },
        })
    }

    /// Returns the decoded READY evidence that admitted this store.
    #[must_use]
    pub const fn ready_evidence(&self) -> &WalletProjectionReadyEvidence {
        &self.ready_evidence
    }

    /// Returns the size of the last transition this handle published, or
    /// `None` before its first.
    #[must_use]
    pub const fn last_transition_footprint(&self) -> Option<WalletTransitionFootprint> {
        self.last_transition_footprint
    }

    /// Returns the store's immutable network.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.control.network
    }

    /// Returns one current output by exact outpoint.
    pub fn find_unspent_output(
        &self,
        outpoint: TransparentOutPoint,
    ) -> Result<Option<WalletUnspentOutput>, RocksDbWalletError> {
        let key = WalletOutpointKey::new(outpoint);
        let Some(stored) = self.read_optional(
            TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
            key.as_bytes(),
            |encoded| StoredWalletUnspentOutput::decode_value(key, encoded),
        )?
        else {
            return Ok(None);
        };
        let block_hash = self.require_block_hash(stored.created_at.block_height)?;
        Ok(Some(stored.into_unspent(block_hash)))
    }

    /// Returns one historical spent output by exact outpoint.
    pub fn find_spent_output(
        &self,
        outpoint: TransparentOutPoint,
    ) -> Result<Option<WalletSpentOutput>, RocksDbWalletError> {
        let key = WalletOutpointKey::new(outpoint);
        let Some(stored) = self.read_optional(
            TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
            key.as_bytes(),
            |encoded| StoredWalletSpentOutput::decode_value(key, encoded),
        )?
        else {
            return Ok(None);
        };
        let created_hash = self.require_block_hash(stored.output.created_at.block_height)?;
        let spent_hash = if stored.spent_at.block_height == stored.output.created_at.block_height {
            created_hash
        } else {
            self.require_block_hash(stored.spent_at.block_height)?
        };
        Ok(Some(stored.into_spent(created_hash, spent_hash)))
    }

    /// Resolves one exact address-ordered unspent-output index key.
    pub fn find_unspent_output_by_address_key(
        &self,
        key: WalletAddressUnspentOutputKey,
    ) -> Result<Option<WalletUnspentOutput>, RocksDbWalletError> {
        let family = column_family(
            &self.bounded_open,
            TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
        )?;
        let Some(encoded_index) = self
            .bounded_open
            .db
            .get_cf(&family, key.as_bytes())
            .map_err(|source| RocksDbWalletError::rocksdb("address unspent index read", source))?
        else {
            return Ok(None);
        };
        if !encoded_index.is_empty() {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "address unspent index values must be empty",
            });
        }
        let output = self.find_unspent_output(key.outpoint())?.ok_or(
            RocksDbWalletError::AdmissionChanged {
                reason: "address unspent index references a missing primary output",
            },
        )?;
        if output.address_script_hash != key.address_script_hash()
            || output.created_at.block.height != key.creation_height()
        {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "address unspent index does not match its primary output",
            });
        }
        Ok(Some(output))
    }

    /// Returns one bounded page of current outputs for an address.
    ///
    /// Rows are ordered by creation height and outpoint. `after` is exclusive:
    /// pass the prior page's `next_page_after` unchanged to continue without
    /// repeating or skipping a row. The non-zero 16-bit page size bounds both
    /// work and returned memory to at most 65,535 outputs.
    pub fn address_unspent_outputs_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        after: Option<WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressUnspentOutputsPage, RocksDbWalletError> {
        self.address_unspent_outputs_page_from_height(
            address_script_hash,
            BlockHeight::new(0),
            after,
            page_size,
        )
    }

    /// Returns one bounded page of current outputs for an address at or above
    /// `start_height`.
    ///
    /// The page walks the address index first and then reads every primary
    /// row it names in one batched lookup, so a page costs one sequential
    /// index scan plus one multi-get rather than one point read per output.
    pub fn address_unspent_outputs_page_from_height(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        start_height: BlockHeight,
        after: Option<WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressUnspentOutputsPage, RocksDbWalletError> {
        if after.is_some_and(|key| key.address_script_hash() != address_script_hash) {
            return Err(RocksDbWalletError::ContinuationAddressMismatch {
                index: TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
            });
        }
        let (index_keys, more_rows) =
            self.address_unspent_index_page(address_script_hash, start_height, after, page_size)?;
        let outputs = self.unspent_outputs_for_index_keys(address_script_hash, &index_keys)?;
        Ok(WalletAddressUnspentOutputsPage {
            outputs,
            next_page_after: if more_rows {
                index_keys.last().copied()
            } else {
                None
            },
        })
    }

    /// Returns up to `page_size` address-index keys after `after`, and whether
    /// the address has more rows beyond them.
    fn address_unspent_index_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        start_height: BlockHeight,
        after: Option<WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<(Vec<WalletAddressUnspentOutputKey>, bool), RocksDbWalletError> {
        let family = column_family(
            &self.bounded_open,
            TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
        )?;
        let address_prefix = address_script_hash.as_bytes();
        let lower_bound =
            WalletAddressUnspentOutputKey::first_at_or_after(address_script_hash, start_height);
        let start = after
            .as_ref()
            .map_or(lower_bound.as_bytes().as_slice(), |key| {
                key.as_bytes().as_slice()
            });
        // Every index key of the address sorts below its prefix followed by
        // more 0xFF bytes than a key carries after the prefix, so the scan
        // stops at the address boundary instead of stepping over the deleted
        // rows of the addresses after it.
        let mut read_options = ReadOptions::default();
        read_options.set_iterate_upper_bound(
            [
                address_prefix.as_slice(),
                &[u8::MAX; ADDRESS_UNSPENT_KEY_BYTES],
            ]
            .concat(),
        );
        let mut index_keys = Vec::with_capacity(usize::from(page_size.get()));
        for row in self.bounded_open.db.iterator_cf_opt(
            &family,
            read_options,
            IteratorMode::From(start, Direction::Forward),
        ) {
            let (key_bytes, encoded_index) = row.map_err(|source| {
                RocksDbWalletError::rocksdb("address unspent page scan", source)
            })?;
            if !key_bytes.starts_with(&address_prefix) {
                break;
            }
            if after.is_some_and(|key| key_bytes.as_ref() <= key.as_bytes().as_slice()) {
                continue;
            }
            if index_keys.len() == usize::from(page_size.get()) {
                return Ok((index_keys, true));
            }
            let key = WalletAddressUnspentOutputKey::decode(&key_bytes)?;
            if !encoded_index.is_empty() {
                return Err(RocksDbWalletError::AdmissionChanged {
                    reason: "address unspent index values must be empty",
                });
            }
            index_keys.push(key);
        }
        Ok((index_keys, false))
    }

    /// Reads the primary output each address-index key names, in index order.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "holds one shared block-hash read lock across the page batch to avoid relocking per output"
    )]
    fn unspent_outputs_for_index_keys(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        index_keys: &[WalletAddressUnspentOutputKey],
    ) -> Result<Vec<WalletUnspentOutput>, RocksDbWalletError> {
        if index_keys.is_empty() {
            return Ok(Vec::new());
        }
        let family = column_family(&self.bounded_open, TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY)?;
        let outpoint_keys: Vec<WalletOutpointKey> = index_keys
            .iter()
            .map(|key| WalletOutpointKey::new(key.outpoint()))
            .collect();
        let rows = self.bounded_open.db.batched_multi_get_cf(
            &family,
            outpoint_keys.iter().map(WalletOutpointKey::as_bytes),
            false,
        );
        let mut outputs = Vec::with_capacity(index_keys.len());
        let mut unresolved = Vec::new();
        {
            let cache_guard = self.block_hash_cache.state.read();
            let mut last_chunk = None;
            for ((index_key, outpoint_key), row) in index_keys.iter().zip(&outpoint_keys).zip(rows)
            {
                let encoded = row
                    .map_err(|source| {
                        RocksDbWalletError::rocksdb("address unspent primary read", source)
                    })?
                    .ok_or(RocksDbWalletError::AdmissionChanged {
                        reason: "address unspent index references a missing primary output",
                    })?;
                let output = StoredWalletUnspentOutput::decode_value(*outpoint_key, &encoded)?;
                if output.address_script_hash != address_script_hash
                    || output.created_at.block_height != index_key.creation_height()
                {
                    return Err(RocksDbWalletError::AdmissionChanged {
                        reason: "address unspent index does not match its primary output",
                    });
                }
                let height = output.created_at.block_height;
                let block_hash = lookup_cached_block_hash(&cache_guard, &mut last_chunk, height)
                    .unwrap_or_else(|| {
                        unresolved.push((outputs.len(), height));
                        BlockHash::from_bytes([0_u8; 32])
                    });
                outputs.push(output.into_unspent(block_hash));
            }
        }
        if !unresolved.is_empty() {
            self.resolve_missing_block_hashes(&mut unresolved, |index, hash| {
                outputs[index].created_at.block.hash = hash;
            })?;
        }
        Ok(outputs)
    }

    /// Returns one exact address-transaction row.
    pub fn find_address_transaction(
        &self,
        key: WalletAddressTransactionKey,
    ) -> Result<Option<WalletAddressTransaction>, RocksDbWalletError> {
        let Some(transaction_id) = self.read_optional(
            TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
            key.as_bytes(),
            |encoded| WalletAddressTransaction::decode_stored_value(key, encoded),
        )?
        else {
            return Ok(None);
        };
        let block_hash = self.require_block_hash(key.block_height())?;
        Ok(Some(WalletAddressTransaction::new(
            key,
            transaction_id,
            block_hash,
        )))
    }

    /// Returns one bounded page of transaction history for an address.
    ///
    /// Rows are ordered by block height and block-local transaction index.
    /// `after` is exclusive: pass the prior page's `next_page_after` unchanged
    /// to continue without repeating or skipping a row. The non-zero 16-bit
    /// page size bounds both work and returned memory to at most 65,535 rows.
    pub fn address_transaction_history_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        after: Option<WalletAddressTransactionKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressTransactionHistoryPage, RocksDbWalletError> {
        self.address_transaction_history_range_page(
            address_script_hash,
            BlockHeightRange::inclusive(BlockHeight::new(0), BlockHeight::new(u32::MAX)),
            after,
            page_size,
        )
    }

    /// Returns one bounded page of address-touching transactions within an
    /// inclusive height range.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "holds one shared block-hash read lock across the page scan to avoid relocking per transaction"
    )]
    pub fn address_transaction_history_range_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        height_range: BlockHeightRange,
        after: Option<WalletAddressTransactionKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressTransactionHistoryPage, RocksDbWalletError> {
        if after.is_some_and(|key| key.address_script_hash() != address_script_hash) {
            return Err(RocksDbWalletError::ContinuationAddressMismatch {
                index: TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
            });
        }
        let family = column_family(
            &self.bounded_open,
            TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
        )?;
        let address_prefix = address_script_hash.as_bytes();
        let lower_bound =
            WalletAddressTransactionKey::first_at_or_after(address_script_hash, height_range.start);
        let upper_bound =
            WalletAddressTransactionKey::new(address_script_hash, height_range.end, u32::MAX);
        let start = after
            .as_ref()
            .map_or(lower_bound.as_bytes().as_slice(), |key| {
                key.as_bytes().as_slice()
            });
        let mut transactions = Vec::with_capacity(usize::from(page_size.get()));
        let mut unresolved = Vec::new();
        let mut last_key = None;
        let mut has_more = false;
        {
            let cache_guard = self.block_hash_cache.state.read();
            let mut last_chunk = None;
            for row in self
                .bounded_open
                .db
                .iterator_cf(&family, IteratorMode::From(start, Direction::Forward))
            {
                let (key_bytes, encoded_transaction) = row.map_err(|source| {
                    RocksDbWalletError::rocksdb("address transaction history page scan", source)
                })?;
                if !key_bytes.starts_with(&address_prefix)
                    || key_bytes.as_ref() > upper_bound.as_bytes().as_slice()
                {
                    break;
                }
                if after.is_some_and(|key| key_bytes.as_ref() <= key.as_bytes().as_slice()) {
                    continue;
                }
                if transactions.len() == usize::from(page_size.get()) {
                    has_more = true;
                    break;
                }
                let key = WalletAddressTransactionKey::decode(&key_bytes)?;
                let transaction_id =
                    WalletAddressTransaction::decode_stored_value(key, &encoded_transaction)?;
                let height = key.block_height();
                let block_hash = lookup_cached_block_hash(&cache_guard, &mut last_chunk, height)
                    .unwrap_or_else(|| {
                        unresolved.push((transactions.len(), height));
                        BlockHash::from_bytes([0_u8; 32])
                    });
                transactions.push(WalletAddressTransaction::new(
                    key,
                    transaction_id,
                    block_hash,
                ));
                last_key = Some(key);
            }
        }
        if !unresolved.is_empty() {
            self.resolve_missing_block_hashes(&mut unresolved, |index, hash| {
                transactions[index].block_hash = hash;
            })?;
        }
        Ok(WalletAddressTransactionHistoryPage {
            transactions,
            next_page_after: if has_more { last_key } else { None },
        })
    }

    /// Returns one exact retained reorg-undo record by block height.
    pub fn find_reorg_undo(
        &self,
        block_height: BlockHeight,
    ) -> Result<Option<WalletReorgUndo>, RocksDbWalletError> {
        let key = block_height.value().to_be_bytes();
        self.read_optional(REORG_UNDO_COLUMN_FAMILY, &key, |encoded| {
            WalletReorgUndo::decode(&key, encoded)
        })
    }

    /// Returns one address's current balance, with an absent row represented as zero.
    pub fn address_balance(
        &self,
        address_script_hash: TransparentAddressScriptHash,
    ) -> Result<u64, RocksDbWalletError> {
        let key = address_script_hash.as_bytes();
        Ok(self
            .read_optional(TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY, &key, |encoded| {
                WalletAddressBalance::decode(&key, encoded)
            })?
            .map_or(0, |balance| balance.balance_zat))
    }

    /// Returns the complete current UTXO aggregate committed by READY.
    #[must_use]
    pub const fn utxo_summary(&self) -> &WalletUtxoSetSummary {
        &self.ready_evidence.utxo_summary
    }

    /// Returns the bounded store I/O mode selected at open.
    #[must_use]
    pub const fn io_mode(&self) -> RocksDbIoMode {
        self.bounded_open.io_mode
    }

    fn read_optional<Row>(
        &self,
        family_name: &'static str,
        key: &[u8],
        decode: impl FnOnce(
            &[u8],
        )
            -> Result<Row, zinder_wallet_projection::WalletProjectionContractError>,
    ) -> Result<Option<Row>, RocksDbWalletError> {
        let family = column_family(&self.bounded_open, family_name)?;
        self.bounded_open
            .db
            .get_cf(&family, key)
            .map_err(|source| RocksDbWalletError::rocksdb("query read", source))?
            .map(|encoded| decode(&encoded).map_err(RocksDbWalletError::from))
            .transpose()
    }

    fn require_block_hash(&self, height: BlockHeight) -> Result<BlockHash, RocksDbWalletError> {
        let chunk_id = BlockHashChunk::chunk_id(height);
        if let Some(chunk) = self.block_hash_cache.state.read().chunks.get(&chunk_id)
            && let Some(hash) = chunk.get(height)
        {
            return Ok(hash);
        }
        let chunk = self.load_block_hash_chunk(chunk_id)?;
        let hash = chunk
            .get(height)
            .ok_or(RocksDbWalletError::AdmissionChanged {
                reason: "wallet block hash is absent for indexed height",
            })?;
        self.block_hash_cache.insert_chunk(chunk_id, chunk);
        Ok(hash)
    }

    fn load_block_hash_chunk(
        &self,
        chunk_id: u32,
    ) -> Result<Box<BlockHashChunk>, RocksDbWalletError> {
        let start_height = chunk_id << BLOCK_HASH_CACHE_CHUNK_SHIFT;
        let end_height = start_height.saturating_add((1_u32 << BLOCK_HASH_CACHE_CHUNK_SHIFT) - 1);
        let family = column_family(&self.bounded_open, BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY)?;
        let start_key = start_height.to_be_bytes();
        let mut chunk = Box::new(BlockHashChunk::empty());
        for row in self
            .bounded_open
            .db
            .iterator_cf(&family, IteratorMode::From(&start_key, Direction::Forward))
        {
            let (key_bytes, value_bytes) =
                row.map_err(|source| RocksDbWalletError::rocksdb("block hash chunk scan", source))?;
            let decoded = WalletBlockHash::decode(&key_bytes, &value_bytes)?;
            if decoded.block.height.value() > end_height {
                break;
            }
            chunk.insert(decoded.block.height, decoded.block.hash);
        }
        Ok(chunk)
    }

    fn resolve_missing_block_hashes(
        &self,
        missing: &mut [(usize, BlockHeight)],
        mut assign: impl FnMut(usize, BlockHash),
    ) -> Result<(), RocksDbWalletError> {
        missing.sort_unstable_by_key(|&(_, height)| height);
        let mut cursor = 0_usize;
        while cursor < missing.len() {
            let chunk_id = BlockHashChunk::chunk_id(missing[cursor].1);
            let mut end = cursor + 1;
            while end < missing.len() && BlockHashChunk::chunk_id(missing[end].1) == chunk_id {
                end += 1;
            }
            if !self.resolve_group_from_cache(chunk_id, &missing[cursor..end], &mut assign)? {
                let chunk = self.load_block_hash_chunk(chunk_id)?;
                for &(index, height) in &missing[cursor..end] {
                    let hash = chunk
                        .get(height)
                        .ok_or(RocksDbWalletError::AdmissionChanged {
                            reason: "wallet block hash is absent for indexed height",
                        })?;
                    assign(index, hash);
                }
                self.block_hash_cache.insert_chunk(chunk_id, chunk);
            }
            cursor = end;
        }
        Ok(())
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "holds one shared read lock across all heights in the cached chunk group"
    )]
    fn resolve_group_from_cache(
        &self,
        chunk_id: u32,
        group: &[(usize, BlockHeight)],
        assign: &mut impl FnMut(usize, BlockHash),
    ) -> Result<bool, RocksDbWalletError> {
        let guard = self.block_hash_cache.state.read();
        let Some(chunk) = guard.chunks.get(&chunk_id) else {
            return Ok(false);
        };
        for &(index, height) in group {
            let hash = chunk
                .get(height)
                .ok_or(RocksDbWalletError::AdmissionChanged {
                    reason: "wallet block hash is absent for indexed height",
                })?;
            assign(index, hash);
        }
        Ok(true)
    }
}

impl RocksDbWalletFollowingStore {
    /// Cold-admits a restored READY wallet checkpoint without granting serving
    /// or mutation authority.
    ///
    /// This recovery-only boundary opens the checkpoint under the caller's
    /// bounded `RocksDB` budget, reads its physical database identity, and
    /// performs the complete semantic row scan against its persisted READY
    /// evidence. Callers must still compare the returned evidence with their
    /// admitted recovery manifest before any restore, following, or promotion
    /// transition.
    pub fn cold_admit_recovery_checkpoint(
        target: impl AsRef<Path>,
        expected_network: Network,
        config: WalletRecoveryAdmissionConfig<'_>,
    ) -> Result<WalletOwnerCheckpointEvidence, RocksDbWalletError> {
        validate_resource_budget(config.resource_budget)?;
        let target = target.as_ref();
        require_exact_column_families(target)?;
        let opened = open_bounded_rocksdb(
            RocksDbOpenRole::ReadOnlyProbe { path: target },
            config.resource_budget,
            wallet_column_family_descriptors,
        )
        .map_err(|source| RocksDbWalletError::rocksdb("recovery read-only open", source))?;
        let expected_control = decode_only_control(&opened)?;
        if expected_control.network != expected_network {
            return Err(RocksDbWalletError::NetworkMismatch {
                expected: expected_network,
                observed: expected_control.network,
            });
        }
        let WalletProjectionBuildState::Ready(ready_evidence) = &expected_control.build_state
        else {
            return Err(RocksDbWalletError::StoreNotReady {
                path: target.to_path_buf(),
            });
        };
        let database_identity =
            opened
                .db
                .get_db_identity()
                .map_err(|source| RocksDbWalletError::CheckpointFailed {
                    path: target.to_path_buf(),
                    source,
                })?;
        validate_ready_rows_by_semantic_rederivation(
            &opened,
            expected_network,
            ready_evidence,
            WalletSemanticValidationConfig {
                staging_path: config.staging_path,
                max_sort_memory_bytes_per_sorter: config.max_sort_memory_bytes_per_sorter,
                max_temporary_file_bytes_per_sorter: config.max_temporary_file_bytes_per_sorter,
                max_accounted_reorg_undo_bytes: config.max_accounted_reorg_undo_bytes,
            },
        )?;
        let observed_control = decode_only_control(&opened)?;
        let observed_identity =
            opened
                .db
                .get_db_identity()
                .map_err(|source| RocksDbWalletError::CheckpointFailed {
                    path: target.to_path_buf(),
                    source,
                })?;
        if observed_control != expected_control || observed_identity != database_identity {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "wallet checkpoint changed during recovery cold admission",
            });
        }
        Ok(WalletOwnerCheckpointEvidence {
            database_identity,
            store_identity: WALLET_PROJECTION_STORE_IDENTITY,
            schema_version: WALLET_ROCKSDB_SCHEMA_VERSION,
            network: expected_network,
            ready_evidence: ready_evidence.clone(),
        })
    }

    /// Creates and cold-admits one physical checkpoint from this wallet owner.
    ///
    /// `target` must not exist. The returned identity and READY evidence are
    /// read through exact cold admission of the completed checkpoint, never
    /// copied from this live handle. This operation exists only on the
    /// non-serving following owner; serving stores and secondaries expose no
    /// checkpoint operation.
    pub fn create_owner_checkpoint(
        &mut self,
        target: impl AsRef<Path>,
        admission_resource_budget: RocksDbResourceBudget,
    ) -> Result<WalletOwnerCheckpointEvidence, RocksDbWalletError> {
        let target = target.as_ref();
        let admission = self.create_owner_checkpoint_physical(target)?;
        Self::cold_admit_owner_checkpoint(target, &admission, admission_resource_budget)
    }

    /// Creates the physical checkpoint while this following owner holds its
    /// primary handle, then returns immutable cold-admission context.
    ///
    /// The context captures the source database identity before `RocksDB` starts
    /// checkpoint creation. Callers that separate physical capture from cold
    /// validation must retain this context and invoke
    /// [`Self::cold_admit_owner_checkpoint`] for the same target.
    pub fn create_owner_checkpoint_physical(
        &mut self,
        target: impl AsRef<Path>,
    ) -> Result<WalletOwnerCheckpointAdmission, RocksDbWalletError> {
        let target = target.as_ref();
        require_absent_checkpoint_target(target)?;
        let database_identity = self
            .store
            .bounded_open
            .db
            .get_db_identity()
            .map_err(|source| RocksDbWalletError::CheckpointFailed {
                path: target.to_path_buf(),
                source,
            })?;
        let checkpoint = Checkpoint::new(&self.store.bounded_open.db).map_err(|source| {
            RocksDbWalletError::CheckpointFailed {
                path: target.to_path_buf(),
                source,
            }
        })?;
        checkpoint.create_checkpoint(target).map_err(|source| {
            RocksDbWalletError::CheckpointFailed {
                path: target.to_path_buf(),
                source,
            }
        })?;
        Ok(WalletOwnerCheckpointAdmission {
            network: self.network(),
            database_identity,
        })
    }

    /// Cold-admits one physical wallet checkpoint against immutable owner
    /// context without reopening or retaining the source primary.
    ///
    /// The target's `RocksDB` database identity must exactly match the identity
    /// captured before physical checkpoint creation. A replacement with equal
    /// wallet READY evidence is therefore refused instead of being admitted.
    pub fn cold_admit_owner_checkpoint(
        target: impl AsRef<Path>,
        admission: &WalletOwnerCheckpointAdmission,
        admission_resource_budget: RocksDbResourceBudget,
    ) -> Result<WalletOwnerCheckpointEvidence, RocksDbWalletError> {
        validate_resource_budget(admission_resource_budget)?;
        let target = target.as_ref();
        let cold_checkpoint = RocksDbWalletStore::open_ready_for_following(
            target,
            admission.network,
            admission_resource_budget,
        )?;
        let cold_database_identity = cold_checkpoint
            .store
            .bounded_open
            .db
            .get_db_identity()
            .map_err(|source| RocksDbWalletError::CheckpointFailed {
                path: target.to_path_buf(),
                source,
            })?;
        if cold_database_identity != admission.database_identity {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "checkpoint database identity differs from the physical owner checkpoint",
            });
        }
        Ok(WalletOwnerCheckpointEvidence {
            database_identity: cold_database_identity,
            store_identity: WALLET_PROJECTION_STORE_IDENTITY,
            schema_version: WALLET_ROCKSDB_SCHEMA_VERSION,
            network: cold_checkpoint.network(),
            ready_evidence: cold_checkpoint.ready_evidence().clone(),
        })
    }

    /// Returns the persisted READY evidence that must be converged before serving.
    #[must_use]
    pub const fn ready_evidence(&self) -> &WalletProjectionReadyEvidence {
        self.store.ready_evidence()
    }

    /// Returns the size of the last transition this handle published, or
    /// `None` before its first.
    #[must_use]
    pub const fn last_transition_footprint(&self) -> Option<WalletTransitionFootprint> {
        self.store.last_transition_footprint()
    }

    /// Returns the immutable network admitted from the persisted wallet control.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.store.network()
    }

    /// Returns the maximum rollback depth the retained undo rows support.
    #[must_use]
    pub const fn supported_reorg_depth(&self) -> u32 {
        self.store.control.supported_reorg_depth
    }

    /// Publishes throttled resource gauges for the wallet writer.
    pub fn record_resource_gauges(&self) {
        self.store.bounded_open.record_resource_gauges(
            zinder_store::StoreRole::WalletPrimary,
            &WALLET_DATA_COLUMN_FAMILIES.map(|(name, _)| name),
        );
    }

    /// Returns the DB-wide `RocksDB` statistics ticker count since this
    /// handle opened, or zero when the resource budget turned statistics off.
    #[must_use]
    pub fn rocksdb_ticker_count(&self, ticker: rust_rocksdb::statistics::Ticker) -> u64 {
        self.store.bounded_open.statistics.get_ticker_count(ticker)
    }

    /// Durably raises the supported rollback depth to the canonical store's
    /// effective reorg window.
    ///
    /// Undo rows always cover exactly the canonical unsettled suffix. A
    /// canonical raise holds the settled tip, so retained rows grow with new
    /// blocks and no row is backfilled. A smaller depth is refused because
    /// pruned rows cannot be recovered without a rebuild; an equal depth is a
    /// no-op.
    pub fn raise_supported_reorg_depth(
        &mut self,
        supported_reorg_depth: u32,
    ) -> Result<(), RocksDbWalletError> {
        let current = self.store.control.supported_reorg_depth;
        if supported_reorg_depth == current {
            return Ok(());
        }
        if supported_reorg_depth < current {
            return Err(RocksDbWalletError::WalletProjectionRebuildRequired {
                reason: "configured reorg window is below the wallet's supported reorg depth; a depth is never lowered in place",
            });
        }
        let mut next_control = self.store.control.clone();
        next_control.supported_reorg_depth = supported_reorg_depth;
        write_control_sync(&self.store.bounded_open, &next_control)?;
        if decode_only_control(&self.store.bounded_open)? != next_control {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "wallet supported reorg depth readback differs from the raised control",
            });
        }
        self.store.control = next_control;
        Ok(())
    }

    /// Returns one retained undo record so a follower can verify its common ancestor.
    ///
    /// This narrow inspection surface exposes no query reads or arbitrary row
    /// access; it is only sufficient for a follower to choose the explicit
    /// durable rollback suffix required by atomic reconciliation.
    pub fn find_reorg_undo(
        &self,
        block_height: BlockHeight,
    ) -> Result<Option<WalletReorgUndo>, RocksDbWalletError> {
        self.store.find_reorg_undo(block_height)
    }

    /// Applies one exact canonical event while this handle remains non-serving.
    #[allow(
        clippy::too_many_arguments,
        reason = "the following API keeps its authenticated source, fence, settlement, budget, and replay input explicit"
    )]
    pub fn apply_canonical_event_range<I>(
        &mut self,
        expected_source: WalletCanonicalSourceIdentity,
        event: CanonicalRetainedEvent,
        resulting_fence: CanonicalEventFence,
        resulting_settled_tip: BlockId,
        max_logical_bytes: NonZeroU64,
        replay_rows: I,
    ) -> Result<(), RocksDbWalletError>
    where
        I: IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
    {
        self.store.apply_canonical_event_range(
            expected_source,
            event,
            resulting_fence,
            resulting_settled_tip,
            max_logical_bytes,
            replay_rows,
        )
    }

    /// Applies one canonical event and abandons it before its atomic write on cancellation.
    #[allow(
        clippy::too_many_arguments,
        reason = "the following API keeps its authenticated source, fence, settlement, budget, replay, and cancellation inputs explicit"
    )]
    pub fn apply_canonical_event_range_cancellable<I, Cancel>(
        &mut self,
        expected_source: WalletCanonicalSourceIdentity,
        event: CanonicalRetainedEvent,
        resulting_fence: CanonicalEventFence,
        resulting_settled_tip: BlockId,
        max_logical_bytes: NonZeroU64,
        replay_rows: I,
        cancelled_before_write: Cancel,
    ) -> Result<(), RocksDbWalletError>
    where
        I: IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
        Cancel: FnOnce() -> bool,
    {
        self.store.apply_canonical_event_range_cancellable(
            expected_source,
            event,
            resulting_fence,
            resulting_settled_tip,
            max_logical_bytes,
            replay_rows,
            cancelled_before_write,
        )
    }

    /// Reconciles this non-serving wallet directly to a current canonical fence.
    ///
    /// See [`RocksDbWalletStore::reconcile_canonical_event_sequence`] for the
    /// retained-history, verified-ancestor, bounded-current-replay, and atomic
    /// publication contract.
    #[allow(
        clippy::too_many_arguments,
        reason = "the following API keeps retained history, authenticated target, settlement, rollback, and replay inputs explicit"
    )]
    pub fn reconcile_canonical_event_sequence<I>(
        &mut self,
        expected_source: WalletCanonicalSourceIdentity,
        retained_events: &[CanonicalRetainedEvent],
        target_fence: CanonicalEventFence,
        target_settled_tip: BlockId,
        rollback_range: Option<BlockHeightRange>,
        replay_range: BlockHeightRange,
        max_logical_bytes: NonZeroU64,
        replay_rows: I,
    ) -> Result<(), RocksDbWalletError>
    where
        I: IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
    {
        self.store.reconcile_canonical_event_sequence(
            expected_source,
            retained_events,
            target_fence,
            target_settled_tip,
            rollback_range,
            replay_range,
            max_logical_bytes,
            replay_rows,
        )
    }

    /// Reconciles this non-serving wallet and abandons before the atomic write on cancellation.
    #[allow(
        clippy::too_many_arguments,
        reason = "the following API keeps retained history, authenticated target, settlement, rollback, replay, and cancellation inputs explicit"
    )]
    pub fn reconcile_canonical_event_sequence_cancellable<I, Cancel>(
        &mut self,
        expected_source: WalletCanonicalSourceIdentity,
        retained_events: &[CanonicalRetainedEvent],
        target_fence: CanonicalEventFence,
        target_settled_tip: BlockId,
        rollback_range: Option<BlockHeightRange>,
        replay_range: BlockHeightRange,
        max_logical_bytes: NonZeroU64,
        replay_rows: I,
        cancelled_before_write: Cancel,
    ) -> Result<(), RocksDbWalletError>
    where
        I: IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
        Cancel: FnOnce() -> bool,
    {
        self.store.reconcile_canonical_event_sequence_cancellable(
            expected_source,
            retained_events,
            target_fence,
            target_settled_tip,
            rollback_range,
            replay_range,
            max_logical_bytes,
            replay_rows,
            cancelled_before_write,
        )
    }

    /// Converts this following-only handle into a query-serving READY store.
    ///
    /// The caller supplies the canonical source identity it has independently
    /// converged to. A mismatch fails closed and leaves no serving handle.
    pub fn into_ready_store(
        self,
        expected_source: WalletCanonicalSourceIdentity,
    ) -> Result<RocksDbWalletStore, RocksDbWalletError> {
        let observed_source =
            WalletCanonicalSourceIdentity::from_ready_evidence(self.store.ready_evidence());
        if observed_source != expected_source {
            return Err(RocksDbWalletError::CanonicalSourceMismatch {
                expected: Box::new(expected_source),
                observed: Box::new(observed_source),
            });
        }
        Ok(self.store)
    }
}

fn require_absent_checkpoint_target(path: &Path) -> Result<(), RocksDbWalletError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(RocksDbWalletError::CheckpointTargetExists {
            path: path.to_path_buf(),
        }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(RocksDbWalletError::PathUnavailable {
            path: path.to_path_buf(),
            source,
        }),
    }
}

pub(crate) fn wallet_column_family_descriptors(
    block_cache: &Cache,
    resource_budget: RocksDbResourceBudget,
) -> Vec<ColumnFamilyDescriptor> {
    let options = WalletColumnFamilyOptions::new(block_cache, resource_budget);
    WALLET_DATA_COLUMN_FAMILIES
        .into_iter()
        .map(|(name, family)| ColumnFamilyDescriptor::new(name, options.for_family(family).clone()))
        .collect()
}

const ADDRESS_RANGE_BLOCK_SIZE_BYTES: usize = 16 * 1024;
const OUTPOINT_POINT_LOOKUP_BLOCK_SIZE_BYTES: usize = 8 * 1024;
const COMPACT_UNFILTERED_BLOCK_SIZE_BYTES: usize = 4 * 1024;
const ZSTD_COMPRESSION_WINDOW_BITS: i32 = -14;
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// `RocksDB` options for every wallet data family, all charged to one block cache.
///
/// Every open path and the bulk-build SST writer take a family's options from
/// here, so files written by a rebuild and by following carry matching block,
/// filter, and compression tuning.
pub(crate) struct WalletColumnFamilyOptions {
    address_range: Options,
    address_range_bulk_sst: Options,
    outpoint_point_lookup: Options,
    outpoint_point_lookup_bulk_sst: Options,
    compact_unfiltered: Options,
    compact_unfiltered_bulk_sst: Options,
    block_hash_lookup: Options,
}

impl WalletColumnFamilyOptions {
    pub(crate) fn new(block_cache: &Cache, resource_budget: RocksDbResourceBudget) -> Self {
        let (address_range, address_range_bulk_sst) = wallet_compressed_options(
            &point_lookup_table_options(block_cache, ADDRESS_RANGE_BLOCK_SIZE_BYTES),
            resource_budget,
        );
        let (outpoint_point_lookup, outpoint_point_lookup_bulk_sst) = wallet_compressed_options(
            &point_lookup_table_options(block_cache, OUTPOINT_POINT_LOOKUP_BLOCK_SIZE_BYTES),
            resource_budget,
        );
        let (compact_unfiltered, compact_unfiltered_bulk_sst) = wallet_compressed_options(
            &unfiltered_table_options(block_cache, COMPACT_UNFILTERED_BLOCK_SIZE_BYTES),
            resource_budget,
        );
        let block_hash_lookup = wallet_uncompressed_options(
            &unfiltered_table_options(block_cache, COMPACT_UNFILTERED_BLOCK_SIZE_BYTES),
            resource_budget,
        );
        Self {
            address_range,
            address_range_bulk_sst,
            outpoint_point_lookup,
            outpoint_point_lookup_bulk_sst,
            compact_unfiltered,
            compact_unfiltered_bulk_sst,
            block_hash_lookup,
        }
    }

    /// Returns the live column-family options for `family`.
    pub(crate) const fn for_family(&self, family: WalletProjectionRowFamily) -> &Options {
        match family {
            WalletProjectionRowFamily::TransparentUnspentOutput
            | WalletProjectionRowFamily::TransparentSpentOutput => &self.outpoint_point_lookup,
            WalletProjectionRowFamily::TransparentUnspentOutputByAddress
            | WalletProjectionRowFamily::TransparentAddressTransaction => &self.address_range,
            WalletProjectionRowFamily::TransparentAddressBalance
            | WalletProjectionRowFamily::ReorgUndo => &self.compact_unfiltered,
            WalletProjectionRowFamily::BlockHashByHeight => &self.block_hash_lookup,
        }
    }

    /// Returns the bulk-build `SstFileWriter` options for `family`, writing
    /// Zstd level-3 data blocks directly on ingestion for compressible families.
    pub(crate) const fn for_bulk_sst(&self, family: WalletProjectionRowFamily) -> &Options {
        match family {
            WalletProjectionRowFamily::TransparentUnspentOutput
            | WalletProjectionRowFamily::TransparentSpentOutput => {
                &self.outpoint_point_lookup_bulk_sst
            }
            WalletProjectionRowFamily::TransparentUnspentOutputByAddress
            | WalletProjectionRowFamily::TransparentAddressTransaction => {
                &self.address_range_bulk_sst
            }
            WalletProjectionRowFamily::TransparentAddressBalance
            | WalletProjectionRowFamily::ReorgUndo => &self.compact_unfiltered_bulk_sst,
            WalletProjectionRowFamily::BlockHashByHeight => &self.block_hash_lookup,
        }
    }
}

fn point_lookup_table_options(block_cache: &Cache, block_size: usize) -> BlockBasedOptions {
    let mut table = build_block_based_table_factory(block_cache);
    table.set_block_size(block_size);
    table.set_hybrid_ribbon_filter(
        POINT_LOOKUP_FILTER_BITS_PER_KEY,
        POINT_LOOKUP_FILTER_BLOOM_BEFORE_LEVEL,
    );
    // Partitioned filters require the two-level index. Together they cache
    // filter and index data in small partitions instead of one block per file,
    // and pinning only the top-level directory keeps deep point lookups fast.
    table.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
    table.set_partition_filters(true);
    table.set_pin_top_level_index_and_filter(true);
    table
}

fn unfiltered_table_options(block_cache: &Cache, block_size: usize) -> BlockBasedOptions {
    let mut table = build_block_based_table_factory(block_cache);
    table.set_block_size(block_size);
    table
}

fn wallet_compressed_options(
    table_options: &BlockBasedOptions,
    resource_budget: RocksDbResourceBudget,
) -> (Options, Options) {
    let mut live = wallet_base_options(table_options, resource_budget);
    live.set_compression_type(DBCompressionType::Lz4);
    live.set_bottommost_compression_type(DBCompressionType::Zstd);
    live.set_bottommost_compression_options(
        ZSTD_COMPRESSION_WINDOW_BITS,
        ZSTD_COMPRESSION_LEVEL,
        0,
        0,
        true,
    );
    let mut bulk = live.clone();
    bulk.set_compression_type(DBCompressionType::Zstd);
    bulk.set_compression_options(ZSTD_COMPRESSION_WINDOW_BITS, ZSTD_COMPRESSION_LEVEL, 0, 0);
    (live, bulk)
}

fn wallet_uncompressed_options(
    table_options: &BlockBasedOptions,
    resource_budget: RocksDbResourceBudget,
) -> Options {
    let mut options = wallet_base_options(table_options, resource_budget);
    options.set_compression_type(DBCompressionType::None);
    options
}

fn wallet_base_options(
    table_options: &BlockBasedOptions,
    resource_budget: RocksDbResourceBudget,
) -> Options {
    let mut options = Options::default();
    options.set_block_based_table_factory(table_options);
    options.set_write_buffer_size(
        usize::try_from(resource_budget.write_buffer_bytes).unwrap_or(usize::MAX),
    );
    options.set_max_write_buffer_number(resource_budget.max_write_buffer_count);
    keep_blocks_of_dropped_table_files(&mut options);
    options
}

pub(crate) fn validate_resource_budget(
    resource_budget: RocksDbResourceBudget,
) -> Result<(), RocksDbWalletError> {
    resource_budget
        .validate()
        .map_err(|reason| RocksDbWalletError::InvalidResourceBudget { reason })
}

fn create_fresh_directory(path: &Path) -> Result<(), RocksDbWalletError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| RocksDbWalletError::PathUnavailable {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(RocksDbWalletError::PathNotFresh {
                path: path.to_path_buf(),
            })
        }
        Err(source) => Err(RocksDbWalletError::PathUnavailable {
            path: path.to_path_buf(),
            source,
        }),
    }
}

pub(crate) fn required_column_family_names() -> Vec<String> {
    std::iter::once(DEFAULT_COLUMN_FAMILY_NAME)
        .chain(WALLET_DATA_COLUMN_FAMILIES.map(|(name, _)| name))
        .map(str::to_owned)
        .collect()
}

fn require_exact_column_families(path: &Path) -> Result<(), RocksDbWalletError> {
    let mut expected = required_column_family_names();
    let mut observed = rust_rocksdb::DB::list_cf(&Options::default(), path)
        .map_err(|source| RocksDbWalletError::rocksdb("column-family admission", source))?;
    expected.sort_unstable();
    observed.sort_unstable();
    if expected != observed {
        return Err(RocksDbWalletError::ColumnFamilyContractMismatch { expected, observed });
    }
    Ok(())
}

fn require_building_control(
    control: &WalletStoreControlRecord,
    store_path: &Path,
    expected_network: Network,
) -> Result<(), RocksDbWalletError> {
    if control.network != expected_network {
        return Err(RocksDbWalletError::NetworkMismatch {
            expected: expected_network,
            observed: control.network,
        });
    }
    if !matches!(control.build_state, WalletProjectionBuildState::Building(_)) {
        return Err(RocksDbWalletError::StoreNotReady {
            path: store_path.to_path_buf(),
        });
    }
    Ok(())
}

fn require_no_live_lease_of_another_owner(
    control: &WalletStoreControlRecord,
    owner: WalletProjectionBuildOwner,
    now: UnixTimestampMillis,
) -> Result<(), RocksDbWalletError> {
    if let Some(lease) = control.build_lease
        && lease.expires_at() > now
        && lease.owner() != owner
    {
        return Err(RocksDbWalletError::WalletProjectionBuildLeaseHeld {
            holder: lease.owner(),
            expires_at: lease.expires_at(),
        });
    }
    Ok(())
}

fn validate_lease_request(
    control: &WalletStoreControlRecord,
    request: WalletProjectionBuildLeaseRequest,
    now: UnixTimestampMillis,
) -> Result<(), RocksDbWalletError> {
    if request.expires_at() <= now {
        return Err(RocksDbWalletError::WalletProjectionBuildLeaseExpiryNotFuture);
    }
    let WalletProjectionBuildState::Building(plan) = &control.build_state else {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "projection build lease requires a BUILDING control record",
        });
    };
    let canonical_anchor = request.pinned_canonical_anchor();
    if canonical_anchor.source_position() != plan.target_source_position {
        return Err(
            RocksDbWalletError::WalletProjectionBuildLeaseCanonicalAnchorMismatch {
                reason: "requested source position differs from the BUILDING target",
            },
        );
    }
    if request
        .retained_event_anchor()
        .earliest_retained_event_sequence()
        > canonical_anchor.source_position().event_sequence
    {
        return Err(
            RocksDbWalletError::WalletProjectionBuildLeaseCanonicalAnchorMismatch {
                reason: "retained event anchor follows the pinned canonical event",
            },
        );
    }
    Ok(())
}

fn authorize_active_lease(
    control: &WalletStoreControlRecord,
    lease: WalletProjectionBuildLease,
    now: UnixTimestampMillis,
) -> Result<WalletProjectionBuildLease, RocksDbWalletError> {
    let persisted = control
        .build_lease
        .ok_or(RocksDbWalletError::WalletProjectionBuildLeaseMissing)?;
    if persisted.owner() != lease.owner() {
        return Err(
            RocksDbWalletError::WalletProjectionBuildLeaseOwnerMismatch {
                expected: persisted.owner(),
                observed: lease.owner(),
            },
        );
    }
    if persisted.generation() != lease.generation() {
        return Err(
            RocksDbWalletError::WalletProjectionBuildLeaseGenerationMismatch {
                expected: persisted.generation(),
                observed: lease.generation(),
            },
        );
    }
    if persisted.pinned_canonical_anchor() != lease.pinned_canonical_anchor()
        || persisted.retained_event_anchor() != lease.retained_event_anchor()
        || persisted.expires_at() != lease.expires_at()
        || persisted.network() != lease.network()
        || persisted.projection_schema_version() != lease.projection_schema_version()
        || persisted.version() != lease.version()
    {
        return Err(
            RocksDbWalletError::WalletProjectionBuildLeaseCanonicalAnchorMismatch {
                reason: "supplied capability differs from durable lease identity",
            },
        );
    }
    if persisted.expires_at() <= now {
        return Err(RocksDbWalletError::WalletProjectionBuildLeaseExpired {
            expires_at: persisted.expires_at(),
        });
    }
    Ok(persisted)
}

fn apply_build_lease_heartbeat(
    bounded_open: &BoundedRocksDbOpen,
    control: &mut WalletStoreControlRecord,
    lease: &mut WalletProjectionBuildLease,
    heartbeat: WalletBuildLeaseHeartbeat,
) -> Result<(), RocksDbWalletError> {
    let persisted_control = decode_only_control(bounded_open)?;
    let persisted_lease = authorize_active_lease(&persisted_control, *lease, heartbeat.now())?;
    let Some(renew_until) = heartbeat.renew_until() else {
        *control = persisted_control;
        *lease = persisted_lease;
        return Ok(());
    };
    if renew_until <= heartbeat.now() {
        return Err(RocksDbWalletError::WalletProjectionBuildLeaseExpiryNotFuture);
    }
    if renew_until <= persisted_lease.expires_at() {
        return Err(RocksDbWalletError::WalletProjectionBuildLeaseRenewalNotExtended);
    }
    let renewed = persisted_lease.renewed(renew_until);
    let next_control = WalletStoreControlRecord {
        build_lease: Some(renewed),
        ..persisted_control
    };
    write_control_sync(bounded_open, &next_control)?;
    // Once the synchronous write succeeds, retain the new exact capability
    // even if the defensive readback below fails. The outer build lifecycle
    // can then release only this generation/expiry and cannot clear a
    // successor lease after an admission race.
    *lease = renewed;
    let persisted_after_write = decode_only_control(bounded_open)?;
    if persisted_after_write != next_control {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "projection build lease differs after in-process heartbeat renewal",
        });
    }
    *control = next_control;
    Ok(())
}

pub(crate) fn decode_only_control(
    bounded_open: &BoundedRocksDbOpen,
) -> Result<WalletStoreControlRecord, RocksDbWalletError> {
    let mut iterator = bounded_open.db.iterator(IteratorMode::Start);
    let Some(first) = iterator.next() else {
        return Err(RocksDbWalletError::StoreControlMissing);
    };
    let (key, encoded) =
        first.map_err(|source| RocksDbWalletError::rocksdb("control scan", source))?;
    if key.as_ref() != WALLET_STORE_CONTROL_KEY || iterator.next().is_some() {
        return Err(RocksDbWalletError::StoreControlCardinalityMismatch);
    }
    WalletStoreControlRecord::decode(&encoded).map_err(RocksDbWalletError::from)
}

fn write_control_sync(
    bounded_open: &BoundedRocksDbOpen,
    control: &WalletStoreControlRecord,
) -> Result<(), RocksDbWalletError> {
    let encoded = control.encode()?;
    let mut batch = WriteBatch::default();
    batch.put(WALLET_STORE_CONTROL_KEY, encoded);
    let mut write_options = WriteOptions::default();
    write_options.set_sync(true);
    bounded_open
        .db
        .write_opt(&batch, &write_options)
        .map_err(|source| RocksDbWalletError::rocksdb("control publication", source))
}

fn flush_complete_build(bounded_open: &BoundedRocksDbOpen) -> Result<(), RocksDbWalletError> {
    let mut families = Vec::with_capacity(WALLET_DATA_COLUMN_FAMILIES.len() + 1);
    for family_name in std::iter::once(DEFAULT_COLUMN_FAMILY_NAME)
        .chain(WALLET_DATA_COLUMN_FAMILIES.map(|(name, _)| name))
    {
        families.push(column_family(bounded_open, family_name)?);
    }
    let family_refs = families.iter().collect::<Vec<_>>();
    let mut options = FlushOptions::default();
    options.set_wait(true);
    bounded_open
        .db
        .flush_cfs_opt(&family_refs, &options)
        .map_err(|source| RocksDbWalletError::rocksdb("publication column-family flush", source))?;
    bounded_open
        .db
        .flush_wal(true)
        .map_err(|source| RocksDbWalletError::rocksdb("publication WAL sync", source))
}

#[derive(Debug, Default)]
struct ExpectedReorgUndoEffects {
    block: Option<zinder_core::BlockId>,
    created_outpoints: BTreeSet<WalletOutpointKey>,
    spent_outpoints: BTreeSet<WalletOutpointKey>,
    address_transaction_keys: BTreeSet<WalletAddressTransactionKey>,
}

#[derive(Debug)]
struct AccountedValidationReorgUndoMemory {
    limit: u64,
    current: u64,
    peak: u64,
}

impl AccountedValidationReorgUndoMemory {
    const fn new(limit: u64) -> Self {
        Self {
            limit,
            current: 0,
            peak: 0,
        }
    }

    fn reserve(&mut self, bytes: usize) -> Result<(), RocksDbWalletError> {
        let bytes = u64::try_from(bytes)
            .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        let required_bytes = self
            .current
            .checked_add(bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        if required_bytes > self.limit {
            return Err(RocksDbWalletError::AccountedReorgUndoMemoryLimit {
                limit_bytes: self.limit,
                required_bytes,
            });
        }
        self.current = required_bytes;
        self.peak = self.peak.max(required_bytes);
        Ok(())
    }
}

#[allow(
    clippy::set_contains_or_insert,
    reason = "the retained-key budget must be admitted only for absent keys and before insertion"
)]
fn insert_accounted_relation_key<Key: Ord>(
    keys: &mut BTreeSet<Key>,
    key: Key,
    memory: &mut AccountedValidationReorgUndoMemory,
) -> Result<(), RocksDbWalletError> {
    if keys.contains(&key) {
        return Ok(());
    }
    memory.reserve(size_of::<Key>())?;
    if !keys.insert(key) {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "validation reorg-undo key changed during single-threaded admission",
        });
    }
    Ok(())
}

#[derive(Debug)]
struct ExpectedReorgUndoSuffix {
    undo_by_height: BTreeMap<u32, ExpectedReorgUndoEffects>,
    memory: AccountedValidationReorgUndoMemory,
}

impl ExpectedReorgUndoSuffix {
    fn new(
        settled_tip: zinder_core::BlockId,
        undo_count: u64,
        max_accounted_bytes: u64,
    ) -> Result<Self, RocksDbWalletError> {
        let mut suffix = Self {
            undo_by_height: BTreeMap::new(),
            memory: AccountedValidationReorgUndoMemory::new(max_accounted_bytes),
        };
        let first_height = u64::from(settled_tip.height.value()).checked_add(1).ok_or(
            RocksDbWalletError::AdmissionChanged {
                reason: "reorg undo suffix falls outside the READY source range",
            },
        )?;
        for offset in 0..undo_count {
            let height =
                first_height
                    .checked_add(offset)
                    .ok_or(RocksDbWalletError::AdmissionChanged {
                        reason: "reorg undo suffix height overflow",
                    })?;
            let height =
                u32::try_from(height).map_err(|_| RocksDbWalletError::AdmissionChanged {
                    reason: "reorg undo suffix height exceeds u32::MAX",
                })?;
            suffix
                .memory
                .reserve(size_of::<(u32, ExpectedReorgUndoEffects)>())?;
            suffix
                .undo_by_height
                .insert(height, ExpectedReorgUndoEffects::default());
        }
        Ok(suffix)
    }

    fn observe_address_transaction(
        &mut self,
        key: WalletAddressTransactionKey,
        block: zinder_core::BlockId,
    ) -> Result<(), RocksDbWalletError> {
        if let Some(undo) = self.undo_by_height.get_mut(&block.height.value()) {
            remember_undo_block(undo, block)?;
            insert_accounted_relation_key(
                &mut undo.address_transaction_keys,
                key,
                &mut self.memory,
            )?;
        }
        Ok(())
    }

    fn observe_created(
        &mut self,
        outpoint: TransparentOutPoint,
        block: zinder_core::BlockId,
    ) -> Result<(), RocksDbWalletError> {
        let Some(undo) = self.undo_by_height.get_mut(&block.height.value()) else {
            return Ok(());
        };
        remember_undo_block(undo, block)?;
        let key = WalletOutpointKey::new(outpoint);
        insert_accounted_relation_key(&mut undo.created_outpoints, key, &mut self.memory)?;
        Ok(())
    }

    fn observe_spent(
        &mut self,
        outpoint: TransparentOutPoint,
        block: zinder_core::BlockId,
    ) -> Result<(), RocksDbWalletError> {
        let Some(undo) = self.undo_by_height.get_mut(&block.height.value()) else {
            return Ok(());
        };
        remember_undo_block(undo, block)?;
        let key = WalletOutpointKey::new(outpoint);
        insert_accounted_relation_key(&mut undo.spent_outpoints, key, &mut self.memory)?;
        Ok(())
    }
}

/// The expected reorg-undo suffix shared by concurrent primary-output scans.
///
/// The suffix's heights are fixed when it is created, so a row outside them
/// is skipped without taking the lock. Rows inside them update one suffix,
/// and because each key is admitted once whatever the order, the suffix ends
/// with the same keys and accounted bytes as after one serial scan.
#[derive(Debug)]
struct SharedExpectedReorgUndoSuffix {
    heights: Range<u64>,
    suffix: parking_lot::Mutex<ExpectedReorgUndoSuffix>,
}

impl SharedExpectedReorgUndoSuffix {
    fn new(suffix: ExpectedReorgUndoSuffix) -> Self {
        let heights = match (
            suffix.undo_by_height.first_key_value(),
            suffix.undo_by_height.last_key_value(),
        ) {
            (Some((first, _)), Some((last, _))) => u64::from(*first)..u64::from(*last) + 1,
            _ => 0..0,
        };
        Self {
            heights,
            suffix: parking_lot::Mutex::new(suffix),
        }
    }

    fn tracks(&self, height: BlockHeight) -> bool {
        self.heights.contains(&u64::from(height.value()))
    }

    fn observe_address_transaction(
        &self,
        key: WalletAddressTransactionKey,
        height: BlockHeight,
        unsettled_hashes: &BTreeMap<BlockHeight, BlockHash>,
    ) -> Result<(), RocksDbWalletError> {
        if !self.tracks(height) {
            return Ok(());
        }
        let block = unsettled_block_id(height, unsettled_hashes)?;
        self.suffix.lock().observe_address_transaction(key, block)
    }

    fn observe_created(
        &self,
        outpoint: TransparentOutPoint,
        height: BlockHeight,
        unsettled_hashes: &BTreeMap<BlockHeight, BlockHash>,
    ) -> Result<(), RocksDbWalletError> {
        if !self.tracks(height) {
            return Ok(());
        }
        let block = unsettled_block_id(height, unsettled_hashes)?;
        self.suffix.lock().observe_created(outpoint, block)
    }

    fn observe_spent(
        &self,
        outpoint: TransparentOutPoint,
        height: BlockHeight,
        unsettled_hashes: &BTreeMap<BlockHeight, BlockHash>,
    ) -> Result<(), RocksDbWalletError> {
        if !self.tracks(height) {
            return Ok(());
        }
        let block = unsettled_block_id(height, unsettled_hashes)?;
        self.suffix.lock().observe_spent(outpoint, block)
    }

    fn into_inner(self) -> ExpectedReorgUndoSuffix {
        self.suffix.into_inner()
    }
}

fn unsettled_block_id(
    height: BlockHeight,
    unsettled_hashes: &BTreeMap<BlockHeight, BlockHash>,
) -> Result<BlockId, RocksDbWalletError> {
    let hash =
        unsettled_hashes
            .get(&height)
            .copied()
            .ok_or(RocksDbWalletError::AdmissionChanged {
                reason: "unsettled block height is missing from block_hash_by_height",
            })?;
    Ok(BlockId::new(height, hash))
}

fn validate_spent_position(spent: &StoredWalletSpentOutput) -> Result<(), RocksDbWalletError> {
    let created = spent.output.created_at;
    let consumed = spent.spent_at;
    if created.block_height.value() > consumed.block_height.value()
        || (created.block_height == consumed.block_height
            && created.tx_index_in_block >= consumed.tx_index_in_block)
    {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "spent output does not follow its exact canonical creation position",
        });
    }
    Ok(())
}

fn remember_undo_block(
    undo: &mut ExpectedReorgUndoEffects,
    block: zinder_core::BlockId,
) -> Result<(), RocksDbWalletError> {
    if undo.block.is_some_and(|observed| observed != block) {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "wallet effects disagree on their canonical block identity",
        });
    }
    undo.block = Some(block);
    Ok(())
}

fn require_address_index_covers_unspent_outputs(
    counts: &zinder_wallet_projection::WalletProjectionFamilyRowCounts,
) -> Result<(), RocksDbWalletError> {
    if counts.transparent_unspent_output_count != counts.transparent_unspent_output_by_address_count
    {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "address unspent index does not exactly cover every primary unspent output",
        });
    }
    Ok(())
}

fn require_ready_evidence_matches_rows(
    digest: WalletProjectionDigestBuilder,
    evidence: &WalletProjectionReadyEvidence,
    utxo_summary: &WalletUtxoSetSummary,
) -> Result<(), RocksDbWalletError> {
    let observed_row_counts = digest.row_counts();
    let observed_accumulator = digest.accumulator().clone();
    let observed_digest = digest.finish();
    if observed_row_counts != evidence.row_counts
        || observed_digest != evidence.projection_digest
        || observed_accumulator != evidence.projection_accumulator
        || *utxo_summary != evidence.utxo_summary
    {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "READY evidence differs from cold wallet rows",
        });
    }
    Ok(())
}

/// Reads the reopened BUILDING store back against the construction evidence.
///
/// Every family is scanned once in stored key order. The primary output
/// families, the address-ordered families, and the address-transaction family
/// are scanned on separate threads, with the primary output and
/// address-transaction families each split into hash-prefix key ranges with a
/// thread each. Each scan accumulates its own rows,
/// and the order-independent partial accumulators are then combined. The scan recomputes the
/// accumulator and the six row counts and compares them with the values
/// emitted while the SST files were written, so a row that was lost, altered,
/// duplicated, or added between SST writing and publication changes the
/// accumulator. The address-ordered families are not re-derived through a
/// second external sort. Cross-family consistency of the construction itself is
/// established by the construction tests and the serial oracle, and by the
/// semantic re-derivation that recovery admission still runs.
struct ValidatedBlockHashTable {
    digest: WalletProjectionDigestBuilder,
    retained_heights: BlockHeightRange,
    unsettled_hashes: BTreeMap<BlockHeight, BlockHash>,
}

fn validate_block_hash_rows(
    bounded_open: &BoundedRocksDbOpen,
    evidence: &WalletProjectionReadyEvidence,
) -> Result<ValidatedBlockHashTable, RocksDbWalletError> {
    let family = column_family(bounded_open, BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY)?;
    let mut digest = WalletProjectionDigestBuilder::new();
    let mut count = 0_u64;
    let mut first_height: Option<BlockHeight> = None;
    let mut previous_height: Option<BlockHeight> = None;
    let mut last_block: Option<BlockId> = None;
    let mut settled_hash: Option<BlockHash> = None;
    let mut unsettled_hashes = BTreeMap::new();
    for row in
        bounded_open
            .db
            .iterator_cf_opt(&family, validation_read_options(), IteratorMode::Start)
    {
        let (key_bytes, value_bytes) =
            row.map_err(|source| RocksDbWalletError::rocksdb("block hash readback scan", source))?;
        let decoded = WalletBlockHash::decode(&key_bytes, &value_bytes)?;
        let height = decoded.block.height;
        if previous_height.is_some_and(|prev| prev.next() != Some(height)) {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "block hash rows are not contiguous by height",
            });
        }
        first_height.get_or_insert(height);
        previous_height = Some(height);
        last_block = Some(decoded.block);
        if height == evidence.settled_tip.height {
            if decoded.block.hash != evidence.settled_tip.hash {
                return Err(RocksDbWalletError::AdmissionChanged {
                    reason: "settled block hash disagrees with READY settled tip",
                });
            }
            settled_hash = Some(decoded.block.hash);
        } else if height > evidence.settled_tip.height {
            let current = u64::try_from(unsettled_hashes.len()).unwrap_or(u64::MAX);
            if current >= evidence.row_counts.reorg_undo_count {
                return Err(RocksDbWalletError::AdmissionChanged {
                    reason: "block hash unsettled suffix does not match READY reorg-undo count",
                });
            }
            unsettled_hashes.insert(height, decoded.block.hash);
        }
        digest.append_row(
            WalletProjectionRowFamily::BlockHashByHeight,
            &key_bytes,
            &value_bytes,
        )?;
        count = increment_validation_count(count, "block hash row count overflow")?;
    }
    let Some(first_h) = first_height else {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "block hash row count differs from READY evidence",
        });
    };
    if count != evidence.row_counts.block_hash_by_height_count
        || last_block != Some(evidence.source_position.tip)
    {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "block hash row count or tip differs from READY evidence",
        });
    }
    if evidence.settled_tip.height >= first_h && settled_hash != Some(evidence.settled_tip.hash) {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "settled block hash disagrees with READY settled tip",
        });
    }
    if u64::try_from(unsettled_hashes.len()).ok() != Some(evidence.row_counts.reorg_undo_count) {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "block hash unsettled suffix does not match READY reorg-undo count",
        });
    }
    Ok(ValidatedBlockHashTable {
        digest,
        retained_heights: BlockHeightRange::inclusive(first_h, evidence.source_position.tip.height),
        unsettled_hashes,
    })
}

fn validate_ready_rows_by_readback(
    bounded_open: &BoundedRocksDbOpen,
    network: Network,
    evidence: &WalletProjectionReadyEvidence,
    config: WalletColdValidationConfig,
) -> Result<WalletColdValidationEvidence, RocksDbWalletError> {
    let counts = evidence.row_counts;
    require_address_index_covers_unspent_outputs(&counts)?;
    let expected_undo = SharedExpectedReorgUndoSuffix::new(ExpectedReorgUndoSuffix::new(
        evidence.settled_tip,
        counts.reorg_undo_count,
        config.max_accounted_reorg_undo_bytes,
    )?);
    let readback_started = Instant::now();
    let block_hash_table = validate_block_hash_rows(bounded_open, evidence)?;
    let ConcurrentFamilyReadback {
        mut digest,
        utxo_summary,
        primary_output_scan,
        address_index_and_balance_readback,
        address_transaction_readback,
    } = read_back_large_families(
        bounded_open,
        network,
        &counts,
        &expected_undo,
        ReadbackScanPlan {
            ranges: FamilyScanRanges {
                primary_output: primary_output_scan_range_count(&config, &counts),
                address_transaction: address_transaction_scan_range_count(&config, &counts),
            },
            validation_context: PrimaryValidationContext {
                retained_heights: block_hash_table.retained_heights,
                unsettled_hashes: &block_hash_table.unsettled_hashes,
            },
        },
    )?;
    digest.combine(&block_hash_table.digest)?;
    let concurrent_family_readback = readback_started.elapsed();
    let expected_undo = expected_undo.into_inner();

    let phase_started = Instant::now();
    validate_reorg_undo_rows(
        bounded_open,
        &mut digest,
        counts.reorg_undo_count,
        evidence.source_position.tip,
        evidence.settled_tip,
        evidence.source_sequence_digest,
        &expected_undo.undo_by_height,
    )?;
    let reorg_undo_readback = phase_started.elapsed();

    require_ready_evidence_matches_rows(digest, evidence, &utxo_summary)?;
    Ok(WalletColdValidationEvidence {
        peak_accounted_reorg_undo_bytes: expected_undo.memory.peak,
        max_accounted_reorg_undo_bytes: config.max_accounted_reorg_undo_bytes,
        random_read_count: 0,
        phase_durations: WalletColdValidationPhaseDurations {
            primary_output_scan,
            address_index_and_balance_readback,
            address_transaction_readback,
            concurrent_family_readback,
            reorg_undo_readback,
        },
    })
}

/// Partitioned scan range counts for the two largest readback families.
#[derive(Clone, Copy)]
struct FamilyScanRanges {
    primary_output: NonZeroU32,
    address_transaction: NonZeroU32,
}

#[derive(Clone, Copy)]
struct PrimaryValidationContext<'a> {
    retained_heights: BlockHeightRange,
    unsettled_hashes: &'a BTreeMap<BlockHeight, BlockHash>,
}

#[derive(Clone, Copy)]
struct ReadbackScanPlan<'a> {
    ranges: FamilyScanRanges,
    validation_context: PrimaryValidationContext<'a>,
}

/// Combined result of the concurrent primary, address-index, and
/// address-transaction family scans.
struct ConcurrentFamilyReadback {
    digest: WalletProjectionDigestBuilder,
    utxo_summary: WalletUtxoSetSummary,
    primary_output_scan: Duration,
    address_index_and_balance_readback: Duration,
    address_transaction_readback: Duration,
}

/// Scans the three large families on separate threads and combines their
/// order-independent partial accumulators.
fn read_back_large_families(
    bounded_open: &BoundedRocksDbOpen,
    network: Network,
    counts: &zinder_wallet_projection::WalletProjectionFamilyRowCounts,
    expected_undo: &SharedExpectedReorgUndoSuffix,
    scan_plan: ReadbackScanPlan<'_>,
) -> Result<ConcurrentFamilyReadback, RocksDbWalletError> {
    let scan_ranges = scan_plan.ranges;
    let validation_context = scan_plan.validation_context;
    let (primary, address_index, address_transactions) = std::thread::scope(|scope| {
        let address_index_worker = spawn_validation_scan(scope, || {
            let started = Instant::now();
            let mut family_digest = WalletProjectionDigestBuilder::new();
            let balance_total_zat =
                read_back_address_index_and_balance_rows(bounded_open, &mut family_digest, counts)?;
            Ok::<_, RocksDbWalletError>((family_digest, balance_total_zat, started.elapsed()))
        })?;
        let address_transaction_worker = spawn_validation_scan(scope, || {
            let started = Instant::now();
            let family_digest = read_back_address_transaction_rows(
                bounded_open,
                counts.transparent_address_transaction_count,
                validation_context.retained_heights,
                scan_ranges.address_transaction,
            )?;
            Ok::<_, RocksDbWalletError>((family_digest, started.elapsed()))
        })?;
        let started = Instant::now();
        let primary = read_back_primary_output_rows(
            bounded_open,
            network,
            expected_undo,
            counts,
            ReadbackPrimaryPlan {
                ranges: scan_ranges.primary_output,
                context: validation_context,
            },
        )
        .map(|(family_digest, utxo_summary)| (family_digest, utxo_summary, started.elapsed()));
        Ok::<_, RocksDbWalletError>((
            primary,
            join_validation_scan(address_index_worker),
            join_validation_scan(address_transaction_worker),
        ))
    })?;
    let (mut digest, utxo_summary, primary_output_scan) = primary?;
    let (address_index_digest, indexed_balance_zat, address_index_and_balance_readback) =
        address_index?;
    let (address_transaction_digest, address_transaction_readback) = address_transactions?;
    if indexed_balance_zat != utxo_summary.total_value_zat {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "address balance rows differ from indexed unspent-output sums",
        });
    }
    digest.combine(&address_index_digest)?;
    digest.combine(&address_transaction_digest)?;
    Ok(ConcurrentFamilyReadback {
        digest,
        utxo_summary,
        primary_output_scan,
        address_index_and_balance_readback,
        address_transaction_readback,
    })
}

#[cfg(test)]
thread_local! {
    /// Tests set this to refuse a cold-validation thread started from this
    /// thread, as an operating system does when it has no thread left. It
    /// holds how many more starts succeed before the refused one.
    static REFUSED_VALIDATION_THREAD: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

/// Starts one cold-validation scan on its own thread in `scope`.
///
/// A refused thread start is an error, not a panic, so the build fails through
/// its ordinary error path and releases its lease.
fn spawn_validation_scan<'scope, T: Send + 'scope>(
    scope: &'scope std::thread::Scope<'scope, '_>,
    scan: impl FnOnce() -> T + Send + 'scope,
) -> Result<std::thread::ScopedJoinHandle<'scope, T>, RocksDbWalletError> {
    #[cfg(test)]
    match REFUSED_VALIDATION_THREAD.get() {
        Some(0) => {
            REFUSED_VALIDATION_THREAD.set(None);
            return Err(RocksDbWalletError::ColdValidationThreadUnavailable {
                reason: "thread start refused by the test".to_owned(),
            });
        }
        Some(remaining) => REFUSED_VALIDATION_THREAD.set(Some(remaining - 1)),
        None => {}
    }
    std::thread::Builder::new()
        .name("zinder-wallet-validate".to_owned())
        .spawn_scoped(scope, scan)
        .map_err(
            |source| RocksDbWalletError::ColdValidationThreadUnavailable {
                reason: source.to_string(),
            },
        )
}

fn join_validation_scan<T>(worker: std::thread::ScopedJoinHandle<'_, T>) -> T {
    worker
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

/// Re-derives the address-ordered families from the primary output rows through
/// bounded external sorts and compares them with the stored rows.
///
/// Recovery admission uses this stricter scan because a restored checkpoint
/// carries its READY evidence in the same untrusted store, so the evidence
/// cannot vouch for the rows.
fn validate_ready_rows_by_semantic_rederivation(
    bounded_open: &BoundedRocksDbOpen,
    network: Network,
    evidence: &WalletProjectionReadyEvidence,
    config: WalletSemanticValidationConfig<'_>,
) -> Result<(), RocksDbWalletError> {
    let counts = evidence.row_counts;
    require_address_index_covers_unspent_outputs(&counts)?;
    let block_hash_table = validate_block_hash_rows(bounded_open, evidence)?;
    let mut sorters = AddressDerivationSorters {
        index: VariableValueSorter::<ADDRESS_UNSPENT_KEY_BYTES>::new(
            config.staging_path,
            "wallet-cold-validation-address-index",
            config.max_sort_memory_bytes_per_sorter,
            config.max_temporary_file_bytes_per_sorter,
        )?,
        transaction: VariableValueSorter::<ADDRESS_TRANSACTION_KEY_BYTES>::new(
            config.staging_path,
            "wallet-cold-validation-address-transactions",
            config.max_sort_memory_bytes_per_sorter,
            config.max_temporary_file_bytes_per_sorter,
        )?,
    };
    let expected_undo = SharedExpectedReorgUndoSuffix::new(ExpectedReorgUndoSuffix::new(
        evidence.settled_tip,
        counts.reorg_undo_count,
        config.max_accounted_reorg_undo_bytes,
    )?);
    let validation_context = PrimaryValidationContext {
        retained_heights: block_hash_table.retained_heights,
        unsettled_hashes: &block_hash_table.unsettled_hashes,
    };
    let (mut digest, utxo_summary) = validate_primary_output_rows(
        bounded_open,
        network,
        Some(&mut sorters),
        &expected_undo,
        PrimaryOutputValidationTarget {
            counts: &counts,
            context: validation_context,
        },
    )?;
    digest.combine(&block_hash_table.digest)?;
    let expected_undo = expected_undo.into_inner();
    let AddressDerivationSorters {
        index: address_index_sorter,
        transaction: address_transaction_sorter,
    } = sorters;
    let mut sorted_address_index = address_index_sorter.finish()?;
    validate_address_index_and_balance_rows(
        bounded_open,
        &mut digest,
        &mut sorted_address_index,
        &counts,
    )?;
    let mut sorted_address_transactions = address_transaction_sorter.finish()?;
    validate_address_transaction_rows(
        bounded_open,
        &mut digest,
        counts.transparent_address_transaction_count,
        &mut sorted_address_transactions,
    )?;
    validate_reorg_undo_rows(
        bounded_open,
        &mut digest,
        counts.reorg_undo_count,
        evidence.source_position.tip,
        evidence.settled_tip,
        evidence.source_sequence_digest,
        &expected_undo.undo_by_height,
    )?;
    require_ready_evidence_matches_rows(digest, evidence, &utxo_summary)
}

/// Streams the stored address-unspent and address-balance families in key
/// order, appends them to the digest, and returns the total of the balance rows.
fn read_back_address_index_and_balance_rows(
    bounded_open: &BoundedRocksDbOpen,
    digest: &mut WalletProjectionDigestBuilder,
    counts: &zinder_wallet_projection::WalletProjectionFamilyRowCounts,
) -> Result<u64, RocksDbWalletError> {
    let address_family = column_family(
        bounded_open,
        TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
    )?;
    let mut address_count = 0u64;
    for row in bounded_open.db.iterator_cf_opt(
        &address_family,
        validation_read_options(),
        IteratorMode::Start,
    ) {
        let (key_bytes, value_bytes) = row.map_err(|source| {
            RocksDbWalletError::rocksdb("address unspent readback scan", source)
        })?;
        WalletAddressUnspentOutputKey::decode(&key_bytes)?;
        if !value_bytes.is_empty() {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "address unspent index does not exactly cover every primary unspent output",
            });
        }
        digest.append_row(
            WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
            &key_bytes,
            &value_bytes,
        )?;
        address_count =
            increment_validation_count(address_count, "address unspent row count overflow")?;
    }
    if address_count != counts.transparent_unspent_output_by_address_count {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "address unspent index does not exactly cover every primary unspent output",
        });
    }

    let balance_family = column_family(bounded_open, TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY)?;
    let mut balance_count = 0u64;
    let mut balance_total_zat = 0u64;
    for row in bounded_open.db.iterator_cf_opt(
        &balance_family,
        validation_read_options(),
        IteratorMode::Start,
    ) {
        let (key_bytes, value_bytes) = row.map_err(|source| {
            RocksDbWalletError::rocksdb("address balance readback scan", source)
        })?;
        let balance = WalletAddressBalance::decode(&key_bytes, &value_bytes)?;
        if balance.balance_zat == 0 {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "address balance rows do not exactly cover positive indexed addresses",
            });
        }
        balance_total_zat = balance_total_zat.checked_add(balance.balance_zat).ok_or(
            RocksDbWalletError::AdmissionChanged {
                reason: "address balance total overflow",
            },
        )?;
        digest.append_row(
            WalletProjectionRowFamily::TransparentAddressBalance,
            &key_bytes,
            &value_bytes,
        )?;
        balance_count =
            increment_validation_count(balance_count, "address balance row count overflow")?;
    }
    if balance_count != counts.transparent_address_balance_count {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "address balance rows do not exactly cover positive indexed addresses",
        });
    }
    Ok(balance_total_zat)
}

/// Partial accumulator and row count of the address-transaction rows in one
/// key range.
struct AddressTransactionTotals {
    digest: WalletProjectionDigestBuilder,
    row_count: u64,
}

impl AddressTransactionTotals {
    fn new() -> Self {
        Self {
            digest: WalletProjectionDigestBuilder::new(),
            row_count: 0,
        }
    }

    /// Adds the totals of a disjoint key range.
    fn combine(&mut self, other: &Self) -> Result<(), RocksDbWalletError> {
        let row_count = self.row_count.checked_add(other.row_count).ok_or(
            RocksDbWalletError::AdmissionChanged {
                reason: "address transaction row count overflow",
            },
        )?;
        self.digest.combine(&other.digest)?;
        self.row_count = row_count;
        Ok(())
    }

    /// Checks the row count against READY evidence and returns the digest.
    fn finish(
        self,
        expected_count: u64,
    ) -> Result<WalletProjectionDigestBuilder, RocksDbWalletError> {
        if self.row_count != expected_count {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "address transaction rows do not exactly cover output create/spend effects",
            });
        }
        Ok(self.digest)
    }
}

/// Reads the address-transaction family back in `ranges` hash-prefix key
/// ranges, each on its own thread, and combines their partial accumulators.
///
/// Once one range fails, or a range thread cannot start, the others stop at
/// their next row, and the first failure is returned.
fn read_back_address_transaction_rows(
    bounded_open: &BoundedRocksDbOpen,
    expected_count: u64,
    retained_heights: BlockHeightRange,
    ranges: NonZeroU32,
) -> Result<WalletProjectionDigestBuilder, RocksDbWalletError> {
    let ranges = hash_prefix_key_ranges(ranges);
    let stop = AtomicBool::new(false);
    let scan = |keys: &HashPrefixKeyRange| {
        let scanned = scan_address_transaction_range(bounded_open, keys, retained_heights, &stop);
        if scanned.is_err() {
            stop.store(true, Ordering::Relaxed);
        }
        scanned
    };
    let scanned = std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(ranges.len());
        let mut start_failure = None;
        for keys in ranges.iter().skip(1) {
            match spawn_validation_scan(scope, move || scan(keys)) {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    stop.store(true, Ordering::Relaxed);
                    start_failure = Some(error);
                    break;
                }
            }
        }
        let mut scanned = Vec::with_capacity(ranges.len());
        if start_failure.is_none() {
            scanned.push(
                ranges
                    .first()
                    .map_or_else(|| Ok(Some(AddressTransactionTotals::new())), scan),
            );
        }
        for worker in workers {
            scanned.push(join_validation_scan(worker));
        }
        start_failure.map_or(Ok(scanned), Err)
    })?;
    let mut totals = AddressTransactionTotals::new();
    let mut stopped = false;
    for range_totals in scanned {
        match range_totals? {
            Some(range_totals) => totals.combine(&range_totals)?,
            None => stopped = true,
        }
    }
    if stopped {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "an address transaction scan range stopped without a failing range",
        });
    }
    totals.finish(expected_count)
}

/// Scans the address-transaction family over one key range, or returns `None`
/// when another range's failure stopped it first.
fn scan_address_transaction_range(
    bounded_open: &BoundedRocksDbOpen,
    keys: &HashPrefixKeyRange,
    retained_heights: BlockHeightRange,
    stop: &AtomicBool,
) -> Result<Option<AddressTransactionTotals>, RocksDbWalletError> {
    let family = column_family(bounded_open, TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY)?;
    let mut totals = AddressTransactionTotals::new();
    for row in bounded_open
        .db
        .iterator_cf_opt(&family, keys.read_options(), keys.start())
    {
        if stop.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let (key_bytes, value_bytes) = row.map_err(|source| {
            RocksDbWalletError::rocksdb("address transaction readback scan", source)
        })?;
        let key = WalletAddressTransactionKey::decode(&key_bytes)?;
        if key.block_height() < retained_heights.start || key.block_height() > retained_heights.end
        {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "address transaction references block height outside retained block hash table",
            });
        }
        WalletAddressTransaction::decode_stored_value(key, &value_bytes)?;
        totals.digest.append_row(
            WalletProjectionRowFamily::TransparentAddressTransaction,
            &key_bytes,
            &value_bytes,
        )?;
        totals.row_count =
            increment_validation_count(totals.row_count, "address transaction row count overflow")?;
    }
    Ok(Some(totals))
}

/// External sorters that collect the address-ordered rows implied by the
/// primary output families during semantic re-derivation.
struct AddressDerivationSorters {
    index: VariableValueSorter<ADDRESS_UNSPENT_KEY_BYTES>,
    transaction: VariableValueSorter<ADDRESS_TRANSACTION_KEY_BYTES>,
}

/// One contiguous range of hash-prefixed keys, bounded by two-byte prefixes.
///
/// A missing bound leaves that end of the key space open. Both outpoint keys
/// (leading 32-byte transaction id) and address-transaction keys (leading
/// 32-byte address script hash) distribute uniformly across this prefix space.
#[derive(Clone, Debug, Eq, PartialEq)]
struct HashPrefixKeyRange {
    lower: Option<[u8; 2]>,
    upper: Option<[u8; 2]>,
}

impl HashPrefixKeyRange {
    const ALL: Self = Self {
        lower: None,
        upper: None,
    };

    fn read_options(&self) -> ReadOptions {
        let mut options = validation_read_options();
        if let Some(upper) = self.upper {
            options.set_iterate_upper_bound(upper.to_vec());
        }
        options
    }

    fn start(&self) -> IteratorMode<'_> {
        self.lower.as_ref().map_or(IteratorMode::Start, |lower| {
            IteratorMode::From(lower.as_slice(), Direction::Forward)
        })
    }
}

/// Splits a hash-prefixed key space into `count` contiguous ranges of equal
/// width by the keys' first two bytes, at most one range per prefix.
///
/// Keys start with a cryptographic hash (`txid` or `address_script_hash`), so
/// the ranges hold about the same number of rows.
fn hash_prefix_key_ranges(count: NonZeroU32) -> Vec<HashPrefixKeyRange> {
    const PREFIXES: u64 = 1 << 16;
    let count = u64::from(count.get()).min(PREFIXES);
    let boundary = |index: u64| {
        let prefix = u16::try_from(index * PREFIXES / count).unwrap_or(u16::MAX);
        prefix.to_be_bytes()
    };
    (0..count)
        .map(|index| HashPrefixKeyRange {
            lower: (index > 0).then(|| boundary(index)),
            upper: (index + 1 < count).then(|| boundary(index + 1)),
        })
        .collect()
}

/// Accumulators and totals of the primary output rows in one key range.
struct PrimaryOutputTotals {
    digest: WalletProjectionDigestBuilder,
    unspent_count: u64,
    spent_count: u64,
    total_value_zat: u64,
    commitment: TransparentUtxoSetCommitment,
}

impl PrimaryOutputTotals {
    fn new() -> Self {
        Self {
            digest: WalletProjectionDigestBuilder::new(),
            unspent_count: 0,
            spent_count: 0,
            total_value_zat: 0,
            commitment: TransparentUtxoSetCommitment::empty(),
        }
    }

    /// Adds the totals of a disjoint key range.
    fn combine(&mut self, other: &Self) -> Result<(), RocksDbWalletError> {
        let overflow = |reason| RocksDbWalletError::AdmissionChanged { reason };
        let unspent_count = self
            .unspent_count
            .checked_add(other.unspent_count)
            .ok_or_else(|| overflow("unspent row count overflow"))?;
        let spent_count = self
            .spent_count
            .checked_add(other.spent_count)
            .ok_or_else(|| overflow("spent row count overflow"))?;
        let total_value_zat = self
            .total_value_zat
            .checked_add(other.total_value_zat)
            .ok_or_else(|| overflow("unspent value total overflow"))?;
        self.digest.combine(&other.digest)?;
        self.commitment.combine(&other.commitment)?;
        self.unspent_count = unspent_count;
        self.spent_count = spent_count;
        self.total_value_zat = total_value_zat;
        Ok(())
    }

    /// Checks the row counts against the READY evidence and returns the
    /// digest and the UTXO set summary.
    fn finish(
        self,
        counts: &zinder_wallet_projection::WalletProjectionFamilyRowCounts,
    ) -> Result<(WalletProjectionDigestBuilder, WalletUtxoSetSummary), RocksDbWalletError> {
        if self.unspent_count != counts.transparent_unspent_output_count {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "unspent row count differs from READY evidence",
            });
        }
        if self.spent_count != counts.transparent_spent_output_count {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "spent row count differs from READY evidence",
            });
        }
        Ok((
            self.digest,
            WalletUtxoSetSummary {
                utxo_count: self.unspent_count,
                total_value_zat: self.total_value_zat,
                commitment: self.commitment,
            },
        ))
    }
}

#[derive(Clone, Copy)]
struct PrimaryOutputValidationTarget<'a> {
    counts: &'a zinder_wallet_projection::WalletProjectionFamilyRowCounts,
    context: PrimaryValidationContext<'a>,
}

#[derive(Clone, Copy)]
struct ReadbackPrimaryPlan<'a> {
    ranges: NonZeroU32,
    context: PrimaryValidationContext<'a>,
}

/// Validates both primary output families in one ordered merged scan and
/// re-derives their address rows into `address_sorters` when given.
fn validate_primary_output_rows(
    bounded_open: &BoundedRocksDbOpen,
    network: Network,
    address_sorters: Option<&mut AddressDerivationSorters>,
    expected_undo: &SharedExpectedReorgUndoSuffix,
    target: PrimaryOutputValidationTarget<'_>,
) -> Result<(WalletProjectionDigestBuilder, WalletUtxoSetSummary), RocksDbWalletError> {
    let range = PrimaryScanRange {
        keys: &HashPrefixKeyRange::ALL,
        stop: None,
        context: target.context,
    };
    scan_primary_output_range(bounded_open, network, range, address_sorters, expected_undo)?
        .ok_or(RocksDbWalletError::AdmissionChanged {
            reason: "an unstoppable primary output scan stopped",
        })?
        .finish(target.counts)
}

/// Returns how many hash-prefix key ranges cold validation uses for a family
/// with `row_count` rows.
///
/// The count is the requested one, at most
/// [`crate::MAX_ACCUMULATOR_HASHING_WORKERS`], and at most one range per
/// `min_rows_per_scan_range` rows, so a small store starts few threads
/// whatever the setting.
fn scan_range_count(
    requested_ranges: NonZeroU32,
    row_count: u64,
    min_rows_per_scan_range: NonZeroU64,
) -> NonZeroU32 {
    let ranges_by_rows = row_count / min_rows_per_scan_range;
    let ranges = u64::from(requested_ranges.get())
        .min(u64::from(crate::MAX_ACCUMULATOR_HASHING_WORKERS))
        .min(ranges_by_rows);
    NonZeroU32::new(u32::try_from(ranges).unwrap_or(u32::MAX)).unwrap_or(NonZeroU32::MIN)
}

/// Returns how many outpoint key ranges cold validation reads the primary
/// output families in.
fn primary_output_scan_range_count(
    config: &WalletColdValidationConfig,
    counts: &zinder_wallet_projection::WalletProjectionFamilyRowCounts,
) -> NonZeroU32 {
    let primary_rows = counts
        .transparent_unspent_output_count
        .saturating_add(counts.transparent_spent_output_count);
    scan_range_count(
        config.family_scan_ranges,
        primary_rows,
        config.min_rows_per_scan_range,
    )
}

/// Returns how many address-prefix key ranges cold validation reads the
/// address-transaction family in.
fn address_transaction_scan_range_count(
    config: &WalletColdValidationConfig,
    counts: &zinder_wallet_projection::WalletProjectionFamilyRowCounts,
) -> NonZeroU32 {
    scan_range_count(
        config.family_scan_ranges,
        counts.transparent_address_transaction_count,
        config.min_rows_per_scan_range,
    )
}

/// Reads both primary output families back in `ranges` outpoint key ranges,
/// each on its own thread, and combines their totals.
///
/// Both families share the outpoint key, so each range sees every row of an
/// outpoint and runs the same per-row checks as one scan of the whole store.
/// Once one range fails, or a range thread cannot start, the others stop at
/// their next row, and the first failure is returned.
fn read_back_primary_output_rows(
    bounded_open: &BoundedRocksDbOpen,
    network: Network,
    expected_undo: &SharedExpectedReorgUndoSuffix,
    counts: &zinder_wallet_projection::WalletProjectionFamilyRowCounts,
    plan: ReadbackPrimaryPlan<'_>,
) -> Result<(WalletProjectionDigestBuilder, WalletUtxoSetSummary), RocksDbWalletError> {
    let ranges = hash_prefix_key_ranges(plan.ranges);
    let stop = AtomicBool::new(false);
    let scan = |keys: &HashPrefixKeyRange| {
        let range = PrimaryScanRange {
            keys,
            stop: Some(&stop),
            context: plan.context,
        };
        let scanned = scan_primary_output_range(bounded_open, network, range, None, expected_undo);
        if scanned.is_err() {
            stop.store(true, Ordering::Relaxed);
        }
        scanned
    };
    let scanned = std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(ranges.len());
        let mut start_failure = None;
        for keys in ranges.iter().skip(1) {
            match spawn_validation_scan(scope, move || scan(keys)) {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    stop.store(true, Ordering::Relaxed);
                    start_failure = Some(error);
                    break;
                }
            }
        }
        let mut scanned = Vec::with_capacity(ranges.len());
        if start_failure.is_none() {
            scanned.push(
                ranges
                    .first()
                    .map_or_else(|| Ok(Some(PrimaryOutputTotals::new())), scan),
            );
        }
        for worker in workers {
            scanned.push(join_validation_scan(worker));
        }
        start_failure.map_or(Ok(scanned), Err)
    })?;
    let mut totals = PrimaryOutputTotals::new();
    let mut stopped = false;
    for range_totals in scanned {
        match range_totals? {
            Some(range_totals) => totals.combine(&range_totals)?,
            None => stopped = true,
        }
    }
    if stopped {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "a primary output scan range stopped without a failing range",
        });
    }
    totals.finish(counts)
}

/// One key range of a primary output scan and the flag that stops it early.
#[derive(Clone, Copy)]
struct PrimaryScanRange<'scan> {
    keys: &'scan HashPrefixKeyRange,
    /// Set once another range has failed, so this one stops at its next row.
    stop: Option<&'scan AtomicBool>,
    context: PrimaryValidationContext<'scan>,
}

impl PrimaryScanRange<'_> {
    fn stopped(&self) -> bool {
        self.stop.is_some_and(|stop| stop.load(Ordering::Relaxed))
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the ordered two-family merge keeps every derived relation tied to its decoded primary row"
)]
/// Scans both primary output families over one key range, or returns `None`
/// when another range's failure stopped it first.
fn scan_primary_output_range(
    bounded_open: &BoundedRocksDbOpen,
    network: Network,
    range: PrimaryScanRange<'_>,
    mut address_sorters: Option<&mut AddressDerivationSorters>,
    expected_undo: &SharedExpectedReorgUndoSuffix,
) -> Result<Option<PrimaryOutputTotals>, RocksDbWalletError> {
    let unspent_family = column_family(bounded_open, TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY)?;
    let spent_family = column_family(bounded_open, TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY)?;
    let mut unspent_rows = bounded_open.db.iterator_cf_opt(
        &unspent_family,
        range.keys.read_options(),
        range.keys.start(),
    );
    let mut spent_rows = bounded_open.db.iterator_cf_opt(
        &spent_family,
        range.keys.read_options(),
        range.keys.start(),
    );
    let mut next_unspent = next_validation_row(&mut unspent_rows, "unspent validation scan")?;
    let mut next_spent = next_validation_row(&mut spent_rows, "spent validation scan")?;
    let PrimaryOutputTotals {
        digest: mut range_digest,
        mut unspent_count,
        mut spent_count,
        mut total_value_zat,
        mut commitment,
    } = PrimaryOutputTotals::new();
    let digest = &mut range_digest;
    while next_unspent.is_some() || next_spent.is_some() {
        if range.stopped() {
            return Ok(None);
        }
        let take_unspent = match (&next_unspent, &next_spent) {
            (Some((unspent_key, _)), Some((spent_key, _))) => {
                match unspent_key.as_ref().cmp(spent_key.as_ref()) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Greater => false,
                    std::cmp::Ordering::Equal => {
                        return Err(RocksDbWalletError::AdmissionChanged {
                            reason: "one outpoint appears in both unspent and spent output families",
                        });
                    }
                }
            }
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        if take_unspent {
            let (key_bytes, value_bytes) =
                next_unspent
                    .take()
                    .ok_or(RocksDbWalletError::AdmissionChanged {
                        reason: "unspent validation merge lost its current row",
                    })?;
            let key = WalletOutpointKey::decode(&key_bytes)?;
            let output = StoredWalletUnspentOutput::decode_value(key, &value_bytes)?;
            require_retained_height(
                output.created_at.block_height,
                range.context.retained_heights,
            )?;
            let address_key = output.address_unspent_output_key();
            if let Some(sorters) = address_sorters.as_deref_mut() {
                sorters
                    .index
                    .push(*address_key.as_bytes(), &output.value_zat.to_be_bytes())?;
            }
            stage_expected_address_transaction(
                address_sorters
                    .as_deref_mut()
                    .map(|sorters| &mut sorters.transaction),
                expected_undo,
                output.address_script_hash,
                output.created_at,
                range.context.unsettled_hashes,
            )?;
            expected_undo.observe_created(
                output.outpoint,
                output.created_at.block_height,
                range.context.unsettled_hashes,
            )?;
            digest.append_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                &key_bytes,
                &value_bytes,
            )?;
            unspent_count =
                increment_validation_count(unspent_count, "unspent row count overflow")?;
            total_value_zat = total_value_zat.checked_add(output.value_zat).ok_or(
                RocksDbWalletError::AdmissionChanged {
                    reason: "unspent value total overflow",
                },
            )?;
            commitment.insert(&output.commitment_element(network));
            next_unspent = next_validation_row(&mut unspent_rows, "unspent validation scan")?;
        } else {
            let (key_bytes, value_bytes) =
                next_spent
                    .take()
                    .ok_or(RocksDbWalletError::AdmissionChanged {
                        reason: "spent validation merge lost its current row",
                    })?;
            let key = WalletOutpointKey::decode(&key_bytes)?;
            let spent = StoredWalletSpentOutput::decode_value(key, &value_bytes)?;
            require_retained_height(
                spent.output.created_at.block_height,
                range.context.retained_heights,
            )?;
            require_retained_height(spent.spent_at.block_height, range.context.retained_heights)?;
            validate_spent_position(&spent)?;
            stage_expected_address_transaction(
                address_sorters
                    .as_deref_mut()
                    .map(|sorters| &mut sorters.transaction),
                expected_undo,
                spent.output.address_script_hash,
                spent.output.created_at,
                range.context.unsettled_hashes,
            )?;
            stage_expected_address_transaction(
                address_sorters
                    .as_deref_mut()
                    .map(|sorters| &mut sorters.transaction),
                expected_undo,
                spent.output.address_script_hash,
                spent.spent_at,
                range.context.unsettled_hashes,
            )?;
            expected_undo.observe_created(
                spent.output.outpoint,
                spent.output.created_at.block_height,
                range.context.unsettled_hashes,
            )?;
            if spent.output.created_at.block_height != spent.spent_at.block_height {
                expected_undo.observe_spent(
                    spent.output.outpoint,
                    spent.spent_at.block_height,
                    range.context.unsettled_hashes,
                )?;
            }
            digest.append_row(
                WalletProjectionRowFamily::TransparentSpentOutput,
                &key_bytes,
                &value_bytes,
            )?;
            spent_count = increment_validation_count(spent_count, "spent row count overflow")?;
            next_spent = next_validation_row(&mut spent_rows, "spent validation scan")?;
        }
    }
    Ok(Some(PrimaryOutputTotals {
        digest: range_digest,
        unspent_count,
        spent_count,
        total_value_zat,
        commitment,
    }))
}

fn require_retained_height(
    height: BlockHeight,
    retained_heights: BlockHeightRange,
) -> Result<(), RocksDbWalletError> {
    if height < retained_heights.start || height > retained_heights.end {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "output references block height outside retained block hash table",
        });
    }
    Ok(())
}

fn stage_expected_address_transaction(
    sorter: Option<&mut VariableValueSorter<ADDRESS_TRANSACTION_KEY_BYTES>>,
    expected_undo: &SharedExpectedReorgUndoSuffix,
    address_script_hash: TransparentAddressScriptHash,
    position: WalletStoredTransactionPosition,
    unsettled_hashes: &BTreeMap<BlockHeight, BlockHash>,
) -> Result<(), RocksDbWalletError> {
    let key = WalletAddressTransactionKey::new(
        address_script_hash,
        position.block_height,
        position.tx_index_in_block,
    );
    if let Some(sorter) = sorter {
        sorter.push(*key.as_bytes(), &position.transaction_id.as_bytes())?;
    }
    expected_undo.observe_address_transaction(key, position.block_height, unsettled_hashes)
}

#[allow(
    clippy::too_many_lines,
    reason = "the sequential index and balance comparison must retain one visible address-group state"
)]
fn validate_address_index_and_balance_rows(
    bounded_open: &BoundedRocksDbOpen,
    digest: &mut WalletProjectionDigestBuilder,
    expected_rows: &mut SortedVariableValues<ADDRESS_UNSPENT_KEY_BYTES>,
    counts: &zinder_wallet_projection::WalletProjectionFamilyRowCounts,
) -> Result<(), RocksDbWalletError> {
    let address_family = column_family(
        bounded_open,
        TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
    )?;
    let balance_family = column_family(bounded_open, TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY)?;
    let mut address_rows = bounded_open.db.iterator_cf_opt(
        &address_family,
        validation_read_options(),
        IteratorMode::Start,
    );
    let mut balance_rows = bounded_open.db.iterator_cf_opt(
        &balance_family,
        validation_read_options(),
        IteratorMode::Start,
    );
    let mut address_count = 0u64;
    let mut balance_count = 0u64;
    let mut current_address = None;
    let mut current_balance_zat = 0u64;
    while let Some(expected) = expected_rows.next_record()? {
        let (key_bytes, value_bytes) = next_validation_row(
            &mut address_rows,
            "address unspent validation scan",
        )?
        .ok_or(RocksDbWalletError::AdmissionChanged {
            reason: "address unspent index does not exactly cover every primary unspent output",
        })?;
        if key_bytes.as_ref() != expected.key.as_slice() || !value_bytes.is_empty() {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "address unspent index does not exactly cover every primary unspent output",
            });
        }
        let address_key = WalletAddressUnspentOutputKey::decode(&expected.key)?;
        let value_zat =
            u64::from_be_bytes(expected.encoded_value.as_slice().try_into().map_err(|_| {
                RocksDbWalletError::AdmissionChanged {
                    reason: "cold validation address-index value is not an exact u64",
                }
            })?);
        let address = address_key.address_script_hash();
        if current_address.is_some_and(|current| current != address) {
            validate_expected_balance(
                &mut balance_rows,
                digest,
                current_address.ok_or(RocksDbWalletError::AdmissionChanged {
                    reason: "address unspent validation group disappeared",
                })?,
                current_balance_zat,
                &mut balance_count,
            )?;
            current_balance_zat = 0;
        }
        current_address = Some(address);
        current_balance_zat = current_balance_zat.checked_add(value_zat).ok_or(
            RocksDbWalletError::AdmissionChanged {
                reason: "address unspent value total overflow",
            },
        )?;
        digest.append_row(
            WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
            &key_bytes,
            &value_bytes,
        )?;
        address_count =
            increment_validation_count(address_count, "address unspent row count overflow")?;
    }
    if next_validation_row(&mut address_rows, "address unspent validation scan")?.is_some()
        || address_count != counts.transparent_unspent_output_by_address_count
    {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "address unspent index does not exactly cover every primary unspent output",
        });
    }
    if let Some(address) = current_address {
        validate_expected_balance(
            &mut balance_rows,
            digest,
            address,
            current_balance_zat,
            &mut balance_count,
        )?;
    }
    if next_validation_row(&mut balance_rows, "address balance validation scan")?.is_some()
        || balance_count != counts.transparent_address_balance_count
    {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "address balance rows do not exactly cover positive indexed addresses",
        });
    }
    Ok(())
}

fn validate_expected_balance<I, Key, Value>(
    balance_rows: &mut I,
    digest: &mut WalletProjectionDigestBuilder,
    address_script_hash: TransparentAddressScriptHash,
    balance_zat: u64,
    balance_count: &mut u64,
) -> Result<(), RocksDbWalletError>
where
    I: Iterator<Item = Result<(Key, Value), rust_rocksdb::Error>>,
    Key: AsRef<[u8]>,
    Value: AsRef<[u8]>,
{
    if balance_zat == 0 {
        return Ok(());
    }
    let expected = WalletAddressBalance {
        address_script_hash,
        balance_zat,
    };
    let (key_bytes, value_bytes) =
        next_validation_row(balance_rows, "address balance validation scan")?.ok_or(
            RocksDbWalletError::AdmissionChanged {
                reason: "address balance rows differ from indexed unspent-output sums",
            },
        )?;
    let observed = WalletAddressBalance::decode(key_bytes.as_ref(), value_bytes.as_ref())?;
    if observed != expected {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "address balance rows differ from indexed unspent-output sums",
        });
    }
    digest.append_row(
        WalletProjectionRowFamily::TransparentAddressBalance,
        key_bytes.as_ref(),
        value_bytes.as_ref(),
    )?;
    *balance_count =
        increment_validation_count(*balance_count, "address balance row count overflow")?;
    Ok(())
}

fn validate_address_transaction_rows(
    bounded_open: &BoundedRocksDbOpen,
    digest: &mut WalletProjectionDigestBuilder,
    expected_count: u64,
    expected_rows: &mut SortedVariableValues<ADDRESS_TRANSACTION_KEY_BYTES>,
) -> Result<(), RocksDbWalletError> {
    let family = column_family(bounded_open, TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY)?;
    let mut rows =
        bounded_open
            .db
            .iterator_cf_opt(&family, validation_read_options(), IteratorMode::Start);
    let mut count = 0u64;
    let mut pending = expected_rows.next_record()?;
    while let Some(expected) = pending.take() {
        WalletAddressTransaction::decode_stored_value(
            WalletAddressTransactionKey::decode(&expected.key)?,
            &expected.encoded_value,
        )?;
        loop {
            let Some(next) = expected_rows.next_record()? else {
                validate_expected_address_transaction(&mut rows, digest, &expected, &mut count)?;
                break;
            };
            if next.key != expected.key {
                validate_expected_address_transaction(&mut rows, digest, &expected, &mut count)?;
                pending = Some(next);
                break;
            }
            if next.encoded_value != expected.encoded_value {
                return Err(RocksDbWalletError::AdmissionChanged {
                    reason: "one address transaction key resolves to different transactions",
                });
            }
        }
    }
    if next_validation_row(&mut rows, "address transaction validation scan")?.is_some()
        || count != expected_count
    {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "address transaction rows do not exactly cover output create/spend effects",
        });
    }
    Ok(())
}

fn validate_expected_address_transaction<I, Key, Value>(
    rows: &mut I,
    digest: &mut WalletProjectionDigestBuilder,
    expected: &zinder_rocksdb_bulk_load::VariableValueRecord<ADDRESS_TRANSACTION_KEY_BYTES>,
    count: &mut u64,
) -> Result<(), RocksDbWalletError>
where
    I: Iterator<Item = Result<(Key, Value), rust_rocksdb::Error>>,
    Key: AsRef<[u8]>,
    Value: AsRef<[u8]>,
{
    let (key_bytes, value_bytes) =
        next_validation_row(rows, "address transaction validation scan")?.ok_or(
            RocksDbWalletError::AdmissionChanged {
                reason: "address transaction rows do not exactly cover output create/spend effects",
            },
        )?;
    if key_bytes.as_ref() != expected.key.as_slice()
        || value_bytes.as_ref() != expected.encoded_value
    {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "address transaction rows differ from output create/spend effects",
        });
    }
    let key = WalletAddressTransactionKey::decode(key_bytes.as_ref())?;
    WalletAddressTransaction::decode_stored_value(key, value_bytes.as_ref())?;
    digest.append_row(
        WalletProjectionRowFamily::TransparentAddressTransaction,
        key_bytes.as_ref(),
        value_bytes.as_ref(),
    )?;
    *count = increment_validation_count(*count, "address transaction row count overflow")?;
    Ok(())
}

fn next_validation_row<I, Key, Value>(
    rows: &mut I,
    operation: &'static str,
) -> Result<Option<(Key, Value)>, RocksDbWalletError>
where
    I: Iterator<Item = Result<(Key, Value), rust_rocksdb::Error>>,
{
    rows.next()
        .transpose()
        .map_err(|source| RocksDbWalletError::rocksdb(operation, source))
}

fn increment_validation_count(
    count: u64,
    overflow_reason: &'static str,
) -> Result<u64, RocksDbWalletError> {
    count
        .checked_add(1)
        .ok_or(RocksDbWalletError::AdmissionChanged {
            reason: overflow_reason,
        })
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one ordered suffix scan keeps READY floor, row effects, digest continuity, and tip evidence coupled"
)]
fn validate_reorg_undo_rows(
    bounded_open: &BoundedRocksDbOpen,
    digest: &mut WalletProjectionDigestBuilder,
    expected_count: u64,
    ready_tip: zinder_core::BlockId,
    settled_tip: zinder_core::BlockId,
    ready_source_sequence_digest: CanonicalBlockFactsSequenceDigest,
    expected_by_height: &BTreeMap<u32, ExpectedReorgUndoEffects>,
) -> Result<(), RocksDbWalletError> {
    let family = column_family(bounded_open, REORG_UNDO_COLUMN_FAMILY)?;
    let first_height = u64::from(settled_tip.height.value()).checked_add(1).ok_or(
        RocksDbWalletError::AdmissionChanged {
            reason: "reorg undo suffix falls outside the READY source range",
        },
    )?;
    let mut count = 0u64;
    let mut last_undo = None;
    let mut previous_block = settled_tip;
    let mut previous_source_sequence_digest = None;
    for row in
        bounded_open
            .db
            .iterator_cf_opt(&family, validation_read_options(), IteratorMode::Start)
    {
        let (key, encoded_value) = row
            .map_err(|source| RocksDbWalletError::rocksdb("reorg undo validation scan", source))?;
        let undo = WalletReorgUndo::decode(&key, &encoded_value)?;
        let expected_height =
            first_height
                .checked_add(count)
                .ok_or(RocksDbWalletError::AdmissionChanged {
                    reason: "reorg undo suffix height overflow",
                })?;
        if u64::from(undo.block.height.value()) != expected_height {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "reorg undo rows are not the exact contiguous READY suffix",
            });
        }
        if undo.parent_hash != previous_block.hash {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "reorg undo rows do not chain from the READY settled tip",
            });
        }
        if previous_source_sequence_digest
            .is_some_and(|digest| digest != undo.source_sequence_digest_before)
        {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "reorg undo rows have disconnected source sequence digests",
            });
        }
        let expected = expected_by_height.get(&undo.block.height.value()).ok_or(
            RocksDbWalletError::AdmissionChanged {
                reason: "reorg undo row falls outside the reconstructed suffix",
            },
        )?;
        if expected.block.is_some_and(|block| block != undo.block)
            || !undo
                .created_outpoints
                .iter()
                .copied()
                .eq(expected.created_outpoints.iter().copied())
            || !undo
                .spent_outpoints
                .iter()
                .copied()
                .eq(expected.spent_outpoints.iter().copied())
            || !undo
                .address_transaction_keys
                .iter()
                .copied()
                .eq(expected.address_transaction_keys.iter().copied())
        {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "reorg undo row differs from reconstructed wallet block effects",
            });
        }
        digest.append_row(WalletProjectionRowFamily::ReorgUndo, &key, &encoded_value)?;
        count = count
            .checked_add(1)
            .ok_or(RocksDbWalletError::AdmissionChanged {
                reason: "reorg undo row count overflow",
            })?;
        previous_block = undo.block;
        previous_source_sequence_digest = Some(undo.source_sequence_digest_after);
        last_undo = Some(undo);
    }
    if count != expected_count
        || u64::try_from(expected_by_height.len()).ok() != Some(expected_count)
    {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "reorg undo row count differs from READY evidence",
        });
    }
    if let Some(last_undo) = last_undo
        && last_undo.block != ready_tip
    {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "tip reorg undo does not match the READY source tip",
        });
    }
    if previous_source_sequence_digest.is_some_and(|digest| digest != ready_source_sequence_digest)
    {
        return Err(RocksDbWalletError::AdmissionChanged {
            reason: "tip reorg undo source digest does not match the READY source digest",
        });
    }
    Ok(())
}

pub(crate) fn column_family<'db>(
    bounded_open: &'db BoundedRocksDbOpen,
    name: &'static str,
) -> Result<Arc<BoundColumnFamily<'db>>, RocksDbWalletError> {
    bounded_open
        .db
        .cf_handle(name)
        .ok_or(RocksDbWalletError::ColumnFamilyUnavailable { name })
}

fn validation_read_options() -> ReadOptions {
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    options.set_readahead_size(2 * 1024 * 1024);
    options
}

#[cfg(test)]
mod tests {
    use rust_rocksdb::statistics::Ticker;
    use tempfile::TempDir;
    use zinder_core::wire::UtxoSetCommitmentElement;
    use zinder_core::{
        BlockHash, BlockHeight, BlockId, CanonicalBlockFactsSequenceDigest,
        CanonicalBlockFactsSequenceDigestVersion, ChainEpochId, TransactionId,
        TransparentUtxoSetCommitment, UnixTimestampMillis,
    };
    use zinder_wallet_projection::{
        WalletProjectionBuildLeaseRequest, WalletProjectionBuildOwner, WalletProjectionDigest,
        WalletProjectionFamilyRowCounts, WalletProjectionRetainedEventAnchor,
        WalletProjectionSourcePosition, WalletTransactionPosition,
    };

    use super::*;

    const TEST_VALIDATION_SORT_MEMORY_BYTES: u64 = 16 * 1024 * 1024;
    const TEST_VALIDATION_TEMPORARY_FILE_BYTES: u64 = 256 * 1024 * 1024;
    const TEST_VALIDATION_REORG_UNDO_BYTES: u64 = 16 * 1024 * 1024;

    /// A wallet secondary may share its block cache with other wallet
    /// secondaries, so no data family erases the blocks of a file it drops.
    #[test]
    fn data_families_keep_the_blocks_of_dropped_table_files() {
        let options = WalletColumnFamilyOptions::new(
            &rust_rocksdb::Cache::new_lru_cache(1024 * 1024),
            RocksDbResourceBudget::for_local_tests(),
        );
        for (name, family) in WALLET_DATA_COLUMN_FAMILIES {
            assert_eq!(
                options.for_family(family).get_uncache_aggressiveness(),
                0,
                "{name}"
            );
        }
    }

    #[test]
    fn wallet_data_families_use_expected_compression_and_bulk_options()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let family_options = WalletColumnFamilyOptions::new(
            &Cache::new_lru_cache(1024 * 1024),
            RocksDbResourceBudget::for_local_tests(),
        );
        for (name, family) in WALLET_DATA_COLUMN_FAMILIES {
            let options = family_options.for_family(family);
            let bulk_options = family_options.for_bulk_sst(family);
            if matches!(family, WalletProjectionRowFamily::BlockHashByHeight) {
                assert_eq!(
                    options.get_compression_type(),
                    Some(DBCompressionType::None),
                    "{name}"
                );
                assert_eq!(options.get_bottommost_compression_type(), None, "{name}");
                assert_eq!(
                    bulk_options.get_compression_type(),
                    Some(DBCompressionType::None),
                    "{name}"
                );
            } else {
                assert_eq!(
                    options.get_compression_type(),
                    Some(DBCompressionType::Lz4),
                    "{name}"
                );
                assert_eq!(
                    options.get_bottommost_compression_type(),
                    Some(DBCompressionType::Zstd),
                    "{name}"
                );
                assert_eq!(
                    bulk_options.get_compression_type(),
                    Some(DBCompressionType::Zstd),
                    "{name}"
                );
            }
            let mut writer = zinder_rocksdb_bulk_load::OrderedSstWriter::new(
                temporary.path(),
                name,
                bulk_options,
                1024 * 1024,
            )?;
            for index in 0_u32..512 {
                writer.put(&index.to_be_bytes(), &[0x7b; 72])?;
            }
            let sst = writer.finish()?;
            if !matches!(family, WalletProjectionRowFamily::BlockHashByHeight) {
                assert!(sst.file_bytes < 512 * 72 / 2, "{name}");
            }
        }
        Ok(())
    }

    const fn validation_config() -> WalletColdValidationConfig {
        WalletColdValidationConfig {
            max_accounted_reorg_undo_bytes: TEST_VALIDATION_REORG_UNDO_BYTES,
            family_scan_ranges: NonZeroU32::MIN.saturating_add(1),
            min_rows_per_scan_range: NonZeroU64::MIN,
        }
    }

    /// Key-range counts the readback tests cover: one range, two, and enough
    /// to put most fixture rows in ranges of their own.
    const PRIMARY_OUTPUT_SCAN_RANGES: [u32; 4] = [1, 2, 8, 64];

    fn validation_config_with_ranges(
        ranges: u32,
    ) -> Result<WalletColdValidationConfig, Box<dyn std::error::Error>> {
        Ok(WalletColdValidationConfig {
            family_scan_ranges: NonZeroU32::new(ranges).ok_or("zero scan ranges")?,
            ..validation_config()
        })
    }

    fn semantic_validation_config(staging_path: &Path) -> WalletSemanticValidationConfig<'_> {
        WalletSemanticValidationConfig {
            staging_path,
            max_sort_memory_bytes_per_sorter: TEST_VALIDATION_SORT_MEMORY_BYTES,
            max_temporary_file_bytes_per_sorter: TEST_VALIDATION_TEMPORARY_FILE_BYTES,
            max_accounted_reorg_undo_bytes: TEST_VALIDATION_REORG_UNDO_BYTES,
        }
    }

    struct SemanticValidationFixture {
        supported_reorg_depth: u32,
        unspent_outputs: Vec<(WalletOutpointKey, WalletUnspentOutput)>,
        unspent_output_by_address: Vec<WalletAddressUnspentOutputKey>,
        spent_outputs: Vec<(WalletOutpointKey, WalletSpentOutput)>,
        address_transactions: Vec<WalletAddressTransaction>,
        address_balances: Vec<WalletAddressBalance>,
        reorg_undo: Vec<WalletReorgUndo>,
        block_hashes: Vec<WalletBlockHash>,
        row_counts: WalletProjectionFamilyRowCounts,
        projection_digest: WalletProjectionDigest,
    }

    fn load_semantic_validation_fixture(
        builder: &RocksDbWalletBuilder,
        fixture: &SemanticValidationFixture,
    ) -> Result<(), RocksDbWalletError> {
        let mut batch = WriteBatch::default();
        let unspent_family = column_family(
            &builder.bounded_open,
            TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
        )?;
        for (key, output) in &fixture.unspent_outputs {
            batch.put_cf(&unspent_family, key.as_bytes(), output.encode_value()?);
        }
        let address_index_family = column_family(
            &builder.bounded_open,
            TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
        )?;
        for key in &fixture.unspent_output_by_address {
            batch.put_cf(&address_index_family, key.as_bytes(), []);
        }
        let spent_family = column_family(
            &builder.bounded_open,
            TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
        )?;
        for (key, output) in &fixture.spent_outputs {
            batch.put_cf(&spent_family, key.as_bytes(), output.encode_value()?);
        }
        let address_transaction_family = column_family(
            &builder.bounded_open,
            TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
        )?;
        for transaction in &fixture.address_transactions {
            batch.put_cf(
                &address_transaction_family,
                transaction.key.as_bytes(),
                transaction.encode_value(),
            );
        }
        let balance_family = column_family(
            &builder.bounded_open,
            TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
        )?;
        for balance in &fixture.address_balances {
            batch.put_cf(
                &balance_family,
                balance.encode_key(),
                balance.encode_value(),
            );
        }
        let undo_family = column_family(&builder.bounded_open, REORG_UNDO_COLUMN_FAMILY)?;
        for undo in &fixture.reorg_undo {
            batch.put_cf(&undo_family, undo.encode_key(), undo.encode_value()?);
        }
        let block_hash_family =
            column_family(&builder.bounded_open, BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY)?;
        for block_hash in &fixture.block_hashes {
            batch.put_cf(
                &block_hash_family,
                block_hash.encode_key(),
                block_hash.encode_value(),
            );
        }
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);
        builder
            .bounded_open
            .db
            .write_opt(&batch, &write_options)
            .map_err(|source| RocksDbWalletError::rocksdb("semantic validation fixture", source))
    }

    fn source_position() -> WalletProjectionSourcePosition {
        WalletProjectionSourcePosition::new(
            ChainEpochId::new(1),
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0x11; 32])),
            1,
        )
    }

    fn settled_tip() -> BlockId {
        BlockId::new(BlockHeight::new(0), Network::ZcashRegtest.genesis_hash())
    }

    fn source_identity() -> WalletCanonicalSourceIdentity {
        WalletCanonicalSourceIdentity::new(
            source_position(),
            CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                CanonicalBlockFactsSequenceDigestVersion::V1,
                1,
                [0x77; 32],
            ),
            source_position().tip,
        )
    }

    fn valid_reorg_undo(block: BlockId) -> WalletReorgUndo {
        WalletReorgUndo {
            block,
            parent_hash: Network::ZcashRegtest.genesis_hash(),
            source_sequence_digest_before:
                CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                    CanonicalBlockFactsSequenceDigestVersion::V1,
                    0,
                    [0x76; 32],
                ),
            source_sequence_digest_after: source_identity().source_sequence_digest(),
            created_outpoints: Vec::new(),
            spent_outpoints: Vec::new(),
            address_transaction_keys: Vec::new(),
        }
    }

    fn test_lease_request() -> WalletProjectionBuildLeaseRequest {
        WalletProjectionBuildLeaseRequest::new(
            WalletProjectionBuildOwner::from_bytes([0x55; 16]),
            source_identity(),
            WalletProjectionRetainedEventAnchor::new(
                source_identity().source_position().event_sequence,
            ),
            UnixTimestampMillis::new(u64::MAX),
        )
    }

    fn fresh_builder(
        path: impl AsRef<Path>,
        network: Network,
        _target_source_position: WalletProjectionSourcePosition,
        supported_reorg_depth: u32,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<RocksDbWalletBuilder, RocksDbWalletError> {
        let build_store = RocksDbWalletBuildStore::create_fresh(
            path,
            network,
            source_identity(),
            supported_reorg_depth,
            resource_budget,
        )?;
        RocksDbWalletBuilder::create_fresh(
            build_store,
            test_lease_request(),
            UnixTimestampMillis::new(0),
        )
    }

    fn sample_output(
        transaction_byte: u8,
        value_zat: u64,
    ) -> Result<WalletUnspentOutput, zinder_wallet_projection::WalletProjectionContractError> {
        let transaction_id = TransactionId::from_bytes([transaction_byte; 32]);
        WalletUnspentOutput::new(
            TransparentOutPoint::new(transaction_id, 0),
            TransparentAddressScriptHash::from_bytes([transaction_byte; 32]),
            value_zat,
            [0x51],
            WalletTransactionPosition::new(transaction_id, 0, source_position().tip),
        )
    }

    fn expected_address_transactions(
        unspent: &WalletUnspentOutput,
        spent: Option<&WalletSpentOutput>,
    ) -> Vec<WalletAddressTransaction> {
        let mut positions = vec![(unspent.address_script_hash, unspent.created_at)];
        if let Some(spent) = spent {
            positions.push((spent.output.address_script_hash, spent.output.created_at));
            positions.push((spent.output.address_script_hash, spent.spent_at));
        }
        let mut transactions = positions
            .into_iter()
            .map(|(address_script_hash, position)| {
                let key = WalletAddressTransactionKey::new(
                    address_script_hash,
                    position.block.height,
                    position.tx_index_in_block,
                );
                WalletAddressTransaction::new(key, position.transaction_id, position.block.hash)
            })
            .collect::<Vec<_>>();
        transactions.sort_unstable_by_key(|transaction| transaction.key);
        transactions.dedup_by_key(|transaction| transaction.key);
        transactions
    }

    fn projection_evidence(
        unspent: &WalletUnspentOutput,
        spent: &WalletSpentOutput,
        balance: WalletAddressBalance,
    ) -> Result<
        (
            zinder_wallet_projection::WalletProjectionAccumulator,
            WalletProjectionDigest,
        ),
        zinder_wallet_projection::WalletProjectionContractError,
    > {
        let mut digest = WalletProjectionDigestBuilder::new();
        let unspent_key = WalletOutpointKey::new(unspent.outpoint);
        digest.append_row(
            WalletProjectionRowFamily::TransparentUnspentOutput,
            unspent_key.as_bytes(),
            &unspent.encode_value()?,
        )?;
        let address_key = WalletAddressUnspentOutputKey::new(unspent);
        digest.append_row(
            WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
            address_key.as_bytes(),
            &[],
        )?;
        let spent_key = WalletOutpointKey::new(spent.output.outpoint);
        digest.append_row(
            WalletProjectionRowFamily::TransparentSpentOutput,
            spent_key.as_bytes(),
            &spent.encode_value()?,
        )?;
        let address_transactions = expected_address_transactions(unspent, Some(spent));
        for transaction in address_transactions {
            digest.append_row(
                WalletProjectionRowFamily::TransparentAddressTransaction,
                transaction.key.as_bytes(),
                &transaction.encode_value(),
            )?;
        }
        digest.append_row(
            WalletProjectionRowFamily::TransparentAddressBalance,
            &balance.encode_key(),
            &balance.encode_value(),
        )?;
        let tip_hash = WalletBlockHash::new(source_position().tip);
        digest.append_row(
            WalletProjectionRowFamily::BlockHashByHeight,
            &tip_hash.encode_key(),
            &tip_hash.encode_value(),
        )?;
        Ok(digest.finish_with_accumulator())
    }

    fn ready_evidence(
        network: Network,
        unspent: &WalletUnspentOutput,
        spent: &WalletSpentOutput,
        balance: WalletAddressBalance,
    ) -> Result<
        WalletProjectionReadyEvidence,
        zinder_wallet_projection::WalletProjectionContractError,
    > {
        let mut commitment = TransparentUtxoSetCommitment::empty();
        commitment.insert(&UtxoSetCommitmentElement {
            network_id: network.id(),
            outpoint: unspent.outpoint,
            value_zat: unspent.value_zat,
            script_pub_key: &unspent.script_pub_key,
            block_height: unspent.created_at.block.height,
        });
        let (projection_accumulator, projection_digest) =
            projection_evidence(unspent, spent, balance)?;
        Ok(WalletProjectionReadyEvidence {
            source_position: source_position(),
            source_sequence_digest:
                CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                    CanonicalBlockFactsSequenceDigestVersion::V1,
                    1,
                    [0x77; 32],
                ),
            settled_tip: source_position().tip,
            projection_digest,
            projection_accumulator,
            row_counts: WalletProjectionFamilyRowCounts {
                transparent_unspent_output_count: 1,
                transparent_unspent_output_by_address_count: 1,
                transparent_spent_output_count: 1,
                transparent_address_transaction_count: 3,
                transparent_address_balance_count: 1,
                reorg_undo_count: 0,
                block_hash_by_height_count: 1,
            },
            utxo_summary: WalletUtxoSetSummary {
                utxo_count: 1,
                total_value_zat: unspent.value_zat,
                commitment,
            },
        })
    }

    fn semantic_validation_fixture(
        network: Network,
        unspent: &WalletUnspentOutput,
        spent: &WalletSpentOutput,
        balance: WalletAddressBalance,
    ) -> Result<SemanticValidationFixture, zinder_wallet_projection::WalletProjectionContractError>
    {
        let evidence = ready_evidence(network, unspent, spent, balance)?;
        Ok(SemanticValidationFixture {
            supported_reorg_depth: 0,
            unspent_outputs: vec![(WalletOutpointKey::new(unspent.outpoint), unspent.clone())],
            unspent_output_by_address: vec![WalletAddressUnspentOutputKey::new(unspent)],
            spent_outputs: vec![(WalletOutpointKey::new(spent.output.outpoint), spent.clone())],
            address_transactions: expected_address_transactions(unspent, Some(spent)),
            address_balances: vec![balance],
            reorg_undo: Vec::new(),
            block_hashes: vec![WalletBlockHash::new(source_position().tip)],
            row_counts: evidence.row_counts,
            projection_digest: evidence.projection_digest,
        })
    }

    fn zero_value_ready_evidence(
        network: Network,
        unspent: &WalletUnspentOutput,
    ) -> Result<
        WalletProjectionReadyEvidence,
        zinder_wallet_projection::WalletProjectionContractError,
    > {
        let unspent_key = WalletOutpointKey::new(unspent.outpoint);
        let address_key = WalletAddressUnspentOutputKey::new(unspent);
        let mut digest = WalletProjectionDigestBuilder::new();
        digest.append_row(
            WalletProjectionRowFamily::TransparentUnspentOutput,
            unspent_key.as_bytes(),
            &unspent.encode_value()?,
        )?;
        digest.append_row(
            WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
            address_key.as_bytes(),
            &[],
        )?;
        let address_transactions = expected_address_transactions(unspent, None);
        for transaction in address_transactions {
            digest.append_row(
                WalletProjectionRowFamily::TransparentAddressTransaction,
                transaction.key.as_bytes(),
                &transaction.encode_value(),
            )?;
        }
        let tip_hash = WalletBlockHash::new(source_position().tip);
        digest.append_row(
            WalletProjectionRowFamily::BlockHashByHeight,
            &tip_hash.encode_key(),
            &tip_hash.encode_value(),
        )?;

        let mut commitment = TransparentUtxoSetCommitment::empty();
        commitment.insert(&UtxoSetCommitmentElement {
            network_id: network.id(),
            outpoint: unspent.outpoint,
            value_zat: 0,
            script_pub_key: &unspent.script_pub_key,
            block_height: unspent.created_at.block.height,
        });
        let (projection_accumulator, projection_digest) = digest.finish_with_accumulator();
        Ok(WalletProjectionReadyEvidence {
            source_position: source_position(),
            source_sequence_digest: source_identity().source_sequence_digest(),
            settled_tip: source_position().tip,
            projection_digest,
            projection_accumulator,
            row_counts: WalletProjectionFamilyRowCounts {
                transparent_unspent_output_count: 1,
                transparent_unspent_output_by_address_count: 1,
                transparent_spent_output_count: 0,
                transparent_address_transaction_count: 1,
                transparent_address_balance_count: 0,
                reorg_undo_count: 0,
                block_hash_by_height_count: 1,
            },
            utxo_summary: WalletUtxoSetSummary {
                utxo_count: 1,
                total_value_zat: 0,
                commitment,
            },
        })
    }

    fn zero_value_semantic_validation_fixture(
        network: Network,
        unspent: &WalletUnspentOutput,
    ) -> Result<SemanticValidationFixture, zinder_wallet_projection::WalletProjectionContractError>
    {
        let evidence = zero_value_ready_evidence(network, unspent)?;
        Ok(SemanticValidationFixture {
            supported_reorg_depth: 0,
            unspent_outputs: vec![(WalletOutpointKey::new(unspent.outpoint), unspent.clone())],
            unspent_output_by_address: vec![WalletAddressUnspentOutputKey::new(unspent)],
            spent_outputs: Vec::new(),
            address_transactions: expected_address_transactions(unspent, None),
            address_balances: Vec::new(),
            reorg_undo: Vec::new(),
            block_hashes: vec![WalletBlockHash::new(source_position().tip)],
            row_counts: evidence.row_counts,
            projection_digest: evidence.projection_digest,
        })
    }

    #[derive(Clone, Copy, Debug)]
    enum SemanticTamper {
        OrphanAddressIndex,
        MissingAddressIndex,
        OverlappingOutputState,
        OutOfRangeBlockHeight,
        NonForwardSameBlockSpend,
        IncorrectBalance,
        MissingAddressTransaction,
        IncorrectUndo,
    }

    impl SemanticTamper {
        /// Tampering that only changes an address-ordered family so that it
        /// disagrees with the primary output rows while its rows, counts, and
        /// the READY digest stay mutually consistent. Digest readback cannot
        /// see this; semantic re-derivation can.
        const fn is_visible_only_to_semantic_rederivation(self) -> bool {
            matches!(
                self,
                Self::OrphanAddressIndex | Self::MissingAddressTransaction
            )
        }

        const fn expected_reason(self) -> &'static str {
            match self {
                Self::OrphanAddressIndex | Self::MissingAddressIndex => {
                    "address unspent index does not exactly cover every primary unspent output"
                }
                Self::OverlappingOutputState => {
                    "one outpoint appears in both unspent and spent output families"
                }
                Self::OutOfRangeBlockHeight => {
                    "output references block height outside retained block hash table"
                }
                Self::NonForwardSameBlockSpend => {
                    "spent output does not follow its exact canonical creation position"
                }
                Self::IncorrectBalance => {
                    "address balance rows differ from indexed unspent-output sums"
                }
                Self::MissingAddressTransaction => {
                    "address transaction rows do not exactly cover output create/spend effects"
                }
                Self::IncorrectUndo => {
                    "reorg undo row differs from reconstructed wallet block effects"
                }
            }
        }
    }

    fn refresh_prepared_evidence(
        prepared: &mut SemanticValidationFixture,
        evidence: &mut WalletProjectionReadyEvidence,
    ) -> Result<(), zinder_wallet_projection::WalletProjectionContractError> {
        let mut digest = WalletProjectionDigestBuilder::new();
        for (key, output) in &prepared.unspent_outputs {
            digest.append_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                key.as_bytes(),
                &output.encode_value()?,
            )?;
        }
        for key in &prepared.unspent_output_by_address {
            digest.append_row(
                WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
                key.as_bytes(),
                &[],
            )?;
        }
        for (key, output) in &prepared.spent_outputs {
            digest.append_row(
                WalletProjectionRowFamily::TransparentSpentOutput,
                key.as_bytes(),
                &output.encode_value()?,
            )?;
        }
        for transaction in &prepared.address_transactions {
            digest.append_row(
                WalletProjectionRowFamily::TransparentAddressTransaction,
                transaction.key.as_bytes(),
                &transaction.encode_value(),
            )?;
        }
        for balance in &prepared.address_balances {
            digest.append_row(
                WalletProjectionRowFamily::TransparentAddressBalance,
                &balance.encode_key(),
                &balance.encode_value(),
            )?;
        }
        for undo in &prepared.reorg_undo {
            digest.append_row(
                WalletProjectionRowFamily::ReorgUndo,
                &undo.encode_key(),
                &undo.encode_value()?,
            )?;
        }
        for block_hash in &prepared.block_hashes {
            digest.append_row(
                WalletProjectionRowFamily::BlockHashByHeight,
                &block_hash.encode_key(),
                &block_hash.encode_value(),
            )?;
        }
        prepared.row_counts = digest.row_counts();
        let (projection_accumulator, projection_digest) = digest.finish_with_accumulator();
        prepared.projection_digest = projection_digest;
        evidence.row_counts = prepared.row_counts;
        evidence.projection_digest = prepared.projection_digest;
        evidence.projection_accumulator = projection_accumulator;
        Ok(())
    }

    fn apply_semantic_tamper(
        tamper: SemanticTamper,
        prepared: &mut SemanticValidationFixture,
        evidence: &mut WalletProjectionReadyEvidence,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match tamper {
            SemanticTamper::OrphanAddressIndex => {
                let orphan = sample_output(0x99, 1)?;
                prepared.unspent_output_by_address =
                    vec![WalletAddressUnspentOutputKey::new(&orphan)];
            }
            SemanticTamper::MissingAddressIndex => {
                prepared.unspent_output_by_address.clear();
            }
            SemanticTamper::OverlappingOutputState => {
                let output = prepared.unspent_outputs[0].1.clone();
                let spent = WalletSpentOutput::new(
                    output.clone(),
                    WalletTransactionPosition::new(
                        TransactionId::from_bytes([0x55; 32]),
                        1,
                        output.created_at.block,
                    ),
                    0,
                );
                prepared
                    .spent_outputs
                    .push((WalletOutpointKey::new(output.outpoint), spent));
                prepared.spent_outputs.sort_unstable_by_key(|(key, _)| *key);
            }
            SemanticTamper::OutOfRangeBlockHeight => {
                let spent = prepared.spent_outputs[0].1.clone();
                prepared.spent_outputs[0].1 = WalletSpentOutput::new(
                    spent.output,
                    WalletTransactionPosition::new(
                        spent.spent_at.transaction_id,
                        spent.spent_at.tx_index_in_block,
                        BlockId::new(BlockHeight::new(99), BlockHash::from_bytes([0xaa; 32])),
                    ),
                    spent.input_index,
                );
            }
            SemanticTamper::NonForwardSameBlockSpend => {
                let spent = prepared.spent_outputs[0].1.clone();
                prepared.spent_outputs[0].1 = WalletSpentOutput::new(
                    spent.output.clone(),
                    WalletTransactionPosition::new(
                        spent.spent_at.transaction_id,
                        spent.output.created_at.tx_index_in_block,
                        spent.output.created_at.block,
                    ),
                    spent.input_index,
                );
            }
            SemanticTamper::IncorrectBalance => {
                prepared.address_balances[0].balance_zat = prepared.address_balances[0]
                    .balance_zat
                    .checked_add(1)
                    .ok_or("test balance overflow")?;
            }
            SemanticTamper::MissingAddressTransaction => {
                prepared.address_transactions.pop();
            }
            SemanticTamper::IncorrectUndo => {
                prepared.supported_reorg_depth = 1;
                prepared.reorg_undo = vec![valid_reorg_undo(source_position().tip)];
                evidence.settled_tip = settled_tip();
            }
        }
        refresh_prepared_evidence(prepared, evidence)?;
        Ok(())
    }

    #[test]
    fn building_store_refuses_ready_admission_and_nonfresh_reuse()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let path = temporary.path().join("wallet");
        let builder = fresh_builder(
            &path,
            Network::ZcashRegtest,
            source_position(),
            0,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        drop(builder);

        assert!(matches!(
            fresh_builder(
                &path,
                Network::ZcashRegtest,
                source_position(),
                0,
                RocksDbResourceBudget::for_local_tests(),
            ),
            Err(RocksDbWalletError::PathNotFresh { .. })
        ));
        assert!(matches!(
            RocksDbWalletStore::open_ready(
                &path,
                Network::ZcashRegtest,
                source_identity(),
                RocksDbResourceBudget::for_local_tests(),
            ),
            Err(RocksDbWalletError::StoreNotReady { .. })
        ));
        Ok(())
    }

    fn create_test_build_store(path: &Path) -> Result<RocksDbWalletBuildStore, RocksDbWalletError> {
        RocksDbWalletBuildStore::create_fresh(
            path,
            Network::ZcashRegtest,
            source_identity(),
            0,
            RocksDbResourceBudget::for_local_tests(),
        )
    }

    #[cfg(unix)]
    const BUILD_STORE_CREATION_CRASH_CHILD: &str = "store::tests::build_store_creation_crash_child";

    #[test]
    fn build_store_creation_crash_child() -> Result<(), Box<dyn std::error::Error>> {
        let Some(child) = zinder_testkit::process_crash::crash_child() else {
            return Ok(());
        };
        create_test_build_store(&child.store_path().join("wallet"))?;
        Err("build-store creation returned without reaching its crash point".into())
    }

    #[cfg(unix)]
    #[test]
    fn interrupted_build_store_creation_leaves_the_wallet_path_absent_across_process_abort()
    -> Result<(), Box<dyn std::error::Error>> {
        for crash_point in [
            "wallet_projection.build_store.after_initialization_directory",
            "wallet_projection.build_store.before_wallet_path_rename",
        ] {
            let temporary = TempDir::new()?;
            let wallet_path = temporary.path().join("wallet");
            zinder_testkit::process_crash::run_crash_child(
                BUILD_STORE_CREATION_CRASH_CHILD,
                temporary.path(),
                crash_point,
            )?;

            assert!(!wallet_path.exists(), "{crash_point} exposed a wallet path");
            assert!(crate::build::store_initialization_path(&wallet_path).is_dir());
            assert!(matches!(
                create_test_build_store(&wallet_path),
                Err(RocksDbWalletError::PathNotFresh { .. })
            ));
            let build_lock = WalletBuildLock::acquire(&wallet_path)?;
            crate::build::remove_abandoned_build_paths(&wallet_path, &build_lock)?;
            drop(create_test_build_store(&wallet_path)?);
            drop(RocksDbWalletBuildStore::open(
                &wallet_path,
                Network::ZcashRegtest,
                RocksDbResourceBudget::for_local_tests(),
            )?);
        }
        Ok(())
    }

    #[test]
    fn a_running_build_of_the_path_refuses_discard_for_any_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let wallet_path = temporary.path().join("wallet");
        drop(create_test_build_store(&wallet_path)?);
        let open_build = || {
            RocksDbWalletBuildStore::open(
                &wallet_path,
                Network::ZcashRegtest,
                RocksDbResourceBudget::for_local_tests(),
            )
        };
        let running_build = WalletBuildLock::acquire(&wallet_path)?;

        assert!(matches!(
            open_build()?.discard_unpublished(
                WalletProjectionBuildOwner::from_bytes([0x42; 16]),
                UnixTimestampMillis::new(0),
            ),
            Err(RocksDbWalletError::WalletBuildInProgress { .. })
        ));
        assert!(wallet_path.exists());
        drop(running_build);
        open_build()?.discard_unpublished(
            WalletProjectionBuildOwner::from_bytes([0x42; 16]),
            UnixTimestampMillis::new(0),
        )?;
        assert!(!wallet_path.exists());
        Ok(())
    }

    fn admission_reason<Admitted>(
        result: Result<Admitted, RocksDbWalletError>,
    ) -> Result<&'static str, Box<dyn std::error::Error>> {
        match result {
            Ok(_) => Err("tampered wallet store was admitted".into()),
            Err(RocksDbWalletError::AdmissionChanged { reason }) => Ok(reason),
            Err(error) => Err(std::io::Error::other(error.to_string()).into()),
        }
    }

    #[test]
    fn cold_semantic_rederivation_rejects_cross_family_tampering_with_matching_digest()
    -> Result<(), Box<dyn std::error::Error>> {
        for (index, tamper) in [
            SemanticTamper::OrphanAddressIndex,
            SemanticTamper::MissingAddressIndex,
            SemanticTamper::OverlappingOutputState,
            SemanticTamper::OutOfRangeBlockHeight,
            SemanticTamper::NonForwardSameBlockSpend,
            SemanticTamper::IncorrectBalance,
            SemanticTamper::MissingAddressTransaction,
            SemanticTamper::IncorrectUndo,
        ]
        .into_iter()
        .enumerate()
        {
            let temporary = TempDir::new()?;
            let network = Network::ZcashRegtest;
            let unspent = sample_output(0x22, 12_345)?;
            let spent_source = sample_output(0x33, 45_678)?;
            let spent = WalletSpentOutput::new(
                spent_source,
                WalletTransactionPosition::new(
                    TransactionId::from_bytes([0x44; 32]),
                    1,
                    source_position().tip,
                ),
                0,
            );
            let balance = WalletAddressBalance {
                address_script_hash: unspent.address_script_hash,
                balance_zat: unspent.value_zat,
            };
            let mut prepared = semantic_validation_fixture(network, &unspent, &spent, balance)?;
            let mut evidence = ready_evidence(network, &unspent, &spent, balance)?;
            apply_semantic_tamper(tamper, &mut prepared, &mut evidence)?;
            let path = temporary.path().join(format!("wallet-{index}"));
            let builder = fresh_builder(
                path,
                network,
                source_position(),
                prepared.supported_reorg_depth,
                RocksDbResourceBudget::for_local_tests(),
            )?;
            load_semantic_validation_fixture(&builder, &prepared)?;
            let (cold, _) = builder.reopen_for_validation()?;

            let semantic = validate_ready_rows_by_semantic_rederivation(
                &cold.bounded_open,
                network,
                &evidence,
                semantic_validation_config(temporary.path()),
            );
            assert_eq!(admission_reason(semantic)?, tamper.expected_reason());

            for ranges in PRIMARY_OUTPUT_SCAN_RANGES {
                let readback = validate_ready_rows_by_readback(
                    &cold.bounded_open,
                    network,
                    &evidence,
                    validation_config_with_ranges(ranges)?,
                );
                if tamper.is_visible_only_to_semantic_rederivation() {
                    readback?;
                } else {
                    assert_eq!(
                        admission_reason(readback)?,
                        tamper.expected_reason(),
                        "{tamper:?} with {ranges} ranges"
                    );
                }
            }
        }
        Ok(())
    }

    /// Builds a store with several addresses, shared transactions, and a mix of
    /// unspent and spent outputs, plus the exact READY evidence of its rows.
    fn multi_address_fixture(
        retain_tip_undo: bool,
    ) -> Result<
        (SemanticValidationFixture, WalletProjectionReadyEvidence),
        Box<dyn std::error::Error>,
    > {
        let transaction_ids = (0u8..40)
            .map(|index| TransactionId::from_bytes([index + 1; 32]))
            .collect::<Vec<_>>();
        fixture_for_transactions(retain_tip_undo, &transaction_ids)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one fixture keeps every derived family and the READY evidence visibly consistent"
    )]
    /// Builds a valid store over `transaction_ids`: every fourth transaction
    /// has two outputs, and the first output of every third is spent.
    fn fixture_for_transactions(
        retain_tip_undo: bool,
        transaction_ids: &[TransactionId],
    ) -> Result<
        (SemanticValidationFixture, WalletProjectionReadyEvidence),
        Box<dyn std::error::Error>,
    > {
        let tip = source_position().tip;
        let mut unspent_outputs = Vec::new();
        let mut spent_outputs = Vec::new();
        let mut address_transactions = Vec::new();
        let mut balance_by_address = BTreeMap::<[u8; 32], u64>::new();
        let mut commitment = TransparentUtxoSetCommitment::empty();
        let mut total_value_zat = 0u64;
        for (index, transaction_id) in transaction_ids.iter().copied().enumerate() {
            let index = u8::try_from(index)?;
            let address_byte = index % 5 + 1;
            let address = TransparentAddressScriptHash::from_bytes([address_byte; 32]);
            let created_at = WalletTransactionPosition::new(transaction_id, u32::from(index), tip);
            let output_count = if index % 4 == 0 { 2 } else { 1 };
            for output_index in 0..output_count {
                let output = WalletUnspentOutput::new(
                    TransparentOutPoint::new(transaction_id, output_index),
                    address,
                    1_000 + u64::from(index) + u64::from(output_index),
                    [0x51],
                    created_at,
                )?;
                let outpoint_key = WalletOutpointKey::new(output.outpoint);
                if index % 3 == 0 && output_index == 0 {
                    let spent = WalletSpentOutput::new(
                        output.clone(),
                        WalletTransactionPosition::new(
                            TransactionId::from_bytes([index + 100; 32]),
                            u32::from(index) + 100,
                            tip,
                        ),
                        0,
                    );
                    address_transactions
                        .extend(expected_address_transactions(&output, Some(&spent)));
                    spent_outputs.push((outpoint_key, spent));
                } else {
                    address_transactions.extend(expected_address_transactions(&output, None));
                    commitment.insert(&UtxoSetCommitmentElement {
                        network_id: Network::ZcashRegtest.id(),
                        outpoint: output.outpoint,
                        value_zat: output.value_zat,
                        script_pub_key: &output.script_pub_key,
                        block_height: output.created_at.block.height,
                    });
                    total_value_zat += output.value_zat;
                    *balance_by_address.entry([address_byte; 32]).or_default() += output.value_zat;
                    unspent_outputs.push((outpoint_key, output));
                }
            }
        }
        unspent_outputs.sort_unstable_by_key(|(key, _)| *key);
        spent_outputs.sort_unstable_by_key(|(key, _)| *key);
        address_transactions.sort_unstable_by_key(|transaction| transaction.key);
        address_transactions.dedup_by_key(|transaction| transaction.key);
        let mut unspent_output_by_address = unspent_outputs
            .iter()
            .map(|(_, output)| WalletAddressUnspentOutputKey::new(output))
            .collect::<Vec<_>>();
        unspent_output_by_address.sort_unstable_by_key(|key| *key.as_bytes());
        let address_balances = balance_by_address
            .into_iter()
            .map(|(address, balance_zat)| WalletAddressBalance {
                address_script_hash: TransparentAddressScriptHash::from_bytes(address),
                balance_zat,
            })
            .collect::<Vec<_>>();
        let mut evidence = WalletProjectionReadyEvidence {
            source_position: source_position(),
            source_sequence_digest: source_identity().source_sequence_digest(),
            settled_tip: source_position().tip,
            projection_digest: WalletProjectionDigest::from_bytes([0; 32]),
            projection_accumulator: zinder_wallet_projection::WalletProjectionAccumulator::empty(),
            row_counts: WalletProjectionFamilyRowCounts::default(),
            utxo_summary: WalletUtxoSetSummary {
                utxo_count: u64::try_from(unspent_outputs.len())?,
                total_value_zat,
                commitment,
            },
        };
        let mut reorg_undo = Vec::new();
        if retain_tip_undo {
            let mut undo = valid_reorg_undo(tip);
            undo.created_outpoints = unspent_outputs
                .iter()
                .map(|(key, _)| *key)
                .chain(spent_outputs.iter().map(|(key, _)| *key))
                .collect();
            undo.created_outpoints.sort_unstable();
            undo.address_transaction_keys = address_transactions
                .iter()
                .map(|transaction| transaction.key)
                .collect();
            reorg_undo.push(undo);
            evidence.settled_tip = settled_tip();
        }
        let mut fixture = SemanticValidationFixture {
            supported_reorg_depth: u32::from(retain_tip_undo),
            unspent_outputs,
            unspent_output_by_address,
            spent_outputs,
            address_transactions,
            address_balances,
            reorg_undo,
            block_hashes: vec![WalletBlockHash::new(tip)],
            row_counts: WalletProjectionFamilyRowCounts::default(),
            projection_digest: WalletProjectionDigest::from_bytes([0; 32]),
        };
        refresh_prepared_evidence(&mut fixture, &mut evidence)?;
        Ok((fixture, evidence))
    }

    fn cold_store_for(
        path: impl AsRef<Path>,
        fixture: &SemanticValidationFixture,
    ) -> Result<RocksDbWalletBuilder, Box<dyn std::error::Error>> {
        let builder = fresh_builder(
            path,
            Network::ZcashRegtest,
            source_position(),
            fixture.supported_reorg_depth,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        load_semantic_validation_fixture(&builder, fixture)?;
        Ok(builder)
    }

    #[test]
    fn cold_readback_and_semantic_rederivation_agree_on_valid_stores()
    -> Result<(), Box<dyn std::error::Error>> {
        let network = Network::ZcashRegtest;
        let unspent = sample_output(0x22, 12_345)?;
        let spent = WalletSpentOutput::new(
            sample_output(0x33, 45_678)?,
            WalletTransactionPosition::new(
                TransactionId::from_bytes([0x44; 32]),
                1,
                source_position().tip,
            ),
            0,
        );
        let balance = WalletAddressBalance {
            address_script_hash: unspent.address_script_hash,
            balance_zat: unspent.value_zat,
        };
        let small = (
            semantic_validation_fixture(network, &unspent, &spent, balance)?,
            ready_evidence(network, &unspent, &spent, balance)?,
        );
        let zero_value = (
            zero_value_semantic_validation_fixture(network, &sample_output(0x22, 0)?)?,
            zero_value_ready_evidence(network, &sample_output(0x22, 0)?)?,
        );
        for (index, (fixture, evidence)) in [
            small,
            zero_value,
            multi_address_fixture(false)?,
            multi_address_fixture(true)?,
        ]
        .into_iter()
        .enumerate()
        {
            let temporary = TempDir::new()?;
            let builder =
                cold_store_for(temporary.path().join(format!("wallet-{index}")), &fixture)?;
            let (cold, _) = builder.reopen_for_validation()?;

            validate_ready_rows_by_semantic_rederivation(
                &cold.bounded_open,
                network,
                &evidence,
                semantic_validation_config(temporary.path()),
            )?;
            let readback = validate_ready_rows_by_readback(
                &cold.bounded_open,
                network,
                &evidence,
                validation_config(),
            )?;
            assert_eq!(readback.random_read_count, 0);
            assert_eq!(
                readback.peak_accounted_reorg_undo_bytes > 0,
                !fixture.reorg_undo.is_empty()
            );
            let validated = cold.validate_rows(evidence.clone(), validation_config())?;
            if fixture.reorg_undo.is_empty() {
                let published = validated.publish_ready_at(UnixTimestampMillis::new(1))?;
                assert_eq!(published.ready_evidence(), &evidence);
            }
        }
        Ok(())
    }

    #[derive(Clone, Copy, Debug)]
    enum StoredRowCorruption {
        DeleteFirst,
        FlipLastValueByte,
        InsertOrphanAddressIndex,
    }

    fn corrupt_stored_family(
        builder: &RocksDbWalletBuilder,
        family_name: &'static str,
        corruption: StoredRowCorruption,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let family = column_family(&builder.bounded_open, family_name)?;
        let db = &builder.bounded_open.db;
        if matches!(corruption, StoredRowCorruption::InsertOrphanAddressIndex) {
            let orphan = WalletAddressUnspentOutputKey::new(&sample_output(0x99, 1)?);
            db.put_cf(&family, orphan.as_bytes(), [])?;
            return Ok(());
        }
        let (key, value) = db
            .iterator_cf(&family, IteratorMode::Start)
            .next()
            .ok_or("corrupted family has no rows")??;
        match corruption {
            StoredRowCorruption::DeleteFirst => db.delete_cf(&family, &key)?,
            StoredRowCorruption::FlipLastValueByte => {
                let mut corrupted = value.to_vec();
                *corrupted.last_mut().ok_or("corrupted row has no value")? ^= 1;
                db.put_cf(&family, &key, corrupted)?;
            }
            StoredRowCorruption::InsertOrphanAddressIndex => {}
        }
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the corruption table pairs each family mutation with its exact rejection reason"
    )]
    fn cold_readback_rejects_rows_corrupted_after_construction()
    -> Result<(), Box<dyn std::error::Error>> {
        const INDEX_COVERAGE: &str =
            "address unspent index does not exactly cover every primary unspent output";
        const BALANCE_SUMS: &str = "address balance rows differ from indexed unspent-output sums";
        const BALANCE_COVERAGE: &str =
            "address balance rows do not exactly cover positive indexed addresses";
        const TRANSACTION_COVERAGE: &str =
            "address transaction rows do not exactly cover output create/spend effects";
        const DIGEST_MISMATCH: &str = "READY evidence differs from cold wallet rows";

        let network = Network::ZcashRegtest;
        let cases: [(&'static str, StoredRowCorruption, Option<&'static str>); 13] = [
            (
                TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
                StoredRowCorruption::DeleteFirst,
                Some("unspent row count differs from READY evidence"),
            ),
            (
                TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
                StoredRowCorruption::FlipLastValueByte,
                None,
            ),
            (
                TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
                StoredRowCorruption::DeleteFirst,
                Some("spent row count differs from READY evidence"),
            ),
            (
                TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
                StoredRowCorruption::FlipLastValueByte,
                None,
            ),
            (
                TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
                StoredRowCorruption::DeleteFirst,
                Some(INDEX_COVERAGE),
            ),
            (
                TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
                StoredRowCorruption::InsertOrphanAddressIndex,
                Some(INDEX_COVERAGE),
            ),
            (
                TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
                StoredRowCorruption::DeleteFirst,
                Some(TRANSACTION_COVERAGE),
            ),
            (
                TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
                StoredRowCorruption::FlipLastValueByte,
                Some(DIGEST_MISMATCH),
            ),
            (
                TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
                StoredRowCorruption::DeleteFirst,
                Some(BALANCE_COVERAGE),
            ),
            (
                TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
                StoredRowCorruption::FlipLastValueByte,
                Some(BALANCE_SUMS),
            ),
            (
                REORG_UNDO_COLUMN_FAMILY,
                StoredRowCorruption::FlipLastValueByte,
                None,
            ),
            (
                BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY,
                StoredRowCorruption::DeleteFirst,
                Some("block hash row count differs from READY evidence"),
            ),
            (
                BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY,
                StoredRowCorruption::FlipLastValueByte,
                Some("block hash row count or tip differs from READY evidence"),
            ),
        ];
        for (index, (family_name, corruption, expected_reason)) in cases.into_iter().enumerate() {
            let temporary = TempDir::new()?;
            let (fixture, evidence) = multi_address_fixture(true)?;
            let builder =
                cold_store_for(temporary.path().join(format!("wallet-{index}")), &fixture)?;
            corrupt_stored_family(&builder, family_name, corruption)?;
            let (cold, _) = builder.reopen_for_validation()?;

            let semantic = validate_ready_rows_by_semantic_rederivation(
                &cold.bounded_open,
                network,
                &evidence,
                semantic_validation_config(temporary.path()),
            );
            assert!(
                semantic.is_err(),
                "semantic re-derivation admitted {family_name} {corruption:?}"
            );
            for ranges in PRIMARY_OUTPUT_SCAN_RANGES {
                let readback = validate_ready_rows_by_readback(
                    &cold.bounded_open,
                    network,
                    &evidence,
                    validation_config_with_ranges(ranges)?,
                );
                match expected_reason {
                    Some(reason) => assert_eq!(
                        admission_reason(readback)?,
                        reason,
                        "{family_name} {corruption:?} with {ranges} ranges"
                    ),
                    None => assert!(
                        readback.is_err(),
                        "digest readback admitted {family_name} {corruption:?} with {ranges} ranges"
                    ),
                }
            }
        }
        Ok(())
    }

    #[test]
    fn a_refused_validation_thread_fails_the_build_and_releases_its_lease()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let wallet_path = temporary.path().join("wallet");
        let (fixture, evidence) = multi_address_fixture(true)?;
        let builder = cold_store_for(&wallet_path, &fixture)?;
        let lease = builder.lease();
        let (cold, _) = builder.reopen_for_validation()?;

        REFUSED_VALIDATION_THREAD.set(Some(0));
        let refused = cold.validate_rows(evidence, validation_config());
        REFUSED_VALIDATION_THREAD.set(None);
        let Err(build_error) = refused else {
            return Err("a refused validation thread must fail cold validation".into());
        };
        assert!(matches!(
            build_error,
            RocksDbWalletError::ColdValidationThreadUnavailable { .. }
        ));

        let other_owner = WalletProjectionBuildOwner::from_bytes([0x66; 16]);
        let now = UnixTimestampMillis::new(1);
        assert!(
            RocksDbWalletBuildStore::open(&wallet_path, Network::ZcashRegtest, test_budget())?
                .discard_unpublished(other_owner, now)
                .is_err(),
            "the failed build's live lease must block another owner until it is released"
        );
        let mut cleanup = crate::build::FailedWalletBuildLeaseCleanup::new(
            &wallet_path,
            Network::ZcashRegtest,
            test_budget(),
            now,
        );
        cleanup.arm(lease, now);
        assert!(matches!(
            crate::build::finish_failed_wallet_build::<()>(&mut cleanup, build_error),
            Err(RocksDbWalletError::ColdValidationThreadUnavailable { .. })
        ));
        RocksDbWalletBuildStore::open(&wallet_path, Network::ZcashRegtest, test_budget())?
            .discard_unpublished(other_owner, now)?;
        assert!(!wallet_path.exists());
        Ok(())
    }

    #[test]
    fn a_refused_range_thread_fails_validation_with_a_typed_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let (fixture, evidence) = multi_address_fixture(true)?;
        let builder = cold_store_for(temporary.path().join("wallet"), &fixture)?;
        let (cold, _) = builder.reopen_for_validation()?;
        // The two address-family threads start first; the fifth start is the
        // third primary range thread.
        for refused_start in [2, 4] {
            REFUSED_VALIDATION_THREAD.set(Some(refused_start));
            let refused = validate_ready_rows_by_readback(
                &cold.bounded_open,
                Network::ZcashRegtest,
                &evidence,
                validation_config_with_ranges(8)?,
            );
            REFUSED_VALIDATION_THREAD.set(None);
            assert!(
                matches!(
                    refused,
                    Err(RocksDbWalletError::ColdValidationThreadUnavailable { .. })
                ),
                "refused start {refused_start}"
            );
        }
        validate_ready_rows_by_readback(
            &cold.bounded_open,
            Network::ZcashRegtest,
            &evidence,
            validation_config_with_ranges(8)?,
        )?;
        Ok(())
    }

    /// Transaction ids whose outpoint keys sit at the ends of the key space
    /// and just below, at, and above the two-range split and the first
    /// eight-range split.
    fn split_boundary_transactions() -> Vec<TransactionId> {
        let mut ids = Vec::new();
        for (prefix, fill) in [
            ([0x00, 0x00], 0x00),
            ([0x1f, 0xff], 0xff),
            ([0x20, 0x00], 0x00),
            ([0x20, 0x00], 0x01),
            ([0x7f, 0xff], 0xff),
            ([0x7f, 0xff], 0xfe),
            ([0x80, 0x00], 0x01),
            ([0x80, 0x00], 0x00),
            ([0x80, 0x01], 0x00),
            ([0xff, 0xff], 0xff),
            ([0xff, 0xff], 0xfe),
            ([0x40, 0x00], 0x00),
        ] {
            let mut id = [fill; 32];
            id[..2].copy_from_slice(&prefix);
            ids.push(TransactionId::from_bytes(id));
        }
        ids
    }

    #[test]
    fn readback_counts_every_key_at_and_around_range_splits()
    -> Result<(), Box<dyn std::error::Error>> {
        let network = Network::ZcashRegtest;
        let (fixture, evidence) = fixture_for_transactions(true, &split_boundary_transactions())?;
        assert!(fixture.spent_outputs.len() >= 3);
        assert!(fixture.unspent_outputs.len() >= 8);
        let temporary = TempDir::new()?;
        let builder = cold_store_for(temporary.path().join("wallet"), &fixture)?;
        let (cold, _) = builder.reopen_for_validation()?;
        let single = validate_ready_rows_by_readback(
            &cold.bounded_open,
            network,
            &evidence,
            validation_config_with_ranges(1)?,
        )?;
        for ranges in [2, 3, 8, 64] {
            let partitioned = validate_ready_rows_by_readback(
                &cold.bounded_open,
                network,
                &evidence,
                validation_config_with_ranges(ranges)?,
            )?;
            assert_eq!(
                partitioned.peak_accounted_reorg_undo_bytes, single.peak_accounted_reorg_undo_bytes,
                "{ranges} ranges"
            );
        }
        Ok(())
    }

    #[test]
    fn an_overlap_at_the_range_split_is_rejected_with_matching_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let network = Network::ZcashRegtest;
        let (mut fixture, mut evidence) =
            fixture_for_transactions(false, &split_boundary_transactions())?;
        // The unspent output keyed exactly at the two-range split prefix.
        let split_key = fixture
            .unspent_outputs
            .iter()
            .position(|(key, _)| key.as_bytes().starts_with(&[0x80, 0x00, 0x00]))
            .ok_or("the fixture needs an unspent output at the split")?;
        let output = fixture.unspent_outputs[split_key].1.clone();
        let spent = WalletSpentOutput::new(
            output.clone(),
            WalletTransactionPosition::new(
                TransactionId::from_bytes([0x55; 32]),
                u32::MAX,
                output.created_at.block,
            ),
            0,
        );
        fixture
            .spent_outputs
            .push((WalletOutpointKey::new(output.outpoint), spent));
        fixture.spent_outputs.sort_unstable_by_key(|(key, _)| *key);
        refresh_prepared_evidence(&mut fixture, &mut evidence)?;
        let temporary = TempDir::new()?;
        let builder = cold_store_for(temporary.path().join("wallet"), &fixture)?;
        let (cold, _) = builder.reopen_for_validation()?;
        for ranges in [1, 2, 8] {
            let readback = validate_ready_rows_by_readback(
                &cold.bounded_open,
                network,
                &evidence,
                validation_config_with_ranges(ranges)?,
            );
            assert_eq!(
                admission_reason(readback)?,
                "one outpoint appears in both unspent and spent output families",
                "{ranges} ranges"
            );
        }
        Ok(())
    }

    #[test]
    fn a_commitment_only_change_to_ready_evidence_is_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let network = Network::ZcashRegtest;
        let (fixture, mut evidence) =
            fixture_for_transactions(false, &split_boundary_transactions())?;
        let mut altered = evidence.utxo_summary.commitment.accumulator().to_vec();
        altered[2047] ^= 1;
        evidence.utxo_summary.commitment = TransparentUtxoSetCommitment::from_parts(
            evidence.utxo_summary.commitment.scheme(),
            &altered,
        )
        .ok_or("the altered commitment keeps its length")?;
        let temporary = TempDir::new()?;
        let builder = cold_store_for(temporary.path().join("wallet"), &fixture)?;
        let (cold, _) = builder.reopen_for_validation()?;
        for ranges in [1, 2, 8] {
            let readback = validate_ready_rows_by_readback(
                &cold.bounded_open,
                network,
                &evidence,
                validation_config_with_ranges(ranges)?,
            );
            assert_eq!(
                admission_reason(readback)?,
                "READY evidence differs from cold wallet rows",
                "{ranges} ranges"
            );
        }
        Ok(())
    }

    #[test]
    fn partitioned_readback_refuses_an_undo_suffix_over_its_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let (fixture, evidence) = multi_address_fixture(true)?;
        let temporary = TempDir::new()?;
        let builder = cold_store_for(temporary.path().join("wallet"), &fixture)?;
        let (cold, _) = builder.reopen_for_validation()?;
        // Room for the suffix entry and one key, while the ranges observe many
        // distinct created outpoints and address-history keys.
        let limit = u64::try_from(
            size_of::<(u32, ExpectedReorgUndoEffects)>() + size_of::<WalletOutpointKey>(),
        )?;
        for ranges in [1, 2, 8] {
            let readback = validate_ready_rows_by_readback(
                &cold.bounded_open,
                Network::ZcashRegtest,
                &evidence,
                WalletColdValidationConfig {
                    max_accounted_reorg_undo_bytes: limit,
                    ..validation_config_with_ranges(ranges)?
                },
            );
            assert!(
                matches!(
                    readback,
                    Err(RocksDbWalletError::AccountedReorgUndoMemoryLimit { limit_bytes, .. })
                        if limit_bytes == limit
                ),
                "{ranges} ranges"
            );
        }
        Ok(())
    }

    #[test]
    fn scan_ranges_are_capped_by_the_maximum_and_the_row_count() {
        let primary_ranges = |requested: u32, primary_rows: u64, min_rows: u64| {
            let config = WalletColdValidationConfig {
                max_accounted_reorg_undo_bytes: TEST_VALIDATION_REORG_UNDO_BYTES,
                family_scan_ranges: NonZeroU32::new(requested).unwrap_or(NonZeroU32::MIN),
                min_rows_per_scan_range: NonZeroU64::new(min_rows).unwrap_or(NonZeroU64::MIN),
            };
            let counts = WalletProjectionFamilyRowCounts {
                transparent_unspent_output_count: primary_rows / 2,
                transparent_spent_output_count: primary_rows - primary_rows / 2,
                ..WalletProjectionFamilyRowCounts::default()
            };
            primary_output_scan_range_count(&config, &counts).get()
        };
        let address_tx_ranges = |requested: u32, address_tx_rows: u64, min_rows: u64| {
            let config = WalletColdValidationConfig {
                max_accounted_reorg_undo_bytes: TEST_VALIDATION_REORG_UNDO_BYTES,
                family_scan_ranges: NonZeroU32::new(requested).unwrap_or(NonZeroU32::MIN),
                min_rows_per_scan_range: NonZeroU64::new(min_rows).unwrap_or(NonZeroU64::MIN),
            };
            let counts = WalletProjectionFamilyRowCounts {
                transparent_address_transaction_count: address_tx_rows,
                ..WalletProjectionFamilyRowCounts::default()
            };
            address_transaction_scan_range_count(&config, &counts).get()
        };
        let production = MIN_ROWS_PER_SCAN_RANGE.get();
        assert_eq!(primary_ranges(4, 17_086_503, production), 4);
        assert_eq!(primary_ranges(64, 10, production), 1);
        assert_eq!(primary_ranges(64, 0, production), 1);
        assert_eq!(primary_ranges(64, 3 * production, production), 3);
        assert_eq!(primary_ranges(64, u64::MAX, production), 64);
        assert_eq!(
            primary_ranges(u32::MAX, u64::MAX, 1),
            crate::MAX_ACCUMULATOR_HASHING_WORKERS
        );
        assert_eq!(primary_ranges(8, 3, 1), 3);
        assert_eq!(address_tx_ranges(4, 290_117_445, production), 4);
        assert_eq!(address_tx_ranges(64, 10, production), 1);
        assert_eq!(address_tx_ranges(64, 0, production), 1);
        assert_eq!(address_tx_ranges(64, 3 * production, production), 3);
        assert_eq!(
            address_tx_ranges(u32::MAX, u64::MAX, 1),
            crate::MAX_ACCUMULATOR_HASHING_WORKERS
        );
        assert_eq!(address_tx_ranges(8, 3, 1), 3);
    }

    fn test_budget() -> RocksDbResourceBudget {
        RocksDbResourceBudget::for_local_tests()
    }

    #[test]
    fn hash_prefix_key_ranges_cover_the_key_space_once() -> Result<(), Box<dyn std::error::Error>> {
        for count in [1, 2, 3, 7, 64, 65_536, 70_000] {
            let ranges = hash_prefix_key_ranges(NonZeroU32::new(count).ok_or("zero ranges")?);
            assert_eq!(ranges.len(), usize::try_from(count.min(65_536))?);
            assert_eq!(ranges.first().and_then(|range| range.lower), None);
            assert_eq!(ranges.last().and_then(|range| range.upper), None);
            for pair in ranges.windows(2) {
                let [current, next] = pair else {
                    return Err("windows of two".into());
                };
                assert!(current.upper.is_some(), "{count} ranges");
                assert_eq!(current.upper, next.lower, "{count} ranges");
                if let (Some(lower), Some(upper)) = (current.lower, current.upper) {
                    assert!(lower < upper, "{count} ranges");
                }
            }
        }
        assert_eq!(
            hash_prefix_key_ranges(NonZeroU32::MIN.saturating_add(1)),
            vec![
                HashPrefixKeyRange {
                    lower: None,
                    upper: Some([0x80, 0x00]),
                },
                HashPrefixKeyRange {
                    lower: Some([0x80, 0x00]),
                    upper: None,
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn a_refused_address_transaction_range_thread_fails_validation_with_a_typed_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let (fixture, evidence) = multi_address_fixture(true)?;
        let builder = cold_store_for(temporary.path().join("wallet"), &fixture)?;
        let (cold, _) = builder.reopen_for_validation()?;
        let expected_count = evidence.row_counts.transparent_address_transaction_count;
        let retained_heights =
            BlockHeightRange::inclusive(source_position().tip.height, source_position().tip.height);
        let eight = NonZeroU32::new(8).ok_or("zero scan ranges")?;
        for refused_start in [0, 2, 6] {
            REFUSED_VALIDATION_THREAD.set(Some(refused_start));
            let refused = read_back_address_transaction_rows(
                &cold.bounded_open,
                expected_count,
                retained_heights,
                eight,
            );
            REFUSED_VALIDATION_THREAD.set(None);
            assert!(
                matches!(
                    refused,
                    Err(RocksDbWalletError::ColdValidationThreadUnavailable { .. })
                ),
                "refused start {refused_start}"
            );
        }
        let digest = read_back_address_transaction_rows(
            &cold.bounded_open,
            expected_count,
            retained_heights,
            eight,
        )?;
        assert_eq!(
            digest.row_counts().transparent_address_transaction_count,
            expected_count
        );
        Ok(())
    }

    /// Address-transaction rows placed at the ends of the address-hash key
    /// space and just below, at, and above the two-range and eight-range splits.
    fn split_boundary_address_transactions() -> Vec<WalletAddressTransaction> {
        let tip = source_position().tip;
        [
            ([0x00, 0x00], 0x00),
            ([0x1f, 0xff], 0xff),
            ([0x20, 0x00], 0x00),
            ([0x20, 0x00], 0x01),
            ([0x7f, 0xff], 0xff),
            ([0x7f, 0xff], 0xfe),
            ([0x80, 0x00], 0x00),
            ([0x80, 0x00], 0x01),
            ([0x80, 0x01], 0x00),
            ([0xff, 0xff], 0xfe),
            ([0xff, 0xff], 0xff),
            ([0x40, 0x00], 0x00),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (prefix, fill))| {
            let mut hash = [fill; 32];
            hash[..2].copy_from_slice(&prefix);
            let tx_index = u32::try_from(index).unwrap_or(u32::MAX);
            WalletAddressTransaction {
                key: WalletAddressTransactionKey::new(
                    TransparentAddressScriptHash::from_bytes(hash),
                    tip.height,
                    tx_index,
                ),
                transaction_id: TransactionId::from_bytes(hash),
                block_hash: tip.hash,
            }
        })
        .collect()
    }

    #[test]
    fn readback_counts_every_address_transaction_at_and_around_range_splits()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut fixture, mut evidence) = multi_address_fixture(false)?;
        fixture.address_transactions = split_boundary_address_transactions();
        fixture
            .address_transactions
            .sort_unstable_by_key(|row| *row.key.as_bytes());
        refresh_prepared_evidence(&mut fixture, &mut evidence)?;
        let temporary = TempDir::new()?;
        let builder = cold_store_for(temporary.path().join("wallet"), &fixture)?;
        let (cold, _) = builder.reopen_for_validation()?;
        let expected_count = evidence.row_counts.transparent_address_transaction_count;
        let retained_heights =
            BlockHeightRange::inclusive(source_position().tip.height, source_position().tip.height);
        let single = read_back_address_transaction_rows(
            &cold.bounded_open,
            expected_count,
            retained_heights,
            NonZeroU32::MIN,
        )?;
        for ranges in [2, 3, 8, 64] {
            let partitioned = read_back_address_transaction_rows(
                &cold.bounded_open,
                expected_count,
                retained_heights,
                NonZeroU32::new(ranges).ok_or("zero ranges")?,
            )?;
            assert_eq!(partitioned.row_counts(), single.row_counts(), "{ranges}");
            assert_eq!(partitioned.accumulator(), single.accumulator(), "{ranges}");
            validate_ready_rows_by_readback(
                &cold.bounded_open,
                Network::ZcashRegtest,
                &evidence,
                validation_config_with_ranges(ranges)?,
            )?;
        }
        // Row-count mismatch must be caught for every range count.
        for ranges in [1, 2, 8] {
            let wrong_count = read_back_address_transaction_rows(
                &cold.bounded_open,
                expected_count.saturating_add(1),
                retained_heights,
                NonZeroU32::new(ranges).ok_or("zero ranges")?,
            );
            assert_eq!(
                admission_reason(wrong_count)?,
                "address transaction rows do not exactly cover output create/spend effects",
                "{ranges} ranges"
            );
        }
        Ok(())
    }

    #[test]
    fn a_corrupt_address_transaction_at_the_range_split_is_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut fixture, mut evidence) = multi_address_fixture(false)?;
        fixture.address_transactions = split_boundary_address_transactions();
        fixture
            .address_transactions
            .sort_unstable_by_key(|row| *row.key.as_bytes());
        refresh_prepared_evidence(&mut fixture, &mut evidence)?;
        let split_key = fixture
            .address_transactions
            .iter()
            .find(|row| row.key.as_bytes().starts_with(&[0x80, 0x00, 0x00]))
            .ok_or("the fixture needs an address transaction at the split")?
            .key;
        let temporary = TempDir::new()?;
        let builder = cold_store_for(temporary.path().join("wallet"), &fixture)?;
        {
            let family = column_family(
                &builder.bounded_open,
                TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
            )?;
            builder
                .bounded_open
                .db
                .put_cf(&family, split_key.as_bytes(), [0xFF_u8; 3])
                .map_err(|source| RocksDbWalletError::rocksdb("corrupt split row", source))?;
        }
        let (cold, _) = builder.reopen_for_validation()?;
        for ranges in [1, 2, 8] {
            let readback = validate_ready_rows_by_readback(
                &cold.bounded_open,
                Network::ZcashRegtest,
                &evidence,
                validation_config_with_ranges(ranges)?,
            );
            assert!(
                matches!(readback, Err(RocksDbWalletError::Contract(_))),
                "{ranges} ranges: {readback:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn partitioned_readback_matches_one_range_on_valid_stores()
    -> Result<(), Box<dyn std::error::Error>> {
        let network = Network::ZcashRegtest;
        for retain_tip_undo in [false, true] {
            let temporary = TempDir::new()?;
            let (fixture, evidence) = multi_address_fixture(retain_tip_undo)?;
            let builder = cold_store_for(temporary.path().join("wallet"), &fixture)?;
            let (cold, _) = builder.reopen_for_validation()?;
            let single = validate_ready_rows_by_readback(
                &cold.bounded_open,
                network,
                &evidence,
                validation_config_with_ranges(1)?,
            )?;
            assert_eq!(single.peak_accounted_reorg_undo_bytes > 0, retain_tip_undo);
            for ranges in [2, 3, 8, 64, 256] {
                let partitioned = validate_ready_rows_by_readback(
                    &cold.bounded_open,
                    network,
                    &evidence,
                    validation_config_with_ranges(ranges)?,
                )?;
                assert_eq!(
                    partitioned.peak_accounted_reorg_undo_bytes,
                    single.peak_accounted_reorg_undo_bytes,
                    "{ranges} ranges"
                );
                assert_eq!(partitioned.random_read_count, 0);
            }
        }
        Ok(())
    }

    #[test]
    fn shared_undo_suffix_admits_each_key_once_across_scans()
    -> Result<(), Box<dyn std::error::Error>> {
        let admitted_bytes = size_of::<(u32, ExpectedReorgUndoEffects)>()
            .checked_add(size_of::<WalletOutpointKey>())
            .ok_or("test reorg undo memory overflow")?;
        let memory_limit = u64::try_from(admitted_bytes)?;
        let shared = SharedExpectedReorgUndoSuffix::new(ExpectedReorgUndoSuffix::new(
            settled_tip(),
            1,
            memory_limit,
        )?);
        let output = sample_output(0x22, 12_345)?;
        let unsettled_hashes =
            BTreeMap::from([(source_position().tip.height, source_position().tip.hash)]);
        std::thread::scope(|scope| {
            let observers = (0..4)
                .map(|_| {
                    scope.spawn(|| {
                        shared.observe_created(
                            output.outpoint,
                            output.created_at.block.height,
                            &unsettled_hashes,
                        )
                    })
                })
                .collect::<Vec<_>>();
            observers
                .into_iter()
                .map(|observer| {
                    observer
                        .join()
                        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
                })
                .collect::<Result<Vec<_>, _>>()
        })?;
        let outside = sample_output(0x23, 1)?;
        let mut outside = outside;
        outside.created_at =
            WalletTransactionPosition::new(outside.created_at.transaction_id, 0, settled_tip());
        shared.observe_created(
            outside.outpoint,
            outside.created_at.block.height,
            &unsettled_hashes,
        )?;

        let suffix = shared.into_inner();
        let undo = suffix
            .undo_by_height
            .get(&source_position().tip.height.value())
            .ok_or("test undo suffix disappeared")?;
        assert_eq!(undo.created_outpoints.len(), 1);
        assert_eq!(suffix.memory.current, memory_limit);
        assert_eq!(suffix.memory.peak, memory_limit);
        Ok(())
    }

    #[test]
    fn reorg_undo_memory_admission_precedes_set_insertion() -> Result<(), Box<dyn std::error::Error>>
    {
        let admitted_bytes = size_of::<(u32, ExpectedReorgUndoEffects)>();
        let memory_limit = u64::try_from(admitted_bytes)?;
        let mut suffix = ExpectedReorgUndoSuffix::new(settled_tip(), 1, memory_limit)?;
        let output = sample_output(0x22, 12_345)?;

        assert!(matches!(
            suffix.observe_created(output.outpoint, output.created_at.block),
            Err(RocksDbWalletError::AccountedReorgUndoMemoryLimit { .. })
        ));
        let undo = suffix
            .undo_by_height
            .get(&source_position().tip.height.value())
            .ok_or("test undo suffix disappeared")?;
        assert!(undo.created_outpoints.is_empty());
        Ok(())
    }

    #[test]
    fn duplicate_reorg_undo_keys_need_no_additional_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let admitted_bytes = size_of::<(u32, ExpectedReorgUndoEffects)>()
            .checked_add(size_of::<WalletOutpointKey>())
            .ok_or("test reorg undo memory overflow")?;
        let memory_limit = u64::try_from(admitted_bytes)?;
        let mut suffix = ExpectedReorgUndoSuffix::new(settled_tip(), 1, memory_limit)?;
        let output = sample_output(0x22, 12_345)?;

        suffix.observe_created(output.outpoint, output.created_at.block)?;
        suffix.observe_created(output.outpoint, output.created_at.block)?;

        let undo = suffix
            .undo_by_height
            .get(&source_position().tip.height.value())
            .ok_or("test undo suffix disappeared")?;
        assert_eq!(undo.created_outpoints.len(), 1);
        assert_eq!(suffix.memory.current, memory_limit);
        assert_eq!(suffix.memory.peak, memory_limit);
        Ok(())
    }

    #[test]
    fn cold_validation_refuses_before_crossing_its_accounted_memory_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let network = Network::ZcashRegtest;
        let unspent = sample_output(0x22, 12_345)?;
        let spent_source = sample_output(0x33, 45_678)?;
        let spent = WalletSpentOutput::new(
            spent_source,
            WalletTransactionPosition::new(
                TransactionId::from_bytes([0x44; 32]),
                1,
                source_position().tip,
            ),
            0,
        );
        let balance = WalletAddressBalance {
            address_script_hash: unspent.address_script_hash,
            balance_zat: unspent.value_zat,
        };
        let mut prepared = semantic_validation_fixture(network, &unspent, &spent, balance)?;
        let mut evidence = ready_evidence(network, &unspent, &spent, balance)?;
        prepared.supported_reorg_depth = 1;
        prepared.reorg_undo = vec![valid_reorg_undo(source_position().tip)];
        evidence.settled_tip = settled_tip();
        refresh_prepared_evidence(&mut prepared, &mut evidence)?;
        let builder = fresh_builder(
            temporary.path().join("wallet"),
            network,
            source_position(),
            1,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        load_semantic_validation_fixture(&builder, &prepared)?;

        let mut config = validation_config();
        config.max_accounted_reorg_undo_bytes = 0;
        assert!(matches!(
            builder
                .reopen_for_validation()?
                .0
                .validate_rows(evidence, config),
            Err(RocksDbWalletError::AccountedReorgUndoMemoryLimit { .. })
        ));
        Ok(())
    }

    #[test]
    fn ready_store_serves_exact_version_two_rows() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let path = temporary.path().join("wallet");
        let network = Network::ZcashRegtest;
        let unspent = sample_output(0x22, 12_345)?;
        let spent_source = sample_output(0x33, 45_678)?;
        let spent = WalletSpentOutput::new(
            spent_source,
            WalletTransactionPosition::new(
                TransactionId::from_bytes([0x44; 32]),
                1,
                source_position().tip,
            ),
            0,
        );
        let balance = WalletAddressBalance {
            address_script_hash: unspent.address_script_hash,
            balance_zat: unspent.value_zat,
        };
        let builder = fresh_builder(
            &path,
            network,
            source_position(),
            0,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let prepared = semantic_validation_fixture(network, &unspent, &spent, balance)?;
        load_semantic_validation_fixture(&builder, &prepared)?;
        let evidence = ready_evidence(network, &unspent, &spent, balance)?;
        let store = builder
            .reopen_for_validation()?
            .0
            .validate_rows(evidence.clone(), validation_config())?
            .publish_ready_at(UnixTimestampMillis::new(1))?;

        assert_eq!(store.ready_evidence(), &evidence);
        assert_eq!(
            store.find_unspent_output(unspent.outpoint)?,
            Some(unspent.clone())
        );
        assert_eq!(store.find_spent_output(spent.output.outpoint)?, Some(spent));
        assert_eq!(
            store.address_balance(unspent.address_script_hash)?,
            unspent.value_zat
        );
        assert_eq!(store.utxo_summary(), &evidence.utxo_summary);
        drop(store);

        let reopened = RocksDbWalletStore::open_ready(
            &path,
            network,
            source_identity(),
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(reopened.ready_evidence(), &evidence);
        Ok(())
    }

    #[test]
    fn recovery_cold_admission_requires_ready_rows_to_match_persisted_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let checkpoint_path = temporary.path().join("wallet-checkpoint");
        let network = Network::ZcashRegtest;
        let unspent = sample_output(0x22, 12_345)?;
        let spent_source = sample_output(0x33, 45_678)?;
        let spent = WalletSpentOutput::new(
            spent_source,
            WalletTransactionPosition::new(
                TransactionId::from_bytes([0x44; 32]),
                1,
                source_position().tip,
            ),
            0,
        );
        let balance = WalletAddressBalance {
            address_script_hash: unspent.address_script_hash,
            balance_zat: unspent.value_zat,
        };
        let source_path = temporary.path().join("wallet-source");
        let builder = fresh_builder(
            &source_path,
            network,
            source_position(),
            0,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let prepared = semantic_validation_fixture(network, &unspent, &spent, balance)?;
        load_semantic_validation_fixture(&builder, &prepared)?;
        let ready = ready_evidence(network, &unspent, &spent, balance)?;
        drop(
            builder
                .reopen_for_validation()?
                .0
                .validate_rows(ready, validation_config())?
                .publish_ready_at(UnixTimestampMillis::new(1))?,
        );
        let mut owner = RocksDbWalletStore::open_ready_for_following(
            &source_path,
            network,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let checkpoint = owner
            .create_owner_checkpoint(&checkpoint_path, RocksDbResourceBudget::for_local_tests())?;
        drop(owner);

        let recovery_config = WalletRecoveryAdmissionConfig {
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            staging_path: temporary.path(),
            max_sort_memory_bytes_per_sorter: TEST_VALIDATION_SORT_MEMORY_BYTES,
            max_temporary_file_bytes_per_sorter: TEST_VALIDATION_TEMPORARY_FILE_BYTES,
            max_accounted_reorg_undo_bytes: TEST_VALIDATION_REORG_UNDO_BYTES,
        };
        let admitted = RocksDbWalletFollowingStore::cold_admit_recovery_checkpoint(
            &checkpoint_path,
            network,
            recovery_config,
        )?;
        assert_eq!(admitted, checkpoint);

        delete_recovery_address_index_row(
            &checkpoint_path,
            prepared.unspent_output_by_address[0].as_bytes(),
        )?;

        assert!(matches!(
            RocksDbWalletFollowingStore::cold_admit_recovery_checkpoint(
                &checkpoint_path,
                network,
                recovery_config,
            ),
            Err(RocksDbWalletError::AdmissionChanged {
                reason: "address unspent index does not exactly cover every primary unspent output"
            })
        ));
        Ok(())
    }

    fn delete_recovery_address_index_row(
        checkpoint_path: &Path,
        address_index_key: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::ExistingPrimary {
                path: checkpoint_path,
            },
            RocksDbResourceBudget::for_local_tests(),
            wallet_column_family_descriptors,
        )?;
        let address_index = column_family(
            &bounded_open,
            TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
        )?;
        bounded_open
            .db
            .delete_cf(&address_index, address_index_key)?;
        drop(address_index);
        Ok(())
    }

    #[test]
    fn ready_store_refuses_stale_source_position_or_digest()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let path = temporary.path().join("wallet");
        let network = Network::ZcashRegtest;
        let unspent = sample_output(0x22, 12_345)?;
        let spent_source = sample_output(0x33, 45_678)?;
        let spent = WalletSpentOutput::new(
            spent_source,
            WalletTransactionPosition::new(
                TransactionId::from_bytes([0x44; 32]),
                1,
                source_position().tip,
            ),
            0,
        );
        let balance = WalletAddressBalance {
            address_script_hash: unspent.address_script_hash,
            balance_zat: unspent.value_zat,
        };
        let builder = fresh_builder(
            &path,
            network,
            source_position(),
            0,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let prepared = semantic_validation_fixture(network, &unspent, &spent, balance)?;
        load_semantic_validation_fixture(&builder, &prepared)?;
        let evidence = ready_evidence(network, &unspent, &spent, balance)?;
        drop(
            builder
                .reopen_for_validation()?
                .0
                .validate_rows(evidence.clone(), validation_config())?
                .publish_ready_at(UnixTimestampMillis::new(1))?,
        );
        let stale_position = WalletCanonicalSourceIdentity::new(
            WalletProjectionSourcePosition::new(
                ChainEpochId::new(2),
                BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0x55; 32])),
                2,
            ),
            evidence.source_sequence_digest,
            evidence.settled_tip,
        );
        assert!(matches!(
            RocksDbWalletStore::open_ready(
                &path,
                network,
                stale_position,
                RocksDbResourceBudget::for_local_tests(),
            ),
            Err(RocksDbWalletError::CanonicalSourceMismatch { .. })
        ));
        let stale_digest = WalletCanonicalSourceIdentity::new(
            evidence.source_position,
            CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                CanonicalBlockFactsSequenceDigestVersion::V1,
                1,
                [0x88; 32],
            ),
            evidence.settled_tip,
        );
        assert!(matches!(
            RocksDbWalletStore::open_ready(
                &path,
                network,
                stale_digest,
                RocksDbResourceBudget::for_local_tests(),
            ),
            Err(RocksDbWalletError::CanonicalSourceMismatch { .. })
        ));
        Ok(())
    }

    #[test]
    fn ready_store_retains_zero_value_utxo_without_balance_row()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let network = Network::ZcashRegtest;
        let unspent = sample_output(0x66, 0)?;
        let builder = fresh_builder(
            temporary.path().join("wallet"),
            network,
            source_position(),
            0,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let prepared = zero_value_semantic_validation_fixture(network, &unspent)?;
        load_semantic_validation_fixture(&builder, &prepared)?;
        let evidence = zero_value_ready_evidence(network, &unspent)?;
        let store = builder
            .reopen_for_validation()?
            .0
            .validate_rows(evidence.clone(), validation_config())?
            .publish_ready_at(UnixTimestampMillis::new(1))?;

        assert_eq!(store.find_unspent_output(unspent.outpoint)?, Some(unspent));
        assert_eq!(
            store.address_balance(prepared.unspent_outputs[0].1.address_script_hash)?,
            0
        );
        assert_eq!(evidence.row_counts.transparent_address_balance_count, 0);
        assert_eq!(store.utxo_summary().utxo_count, 1);
        Ok(())
    }

    #[test]
    fn flushed_hash_keyed_families_carry_partitioned_filters_and_the_rest_do_not()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Primary {
                path: temporary.path(),
            },
            RocksDbResourceBudget::for_local_tests(),
            wallet_column_family_descriptors,
        )?;
        bounded_open.db.put(b"control", b"row")?;
        for (name, _) in WALLET_DATA_COLUMN_FAMILIES {
            let family = column_family(&bounded_open, name)?;
            bounded_open.db.put_cf(&family, [0x5a; 36], b"row")?;
        }
        flush_complete_build(&bounded_open)?;

        for (name, filtered) in [
            (TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY, true),
            (TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY, true),
            (TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY, true),
            (TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY, true),
            (TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY, false),
            (REORG_UNDO_COLUMN_FAMILY, false),
            (BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY, false),
            (DEFAULT_COLUMN_FAMILY_NAME, false),
        ] {
            let property = |field| aggregated_table_property(&bounded_open, name, field);
            assert_eq!(property("# entries")?, Some(1), "{name} was not flushed");
            if filtered {
                assert!(property("filter block size")? > Some(0), "{name}");
                assert_eq!(property("# entries for filter")?, Some(1), "{name}");
                assert!(property("# index partitions")? > Some(0), "{name}");
            } else {
                assert_eq!(property("filter block size")?, Some(0), "{name}");
                assert_eq!(property("# entries for filter")?, Some(0), "{name}");
                assert_eq!(property("# index partitions")?, None, "{name}");
            }
        }
        Ok(())
    }

    #[test]
    fn absent_spent_output_reads_stop_at_the_filter_before_any_data_block()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Primary {
                path: temporary.path(),
            },
            RocksDbResourceBudget::for_local_tests(),
            wallet_column_family_descriptors,
        )?;
        let spent_family = column_family(&bounded_open, TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY)?;
        let outpoint_key = |transaction_byte| {
            WalletOutpointKey::new(TransparentOutPoint::new(
                TransactionId::from_bytes([transaction_byte; 32]),
                0,
            ))
        };
        // The absent key sorts between the stored keys, so RocksDB cannot skip
        // the file on its key range and must consult the filter.
        for transaction_byte in [0x11, 0xee] {
            bounded_open.db.put_cf(
                &spent_family,
                outpoint_key(transaction_byte).as_bytes(),
                b"spent",
            )?;
        }
        flush_complete_build(&bounded_open)?;
        let statistics = &bounded_open.statistics;
        let useful_before = statistics.get_ticker_count(Ticker::BloomFilterUseful);
        let data_misses_before = statistics.get_ticker_count(Ticker::BlockCacheDataMiss);

        let observed = bounded_open
            .db
            .get_cf(&spent_family, outpoint_key(0x80).as_bytes())?;

        assert_eq!(observed, None);
        assert!(statistics.get_ticker_count(Ticker::BloomFilterUseful) > useful_before);
        assert_eq!(
            statistics.get_ticker_count(Ticker::BlockCacheDataMiss),
            data_misses_before
        );
        Ok(())
    }

    /// Reads one numeric field of the family's `rocksdb.aggregated-table-properties`.
    ///
    /// The aggregate sums the family's live SST files and never carries a
    /// filter policy name, and `# index partitions` appears only when nonzero.
    fn aggregated_table_property(
        bounded_open: &BoundedRocksDbOpen,
        family_name: &'static str,
        field: &str,
    ) -> Result<Option<u64>, Box<dyn std::error::Error>> {
        let family = column_family(bounded_open, family_name)?;
        let properties = bounded_open
            .db
            .property_value_cf(&family, "rocksdb.aggregated-table-properties")?
            .ok_or("aggregated table properties are unavailable")?;
        for entry in properties.split("; ") {
            if let Some((name, number)) = entry.split_once('=')
                && name.trim() == field
            {
                return Ok(Some(number.trim().parse()?));
            }
        }
        Ok(None)
    }

    fn publish_minimal_ready_wallet(wallet_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let network = Network::ZcashRegtest;
        let unspent = sample_output(0x22, 12_345)?;
        let spent_source = sample_output(0x33, 45_678)?;
        let spent = WalletSpentOutput::new(
            spent_source,
            WalletTransactionPosition::new(
                TransactionId::from_bytes([0x44; 32]),
                1,
                source_position().tip,
            ),
            0,
        );
        let balance = WalletAddressBalance {
            address_script_hash: unspent.address_script_hash,
            balance_zat: unspent.value_zat,
        };
        let builder = fresh_builder(
            wallet_path,
            network,
            source_position(),
            0,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let prepared = semantic_validation_fixture(network, &unspent, &spent, balance)?;
        load_semantic_validation_fixture(&builder, &prepared)?;
        let evidence = ready_evidence(network, &unspent, &spent, balance)?;
        let ready_store = builder
            .reopen_for_validation()?
            .0
            .validate_rows(evidence, validation_config())?
            .publish_ready_at(UnixTimestampMillis::new(1))?;
        drop(ready_store);
        Ok(())
    }

    fn rewrite_control_header_version(
        wallet_path: &Path,
        byte_offset: usize,
        expected_current: u16,
        replacement: u16,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::ExistingPrimary { path: wallet_path },
            RocksDbResourceBudget::for_local_tests(),
            wallet_column_family_descriptors,
        )?;
        let mut control_bytes = bounded_open
            .db
            .get(WALLET_STORE_CONTROL_KEY)?
            .ok_or("missing wallet store_control")?;
        let slice = control_bytes
            .get_mut(byte_offset..byte_offset + 2)
            .ok_or("control header byte offset out of range")?;
        if slice != expected_current.to_be_bytes() {
            return Err("unexpected current control header version".into());
        }
        slice.copy_from_slice(&replacement.to_be_bytes());
        bounded_open
            .db
            .put(WALLET_STORE_CONTROL_KEY, &control_bytes)?;
        Ok(())
    }

    fn assert_all_ready_entry_points_reject<F>(
        wallet_path: &Path,
        check_label: &str,
        matches_expected: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: Fn(&RocksDbWalletError) -> bool,
    {
        let network = Network::ZcashRegtest;
        let budget = RocksDbResourceBudget::for_local_tests();
        let secondary_dir = TempDir::new()?;
        let recovery_dir = TempDir::new()?;
        let recovery_config = WalletRecoveryAdmissionConfig {
            resource_budget: budget,
            staging_path: recovery_dir.path(),
            max_sort_memory_bytes_per_sorter: TEST_VALIDATION_SORT_MEMORY_BYTES,
            max_temporary_file_bytes_per_sorter: TEST_VALIDATION_TEMPORARY_FILE_BYTES,
            max_accounted_reorg_undo_bytes: TEST_VALIDATION_REORG_UNDO_BYTES,
        };
        let Err(open_err) =
            RocksDbWalletStore::open_ready(wallet_path, network, source_identity(), budget)
        else {
            return Err(format!("{check_label}: open_ready unexpectedly succeeded").into());
        };
        assert!(
            matches_expected(&open_err),
            "{check_label} open_ready: {open_err}"
        );

        let Err(follow_err) =
            RocksDbWalletStore::open_ready_for_following(wallet_path, network, budget)
        else {
            return Err(
                format!("{check_label}: open_ready_for_following unexpectedly succeeded").into(),
            );
        };
        assert!(
            matches_expected(&follow_err),
            "{check_label} following: {follow_err}"
        );

        let Err(secondary_err) = crate::RocksDbWalletSecondary::open_ready(
            wallet_path,
            secondary_dir.path(),
            network,
            budget,
        ) else {
            return Err(format!("{check_label}: secondary unexpectedly succeeded").into());
        };
        assert!(
            matches_expected(&secondary_err),
            "{check_label} secondary: {secondary_err}"
        );

        let Err(recovery_err) = RocksDbWalletFollowingStore::cold_admit_recovery_checkpoint(
            wallet_path,
            network,
            recovery_config,
        ) else {
            return Err(format!("{check_label}: recovery unexpectedly succeeded").into());
        };
        assert!(
            matches_expected(&recovery_err),
            "{check_label} recovery: {recovery_err}"
        );
        Ok(())
    }

    #[test]
    fn ready_entry_points_refuse_six_family_store_missing_block_hash_by_height()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let wallet_path = temporary.path().join("wallet-v1-families");
        publish_minimal_ready_wallet(&wallet_path)?;
        {
            let bounded_open = open_bounded_rocksdb(
                RocksDbOpenRole::ExistingPrimary { path: &wallet_path },
                RocksDbResourceBudget::for_local_tests(),
                wallet_column_family_descriptors,
            )?;
            bounded_open
                .db
                .drop_cf(BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY)?;
        }
        assert_all_ready_entry_points_reject(
            &wallet_path,
            "missing block_hash_by_height",
            |error| {
                matches!(
                    error,
                    RocksDbWalletError::ColumnFamilyContractMismatch { expected, observed }
                        if expected.iter().any(|name| name == BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY)
                            && !observed.iter().any(|name| name == BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY)
                )
            },
        )
    }

    #[test]
    fn ready_entry_points_refuse_unsupported_schema_versions()
    -> Result<(), Box<dyn std::error::Error>> {
        for unsupported_version in [1_u16, 3_u16] {
            let temporary = TempDir::new()?;
            let wallet_path = temporary
                .path()
                .join(format!("wallet-schema-{unsupported_version}"));
            publish_minimal_ready_wallet(&wallet_path)?;
            rewrite_control_header_version(
                &wallet_path,
                WALLET_PROJECTION_STORE_IDENTITY.len(),
                WALLET_PROJECTION_SCHEMA_VERSION,
                unsupported_version,
            )?;
            assert_all_ready_entry_points_reject(
                &wallet_path,
                &format!("schema v{unsupported_version}"),
                |error| {
                    matches!(
                        error,
                        RocksDbWalletError::Contract(
                            zinder_wallet_projection::WalletProjectionContractError::UnsupportedWalletProjectionSchemaVersion {
                                encoded,
                                supported: WALLET_PROJECTION_SCHEMA_VERSION,
                            }
                        ) if *encoded == u64::from(unsupported_version)
                    )
                },
            )?;
        }
        Ok(())
    }

    #[test]
    fn ready_entry_points_refuse_unsupported_value_encoding_versions()
    -> Result<(), Box<dyn std::error::Error>> {
        for unsupported_version in [1_u16, 3_u16] {
            let temporary = TempDir::new()?;
            let wallet_path = temporary
                .path()
                .join(format!("wallet-encoding-{unsupported_version}"));
            publish_minimal_ready_wallet(&wallet_path)?;
            rewrite_control_header_version(
                &wallet_path,
                WALLET_PROJECTION_STORE_IDENTITY.len() + 2,
                zinder_wallet_projection::WALLET_PROJECTION_VALUE_ENCODING_VERSION,
                unsupported_version,
            )?;
            assert_all_ready_entry_points_reject(
                &wallet_path,
                &format!("value encoding v{unsupported_version}"),
                |error| {
                    matches!(
                        error,
                        RocksDbWalletError::Contract(
                            zinder_wallet_projection::WalletProjectionContractError::UnsupportedEncodedValue {
                                field: "wallet projection value encoding version",
                                encoded,
                            }
                        ) if *encoded == u64::from(unsupported_version)
                    )
                },
            )?;
        }
        Ok(())
    }

    #[test]
    fn block_hash_chunk_cache_evicts_oldest_chunk_and_resolves_multi_chunk_misses()
    -> Result<(), Box<dyn std::error::Error>> {
        let bounded = BlockHashChunkCache::with_max_chunks(1);
        let mut chunk_zero = Box::new(BlockHashChunk::empty());
        chunk_zero.insert(BlockHeight::new(1), BlockHash::from_bytes([0x11; 32]));
        bounded.insert_chunk(0, chunk_zero);
        let mut chunk_one = Box::new(BlockHashChunk::empty());
        chunk_one.insert(BlockHeight::new(129), BlockHash::from_bytes([0x22; 32]));
        bounded.insert_chunk(1, chunk_one);
        {
            let guard = bounded.state.read();
            assert_eq!(guard.chunks.len(), 1);
            assert!(!guard.chunks.contains_key(&0));
            assert!(guard.chunks.contains_key(&1));
            assert_eq!(guard.fifo.iter().copied().collect::<Vec<_>>(), vec![1]);
        }

        let two_chunks = BlockHashChunkCache::with_max_chunks(2);
        let mut c0 = Box::new(BlockHashChunk::empty());
        c0.insert(BlockHeight::new(1), BlockHash::from_bytes([0x11; 32]));
        two_chunks.insert_chunk(0, c0);
        let mut c1 = Box::new(BlockHashChunk::empty());
        c1.insert(BlockHeight::new(129), BlockHash::from_bytes([0x22; 32]));
        two_chunks.insert_chunk(1, c1);
        two_chunks.invalidate_from(BlockHeight::new(128));
        {
            let guard = two_chunks.state.read();
            assert!(guard.chunks.contains_key(&0));
            assert!(!guard.chunks.contains_key(&1));
            assert_eq!(guard.fifo.iter().copied().collect::<Vec<_>>(), vec![0]);
        }

        let temporary = TempDir::new()?;
        let wallet_path = temporary.path().join("wallet-cache");
        publish_minimal_ready_wallet(&wallet_path)?;
        let mut store = RocksDbWalletStore::open_ready(
            &wallet_path,
            Network::ZcashRegtest,
            source_identity(),
            RocksDbResourceBudget::for_local_tests(),
        )?;
        store.block_hash_cache = BlockHashChunkCache::with_max_chunks(1);
        let extra_row = WalletBlockHash::new(BlockId::new(
            BlockHeight::new(200),
            BlockHash::from_bytes([0x77; 32]),
        ));
        {
            let family = column_family(&store.bounded_open, BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY)?;
            store.bounded_open.db.put_cf(
                &family,
                extra_row.encode_key(),
                extra_row.encode_value(),
            )?;
        }
        let mut resolved = [BlockHash::from_bytes([0; 32]); 2];
        let mut missing = [(1, BlockHeight::new(200)), (0, BlockHeight::new(1))];
        store.resolve_missing_block_hashes(&mut missing, |index, hash| {
            resolved[index] = hash;
        })?;
        assert_eq!(resolved[0], source_position().tip.hash);
        assert_eq!(resolved[1], extra_row.block.hash);
        assert_eq!(store.block_hash_cache.state.read().chunks.len(), 1);
        Ok(())
    }

    #[test]
    fn block_hash_chunk_cache_fails_closed_on_absent_height()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let wallet_path = temporary.path().join("wallet-absent-hash");
        publish_minimal_ready_wallet(&wallet_path)?;
        let store = RocksDbWalletStore::open_ready(
            &wallet_path,
            Network::ZcashRegtest,
            source_identity(),
            RocksDbResourceBudget::for_local_tests(),
        )?;
        {
            let family = column_family(&store.bounded_open, BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY)?;
            store
                .bounded_open
                .db
                .delete_cf(&family, BlockHeight::new(1).value().to_be_bytes())?;
        }
        let address = sample_output(0x22, 12_345)?.address_script_hash;
        assert!(matches!(
            store.require_block_hash(BlockHeight::new(1)),
            Err(RocksDbWalletError::AdmissionChanged {
                reason: "wallet block hash is absent for indexed height",
            })
        ));
        assert!(matches!(
            store.address_transaction_history_page(address, None, NonZeroU16::MIN),
            Err(RocksDbWalletError::AdmissionChanged {
                reason: "wallet block hash is absent for indexed height",
            })
        ));
        assert!(matches!(
            store.address_unspent_outputs_page(address, None, NonZeroU16::MIN),
            Err(RocksDbWalletError::AdmissionChanged {
                reason: "wallet block hash is absent for indexed height",
            })
        ));
        Ok(())
    }
}
