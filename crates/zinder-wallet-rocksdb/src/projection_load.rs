//! Single-scan external construction of wallet projection SSTs.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    io::{BufReader, BufWriter, Read, Write},
    mem::size_of,
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, CanonicalBlockFacts,
    CanonicalBlockFactsDigest, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestBuilder, CanonicalBlockFactsSequenceDigestVersion, Network,
    TransactionId, ValidatedCanonicalBlockReplay,
};
use zinder_rocksdb_bulk_load::{
    BulkLoadError, OrderedSstWriter, ShardedKeyPrefixSorter, ShardedKeyPrefixWriter,
    ShardedSortBudget, ShardedSortedVariableValues, SortedVariableValues, SstFileSet,
    VariableValueSortDurations, VariableValueSortEvidence, active_merge_fan_in,
};
use zinder_store::{
    CanonicalReplaySliceSource, CanonicalStoreError, ObservedCanonicalReplaySequence,
};
use zinder_wallet_projection::{
    StoredWalletSpentOutput, StoredWalletUnspentOutput, WalletAddressBalance,
    WalletAddressTransaction, WalletAddressTransactionKey, WalletAddressUnspentOutputKey,
    WalletBlockHash, WalletOutpointKey, WalletProjectionAccumulator, WalletProjectionContractError,
    WalletProjectionDigest, WalletProjectionFamilyRowCounts, WalletProjectionRowFamily,
    WalletReorgUndo, WalletStoredTransactionPosition, WalletUtxoSetSummary,
};

use crate::{
    RocksDbWalletError, WalletProjectionReplaySource, WalletSecondaryRowDerivationPhaseDurations,
    WalletSstWriteDurations,
    accumulator_hashing::ConstructionAccumulators,
    store::{
        BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY, REORG_UNDO_COLUMN_FAMILY,
        TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY, TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
        TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
        TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
        TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY, WalletColumnFamilyOptions,
    },
};

const OUTPOINT_EVENT_KEY_BYTES: usize = 37;
const ADDRESS_UNSPENT_KEY_BYTES: usize = 72;
const ADDRESS_TRANSACTION_KEY_BYTES: usize = 40;
const SPEND_TRAILER_BYTES: usize = 44;
const BLOCK_HASH_CHUNK_RECORD_BYTES: usize = 36;
const OUTPUT_EVENT_TAG: u8 = 0;
const SPEND_EVENT_TAG: u8 = 1;

/// Default height span scanned by each parallel canonical-scan chunk.
pub(crate) const DEFAULT_CANONICAL_SCAN_CHUNK_BLOCKS: NonZeroU32 = match NonZeroU32::new(4_096) {
    Some(blocks) => blocks,
    None => NonZeroU32::MIN,
};

/// Minimum accounted outpoint-sort memory required per parallel scan shard.
///
/// Keeps each shard well above any valid transparent output record (~10 KiB
/// consensus script cap plus metadata) so splitting memory across workers
/// never refuses a record that would fit under a smaller worker count.
const MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES: u64 = 64 * 1024;

/// Maximum height chunks claimed ahead of ordered sequence-digest verification.
const MAX_IN_FLIGHT_SCAN_CHUNKS: usize = 64;

/// Minimum bounded pipeline queue budget per pipelined bulk SST writer.
const MIN_PIPELINED_SST_QUEUE_BUDGET_BYTES: u64 = 64 * 1024;

/// Maximum bounded pipeline queue budget per pipelined bulk SST writer.
const MAX_PIPELINED_SST_QUEUE_BUDGET_BYTES: u64 = 4 * 1024 * 1024;

/// Maximum cumulative pipeline queue memory across all concurrently open SST writers.
const PIPELINED_SST_TOTAL_QUEUE_CAP_BYTES: u64 = 16 * 1024 * 1024;

/// Per-shard sort memory floor enforced on production key-prefix drain shards so
/// external sorting avoids run explosion past `MAX_MERGE_FAN_IN`.
const MIN_PRODUCTION_SHARD_SORT_MEMORY_BYTES: u64 = 256 * 1024 * 1024;

/// Per-partition push memory floor for `address_index` write-contention sharding.
const MIN_ADDRESS_INDEX_PUSH_SHARD_MEMORY_BYTES: u64 = 64 * 1024 * 1024;

/// Minimum merge fan-in reserved per key-prefix drain shard under automatic
/// production shard sizing.
const MIN_PRODUCTION_SHARD_MERGE_FAN_IN: usize = 32;

/// Minimum merge fan-in reserved per `address_index` push shard when overlapping
/// secondary derivation with `address_transactions`.
const MIN_ADDRESS_INDEX_SHARD_MERGE_FAN_IN: usize = 8;

/// Upper cap on concurrent key-prefix drain shards in production builds.
const MAX_KEY_PREFIX_DRAIN_SHARDS: u32 = 8;

const fn pipelined_sst_queue_budget_bytes(sst_target_logical_bytes: u64) -> u64 {
    let scaled = sst_target_logical_bytes / 8;
    if scaled < MIN_PIPELINED_SST_QUEUE_BUDGET_BYTES {
        MIN_PIPELINED_SST_QUEUE_BUDGET_BYTES
    } else if scaled > MAX_PIPELINED_SST_QUEUE_BUDGET_BYTES {
        MAX_PIPELINED_SST_QUEUE_BUDGET_BYTES
    } else {
        scaled
    }
}

const fn scaled_pipelined_sst_queue_budget_bytes(
    sst_target_logical_bytes: u64,
    active_concurrent_writers: usize,
) -> Option<u64> {
    let single_writer = pipelined_sst_queue_budget_bytes(sst_target_logical_bytes);
    let writers = if active_concurrent_writers == 0 {
        1_u64
    } else {
        active_concurrent_writers as u64
    };
    let shared_cap = PIPELINED_SST_TOTAL_QUEUE_CAP_BYTES / writers;
    let budget = if single_writer < shared_cap {
        single_writer
    } else {
        shared_cap
    };
    if budget < MIN_PIPELINED_SST_QUEUE_BUDGET_BYTES {
        None
    } else {
        Some(budget)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BuildShardPlan {
    outpoint_shards: NonZeroU32,
    address_index_push_shards: NonZeroU32,
    address_index_per_shard_fan_in: usize,
    address_transaction_shards: NonZeroU32,
    address_transaction_per_shard_fan_in: usize,
    overlap_secondary: bool,
}

fn resolve_key_prefix_shard_count(
    requested_workers: NonZeroU32,
    max_sort_memory_bytes: u64,
    min_shard_sort_memory_bytes: u64,
    total_merge_fan_in_budget: usize,
    min_shard_merge_fan_in: usize,
) -> NonZeroU32 {
    if requested_workers.get() <= 1 {
        return NonZeroU32::MIN;
    }
    let memory_shards = u32::try_from(max_sort_memory_bytes / min_shard_sort_memory_bytes.max(1))
        .unwrap_or(u32::MAX)
        .max(1);
    let fan_in_shards = u32::try_from(total_merge_fan_in_budget / min_shard_merge_fan_in.max(2))
        .unwrap_or(u32::MAX)
        .max(1);
    let count = requested_workers
        .get()
        .min(MAX_KEY_PREFIX_DRAIN_SHARDS)
        .min(memory_shards)
        .min(fan_in_shards)
        .max(1);
    NonZeroU32::new(count).unwrap_or(NonZeroU32::MIN)
}

fn resolve_override_shard_plan(
    forced: NonZeroU32,
    requested_workers: NonZeroU32,
    safe_fan_in: usize,
    max_fan_in_shards: u32,
) -> BuildShardPlan {
    let min_fan_in = 2_usize;
    let overlap_secondary = (requested_workers.get() > 1 || forced.get() > 1) && safe_fan_in >= 4;
    let outpoint_shards =
        NonZeroU32::new(forced.get().min(max_fan_in_shards).max(1)).unwrap_or(NonZeroU32::MIN);
    if !overlap_secondary {
        let per_reader_fan_in =
            (safe_fan_in / usize::try_from(outpoint_shards.get()).unwrap_or(1)).max(min_fan_in);
        return BuildShardPlan {
            outpoint_shards,
            address_index_push_shards: outpoint_shards,
            address_index_per_shard_fan_in: per_reader_fan_in,
            address_transaction_shards: outpoint_shards,
            address_transaction_per_shard_fan_in: per_reader_fan_in,
            overlap_secondary: false,
        };
    }
    // Concurrent secondary readers must stay bounded by:
    // address_index_push_shards * idx_fan_in + address_transaction_shards * tx_fan_in <= safe_fan_in.
    let max_pair_shards = (safe_fan_in / min_fan_in).max(2);
    let idx_cap = u32::try_from(max_pair_shards / 2).unwrap_or(u32::MAX);
    let address_index_push_shards =
        NonZeroU32::new(forced.get().min(outpoint_shards.get()).min(idx_cap).max(1))
            .unwrap_or(NonZeroU32::MIN);
    let idx_shards_usize = usize::try_from(address_index_push_shards.get()).unwrap_or(1);
    let tx_cap =
        u32::try_from(max_pair_shards.saturating_sub(idx_shards_usize).max(1)).unwrap_or(u32::MAX);
    let address_transaction_shards =
        NonZeroU32::new(forced.get().min(tx_cap).max(1)).unwrap_or(NonZeroU32::MIN);
    let total_secondary_shards = idx_shards_usize
        .saturating_add(usize::try_from(address_transaction_shards.get()).unwrap_or(1));
    let per_reader_fan_in = (safe_fan_in / total_secondary_shards.max(1)).max(min_fan_in);
    BuildShardPlan {
        outpoint_shards,
        address_index_push_shards,
        address_index_per_shard_fan_in: per_reader_fan_in,
        address_transaction_shards,
        address_transaction_per_shard_fan_in: per_reader_fan_in,
        overlap_secondary: true,
    }
}

fn clamp_address_index_push_shards(
    outpoint_shards: NonZeroU32,
    max_secondary_sort_memory_bytes_per_sorter: u64,
    idx_fd_budget: usize,
) -> NonZeroU32 {
    if outpoint_shards.get() <= 1 {
        return NonZeroU32::MIN;
    }
    let min_fan_in = 2_usize;
    let mem_cap = u32::try_from(
        max_secondary_sort_memory_bytes_per_sorter / MIN_ADDRESS_INDEX_PUSH_SHARD_MEMORY_BYTES,
    )
    .unwrap_or(u32::MAX)
    .max(1);
    let prod_fd_cap = u32::try_from(idx_fd_budget / MIN_ADDRESS_INDEX_SHARD_MERGE_FAN_IN)
        .unwrap_or(u32::MAX)
        .max(1);
    let hard_fd_cap = u32::try_from(idx_fd_budget / min_fan_in)
        .unwrap_or(u32::MAX)
        .max(1);
    NonZeroU32::new(
        outpoint_shards
            .get()
            .min(mem_cap)
            .min(prod_fd_cap)
            .min(hard_fd_cap)
            .max(1),
    )
    .unwrap_or(NonZeroU32::MIN)
}

fn resolve_build_shard_plan_for(
    requested_workers: NonZeroU32,
    max_outpoint_sort_memory_bytes: u64,
    max_secondary_sort_memory_bytes_per_sorter: u64,
    active_fan_in: usize,
    shard_override: Option<NonZeroU32>,
) -> BuildShardPlan {
    let min_fan_in = 2_usize;
    let safe_fan_in = active_fan_in.max(min_fan_in);
    let max_fan_in_shards = u32::try_from((safe_fan_in / min_fan_in).max(1)).unwrap_or(u32::MAX);
    if let Some(forced) = shard_override {
        return resolve_override_shard_plan(
            forced,
            requested_workers,
            safe_fan_in,
            max_fan_in_shards,
        );
    }

    if requested_workers.get() <= 1 {
        return BuildShardPlan {
            outpoint_shards: NonZeroU32::MIN,
            address_index_push_shards: NonZeroU32::MIN,
            address_index_per_shard_fan_in: safe_fan_in,
            address_transaction_shards: NonZeroU32::MIN,
            address_transaction_per_shard_fan_in: safe_fan_in,
            overlap_secondary: false,
        };
    }

    let outpoint_shards = resolve_key_prefix_shard_count(
        requested_workers,
        max_outpoint_sort_memory_bytes,
        MIN_PRODUCTION_SHARD_SORT_MEMORY_BYTES,
        safe_fan_in,
        MIN_PRODUCTION_SHARD_MERGE_FAN_IN,
    );
    let overlap_secondary = safe_fan_in >= 4;
    let idx_fd_budget = if overlap_secondary {
        (safe_fan_in / 4).max(min_fan_in)
    } else {
        safe_fan_in
    };
    let tx_fd_budget = if overlap_secondary {
        safe_fan_in.saturating_sub(idx_fd_budget).max(min_fan_in)
    } else {
        safe_fan_in
    };
    let address_index_push_shards = clamp_address_index_push_shards(
        outpoint_shards,
        max_secondary_sort_memory_bytes_per_sorter,
        idx_fd_budget,
    );
    let address_tx_memory = address_transaction_sort_memory_bytes(
        max_outpoint_sort_memory_bytes,
        max_secondary_sort_memory_bytes_per_sorter,
    );
    let tx_hard_cap = u32::try_from((tx_fd_budget / min_fan_in).max(1)).unwrap_or(u32::MAX);
    let uncapped_tx_shards = resolve_key_prefix_shard_count(
        requested_workers,
        address_tx_memory,
        MIN_PRODUCTION_SHARD_SORT_MEMORY_BYTES,
        tx_fd_budget,
        MIN_PRODUCTION_SHARD_MERGE_FAN_IN,
    );
    let address_transaction_shards =
        NonZeroU32::new(uncapped_tx_shards.get().min(tx_hard_cap).max(1))
            .unwrap_or(NonZeroU32::MIN);
    let address_index_per_shard_fan_in = (idx_fd_budget
        / usize::try_from(address_index_push_shards.get()).unwrap_or(1))
    .max(min_fan_in);
    let address_transaction_per_shard_fan_in = (tx_fd_budget
        / usize::try_from(address_transaction_shards.get()).unwrap_or(1))
    .max(min_fan_in);
    BuildShardPlan {
        outpoint_shards,
        address_index_push_shards,
        address_index_per_shard_fan_in,
        address_transaction_shards,
        address_transaction_per_shard_fan_in,
        overlap_secondary,
    }
}

fn resolve_build_shard_plan(config: WalletProjectionLoadConfig<'_>) -> BuildShardPlan {
    resolve_build_shard_plan_for(
        config.accumulator_hashing_workers,
        config.max_outpoint_sort_memory_bytes,
        config.max_secondary_sort_memory_bytes_per_sorter,
        active_merge_fan_in(),
        None,
    )
}

/// Resource limits for one external projection construction.
#[derive(Clone, Copy)]
pub(crate) struct WalletProjectionLoadConfig<'options> {
    pub(crate) staging_path: &'options Path,
    pub(crate) options: &'options WalletColumnFamilyOptions,
    pub(crate) network: Network,
    pub(crate) first_retained_block: BlockId,
    pub(crate) settled_tip: BlockId,
    pub(crate) supported_reorg_depth: u32,
    pub(crate) max_outpoint_sort_memory_bytes: u64,
    pub(crate) max_secondary_sort_memory_bytes_per_sorter: u64,
    pub(crate) max_temporary_file_bytes_per_sorter: u64,
    pub(crate) sst_target_logical_bytes: u64,
    pub(crate) max_accounted_reorg_undo_bytes: u64,
    pub(crate) accumulator_hashing_workers: NonZeroU32,
    pub(crate) scan_chunk_blocks: NonZeroU32,
}

/// Counts proving the build used one authenticated scan and no prevout reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalletProjectionLoadCounters {
    pub(crate) scanned_block_count: u64,
    pub(crate) scanned_transaction_count: u64,
    pub(crate) staged_output_count: u64,
    pub(crate) staged_spend_count: u64,
    pub(crate) historical_prevout_read_count: u64,
    pub(crate) peak_accounted_reorg_undo_bytes: u64,
    pub(crate) max_accounted_reorg_undo_bytes: u64,
}

/// Phase timings exposed by the fixed-tip build report.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WalletProjectionLoadPhaseDurations {
    pub(crate) canonical_scan: Duration,
    pub(crate) outpoint_sort: Duration,
    pub(crate) outpoint_merge: Duration,
    pub(crate) secondary_row_derivation: Duration,
    pub(crate) secondary_row_derivation_phases: WalletSecondaryRowDerivationPhaseDurations,
    pub(crate) sst_write: WalletSstWriteDurations,
    pub(crate) logical_evidence: Duration,
}

/// One prepared physical family ready for external-file ingestion.
pub(crate) struct PreparedWalletColumnFamily {
    pub(crate) name: &'static str,
    pub(crate) paths: Vec<PathBuf>,
}

/// Complete version-1 SST artifacts and the evidence derived while writing them.
pub(crate) struct PreparedWalletProjectionLoad {
    pub(crate) network: Network,
    pub(crate) supported_reorg_depth: u32,
    pub(crate) first_block: BlockId,
    pub(crate) tip: BlockId,
    pub(crate) source_sequence_digest: CanonicalBlockFactsSequenceDigest,
    pub(crate) projection_digest: WalletProjectionDigest,
    pub(crate) projection_accumulator: WalletProjectionAccumulator,
    pub(crate) row_counts: WalletProjectionFamilyRowCounts,
    pub(crate) utxo_summary: WalletUtxoSetSummary,
    pub(crate) counters: WalletProjectionLoadCounters,
    pub(crate) phase_durations: WalletProjectionLoadPhaseDurations,
    pub(crate) outpoint_sort_evidence: VariableValueSortEvidence,
    pub(crate) address_index_sort_evidence: VariableValueSortEvidence,
    pub(crate) address_transaction_sort_evidence: VariableValueSortEvidence,
    pub(crate) outpoint_sort_durations: VariableValueSortDurations,
    pub(crate) address_index_sort_durations: VariableValueSortDurations,
    pub(crate) address_transaction_sort_durations: VariableValueSortDurations,
    pub(crate) logical_row_bytes: u64,
    pub(crate) sst_file_bytes: u64,
    pub(crate) sst_file_count: u64,
    pub(crate) families: Vec<PreparedWalletColumnFamily>,
}

#[derive(Clone, Copy)]
struct StagedSpend {
    spent_at: WalletStoredTransactionPosition,
    input_index: u32,
}

#[derive(Default)]
struct WalletProjectionSstEvidence {
    logical_row_bytes: u64,
}

impl WalletProjectionSstEvidence {
    fn add_row(&mut self, key: &[u8], encoded_value: &[u8]) -> Result<(), RocksDbWalletError> {
        let row_bytes = u64::try_from(key.len())
            .ok()
            .and_then(|key_bytes| {
                u64::try_from(encoded_value.len())
                    .ok()
                    .and_then(|value_bytes| key_bytes.checked_add(value_bytes))
            })
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        self.logical_row_bytes = self
            .logical_row_bytes
            .checked_add(row_bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        Ok(())
    }

    fn merge(&mut self, branch: &Self) -> Result<(), RocksDbWalletError> {
        self.logical_row_bytes = self
            .logical_row_bytes
            .checked_add(branch.logical_row_bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        Ok(())
    }
}

struct RetainedUndo {
    records: VecDeque<WalletReorgUndo>,
    supported_depth: usize,
    settled_tip: BlockId,
    tracking_current_block: bool,
    accounted_bytes: u64,
    peak_accounted_bytes: u64,
    max_accounted_bytes: u64,
}

impl RetainedUndo {
    fn new(supported_depth: u32, settled_tip: BlockId, max_accounted_bytes: u64) -> Self {
        Self {
            records: VecDeque::new(),
            supported_depth: usize::try_from(supported_depth).unwrap_or(usize::MAX),
            settled_tip,
            tracking_current_block: false,
            accounted_bytes: 0,
            peak_accounted_bytes: 0,
            max_accounted_bytes,
        }
    }

    fn begin_block(
        &mut self,
        block: BlockId,
        parent_hash: BlockHash,
        source_sequence_digest_before: CanonicalBlockFactsSequenceDigest,
        source_sequence_digest_after: CanonicalBlockFactsSequenceDigest,
    ) -> Result<(), RocksDbWalletError> {
        self.tracking_current_block = false;
        if block.height < self.settled_tip.height {
            return Ok(());
        }
        if block.height == self.settled_tip.height {
            if block != self.settled_tip {
                return Err(RocksDbWalletError::CanonicalSourceFenceMismatch {
                    reason: "canonical replay settled-height block differs from the admitted settled tip",
                });
            }
            return Ok(());
        }
        if self.supported_depth == 0 || self.records.len() >= self.supported_depth {
            return Err(RocksDbWalletError::WalletProjectionRebuildRequired {
                reason: "canonical unsettled suffix exceeds the wallet's configured undo capacity",
            });
        }
        self.reserve(
            u64::try_from(size_of::<WalletReorgUndo>())
                .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
        )?;
        self.records.push_back(WalletReorgUndo {
            block,
            parent_hash,
            source_sequence_digest_before,
            source_sequence_digest_after,
            created_outpoints: Vec::new(),
            spent_outpoints: Vec::new(),
            address_transaction_keys: Vec::new(),
        });
        self.tracking_current_block = true;
        Ok(())
    }

    fn add_created_outpoint(&mut self, key: WalletOutpointKey) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        self.reserve(
            u64::try_from(size_of::<WalletOutpointKey>())
                .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
        )?;
        self.records
            .back_mut()
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?
            .created_outpoints
            .push(key);
        Ok(())
    }

    fn add_spent_outpoint(&mut self, key: WalletOutpointKey) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        self.reserve(
            u64::try_from(size_of::<WalletOutpointKey>())
                .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
        )?;
        self.records
            .back_mut()
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?
            .spent_outpoints
            .push(key);
        Ok(())
    }

    fn finish_block(&mut self) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        let undo = self
            .records
            .back_mut()
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        undo.created_outpoints.sort_unstable();
        undo.spent_outpoints.sort_unstable();
        Ok(())
    }

    fn reserve_transient(&mut self, additional_bytes: u64) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        self.reserve(additional_bytes)
    }

    fn release_transient(&mut self, bytes: u64) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        self.accounted_bytes = self
            .accounted_bytes
            .checked_sub(bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        Ok(())
    }

    fn reserve(&mut self, additional_bytes: u64) -> Result<(), RocksDbWalletError> {
        let required_bytes = self
            .accounted_bytes
            .checked_add(additional_bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        if required_bytes > self.max_accounted_bytes {
            return Err(RocksDbWalletError::AccountedReorgUndoMemoryLimit {
                limit_bytes: self.max_accounted_bytes,
                required_bytes,
            });
        }
        self.accounted_bytes = required_bytes;
        self.peak_accounted_bytes = self.peak_accounted_bytes.max(required_bytes);
        Ok(())
    }
}

const UNDO_KEY_VEC_MIN_INITIAL_CAP: usize = 8;

struct RetainedUndoCollector {
    heights: Vec<BlockHeight>,
    slots: Vec<Mutex<Vec<WalletAddressTransactionKey>>>,
    accounted_bytes: AtomicU64,
    peak_accounted_bytes: AtomicU64,
    max_accounted_bytes: u64,
}

impl RetainedUndoCollector {
    fn from_retained_undo(retained: &mut RetainedUndo) -> Self {
        let mut heights = Vec::with_capacity(retained.records.len());
        let mut slots = Vec::with_capacity(retained.records.len());
        for undo in &mut retained.records {
            heights.push(undo.block.height);
            slots.push(Mutex::new(std::mem::take(
                &mut undo.address_transaction_keys,
            )));
        }
        Self {
            heights,
            slots,
            accounted_bytes: AtomicU64::new(retained.accounted_bytes),
            peak_accounted_bytes: AtomicU64::new(retained.peak_accounted_bytes),
            max_accounted_bytes: retained.max_accounted_bytes,
        }
    }

    fn reserve_accounted_elems(
        &self,
        desired_delta_elems: usize,
        elem_bytes: u64,
    ) -> Result<(usize, u64), RocksDbWalletError> {
        let mut current = self.accounted_bytes.load(Ordering::SeqCst);
        loop {
            let headroom = self.max_accounted_bytes.saturating_sub(current);
            let max_delta_elems = usize::try_from(headroom / elem_bytes).unwrap_or(usize::MAX);
            if max_delta_elems == 0 {
                let required_bytes = current
                    .checked_add(elem_bytes)
                    .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
                return Err(RocksDbWalletError::AccountedReorgUndoMemoryLimit {
                    limit_bytes: self.max_accounted_bytes,
                    required_bytes,
                });
            }
            let grant_elems = desired_delta_elems.min(max_delta_elems).max(1);
            let charge_bytes = u64::try_from(grant_elems)
                .ok()
                .and_then(|elems| elems.checked_mul(elem_bytes))
                .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
            let next = current
                .checked_add(charge_bytes)
                .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
            match self.accounted_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    self.peak_accounted_bytes.fetch_max(next, Ordering::AcqRel);
                    return Ok((grant_elems, charge_bytes));
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn charge_overallocated_elems(
        &self,
        extra_elems: usize,
        elem_bytes: u64,
    ) -> Result<(), RocksDbWalletError> {
        let extra_bytes = u64::try_from(extra_elems)
            .ok()
            .and_then(|elems| elems.checked_mul(elem_bytes))
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        let max_bytes = self.max_accounted_bytes;
        let updated =
            self.accounted_bytes
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    let next = current.checked_add(extra_bytes)?;
                    (next <= max_bytes).then_some(next)
                });
        match updated {
            Ok(previous) => {
                let next = previous.saturating_add(extra_bytes);
                self.peak_accounted_bytes.fetch_max(next, Ordering::AcqRel);
                Ok(())
            }
            Err(current) => {
                let required_bytes = current
                    .checked_add(extra_bytes)
                    .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
                if required_bytes <= max_bytes {
                    Err(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)
                } else {
                    Err(RocksDbWalletError::AccountedReorgUndoMemoryLimit {
                        limit_bytes: max_bytes,
                        required_bytes,
                    })
                }
            }
        }
    }

    fn reserve_slot_capacity(
        &self,
        keys: &mut Vec<WalletAddressTransactionKey>,
    ) -> Result<(), RocksDbWalletError> {
        if keys.len() < keys.capacity() {
            return Ok(());
        }
        let elem_size = size_of::<WalletAddressTransactionKey>();
        const {
            assert!(size_of::<WalletAddressTransactionKey>() > 0);
        }
        let elem_bytes = u64::try_from(elem_size)
            .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        let old_cap = keys.capacity();
        let needed_cap = old_cap
            .checked_add(1)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        let desired_cap = old_cap
            .saturating_mul(2)
            .max(UNDO_KEY_VEC_MIN_INITIAL_CAP)
            .max(needed_cap);
        let desired_delta = desired_cap.saturating_sub(old_cap);
        let (grant_elems, charge_bytes) =
            self.reserve_accounted_elems(desired_delta, elem_bytes)?;
        let expected_cap = old_cap
            .checked_add(grant_elems)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        let additional = expected_cap.saturating_sub(keys.len());
        if let Err(source) = keys.try_reserve_exact(additional) {
            self.accounted_bytes
                .fetch_sub(charge_bytes, Ordering::SeqCst);
            return Err(BulkLoadError::MemoryAllocation {
                operation: "wallet reorg undo address transaction keys",
                source,
            }
            .into());
        }
        let actual_cap = keys.capacity();
        if actual_cap > expected_cap {
            self.charge_overallocated_elems(actual_cap.saturating_sub(expected_cap), elem_bytes)?;
        }
        Ok(())
    }

    fn try_collect(&self, key: WalletAddressTransactionKey) -> Result<(), RocksDbWalletError> {
        let Ok(undo_index) = self.heights.binary_search(&key.block_height()) else {
            return Ok(());
        };
        let mut keys = self.slots[undo_index].lock();
        self.reserve_slot_capacity(&mut keys)?;
        keys.push(key);
        drop(keys);
        Ok(())
    }

    fn apply_collected(self, retained: &mut RetainedUndo) -> Result<(), RocksDbWalletError> {
        if self.slots.len() != retained.records.len() {
            return Err(RocksDbWalletError::WalletProjectionLoadAccountingOverflow);
        }
        for (undo, slot) in retained.records.iter_mut().zip(self.slots) {
            undo.address_transaction_keys = slot.into_inner();
        }
        retained.accounted_bytes = self.accounted_bytes.load(Ordering::Acquire);
        retained.peak_accounted_bytes = retained
            .peak_accounted_bytes
            .max(self.peak_accounted_bytes.load(Ordering::Acquire));
        Ok(())
    }
}

trait OutpointEventSink {
    fn push_event(
        &mut self,
        key: [u8; OUTPOINT_EVENT_KEY_BYTES],
        encoded_value: &[u8],
    ) -> Result<(), BulkLoadError>;
}

impl OutpointEventSink for ShardedKeyPrefixWriter<'_, OUTPOINT_EVENT_KEY_BYTES> {
    fn push_event(
        &mut self,
        key: [u8; OUTPOINT_EVENT_KEY_BYTES],
        encoded_value: &[u8],
    ) -> Result<(), BulkLoadError> {
        self.push(key, encoded_value)
    }
}

struct BlockHashStageSink<'options> {
    writer: TimedSstWriter<'options>,
    accumulators: ConstructionAccumulators,
    evidence: WalletProjectionSstEvidence,
}

struct FinishedBlockHashStage {
    files: SstFileSet,
    accumulators: crate::accumulator_hashing::FinishedBranchAccumulators,
    evidence: WalletProjectionSstEvidence,
    #[cfg(test)]
    write_duration: Duration,
    #[cfg(test)]
    finish_duration: Duration,
}

impl<'options> BlockHashStageSink<'options> {
    fn new(config: WalletProjectionLoadConfig<'options>) -> Result<Self, RocksDbWalletError> {
        Ok(Self {
            writer: ordered_writer(
                config,
                WalletProjectionRowFamily::BlockHashByHeight,
                "wallet-block-hash",
            )?,
            accumulators: ConstructionAccumulators::start_optional_branch(config.network, None),
            evidence: WalletProjectionSstEvidence::default(),
        })
    }

    fn append_block(&mut self, block: BlockId) -> Result<(), RocksDbWalletError> {
        let row = WalletBlockHash::new(block);
        append_row(
            &mut self.writer,
            &mut self.accumulators,
            &mut self.evidence,
            WalletProjectionRowFamily::BlockHashByHeight,
            &row.encode_key(),
            &row.encode_value(),
        )
    }

    fn finish(self) -> Result<FinishedBlockHashStage, RocksDbWalletError> {
        #[cfg(test)]
        let finish_started = Instant::now();
        let (files, write_duration) = self.writer.finish()?;
        #[cfg(not(test))]
        let _ = write_duration;
        let accumulators = self.accumulators.finish_branch()?;
        Ok(FinishedBlockHashStage {
            files,
            accumulators,
            evidence: self.evidence,
            #[cfg(test)]
            write_duration,
            #[cfg(test)]
            finish_duration: finish_started.elapsed(),
        })
    }
}

struct TempChunkBlockHashesFile {
    path: PathBuf,
    armed: bool,
}

impl TempChunkBlockHashesFile {
    fn new_armed(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn delete(mut self) -> Result<(), RocksDbWalletError> {
        let path = std::mem::take(&mut self.path);
        self.armed = false;
        fs::remove_file(&path)
            .map_err(|source| RocksDbWalletError::PathUnavailable { path, source })
    }
}

impl Drop for TempChunkBlockHashesFile {
    fn drop(&mut self) {
        if self.armed && !self.path.as_os_str().is_empty() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn stream_chunk_block_hashes(
    path: &Path,
    block_count: usize,
    sink: &mut BlockHashStageSink<'_>,
) -> Result<(), RocksDbWalletError> {
    let file = fs::File::open(path).map_err(|source| RocksDbWalletError::PathUnavailable {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let mut record = [0_u8; BLOCK_HASH_CHUNK_RECORD_BYTES];
    for _ in 0..block_count {
        reader
            .read_exact(&mut record)
            .map_err(|source| RocksDbWalletError::PathUnavailable {
                path: path.to_path_buf(),
                source,
            })?;
        let mut height_bytes = [0_u8; 4];
        height_bytes.copy_from_slice(&record[..4]);
        let mut hash_bytes = [0_u8; 32];
        hash_bytes.copy_from_slice(&record[4..]);
        let block = BlockId::new(
            BlockHeight::new(u32::from_be_bytes(height_bytes)),
            BlockHash::from_bytes(hash_bytes),
        );
        sink.append_block(block)?;
    }
    Ok(())
}

struct CompletedCanonicalScan {
    first_block: BlockId,
    tip: BlockId,
    source_sequence_digest: CanonicalBlockFactsSequenceDigest,
    retained_undo: RetainedUndo,
    scanned_block_count: u64,
    scanned_transaction_count: u64,
    staged_output_count: u64,
    staged_spend_count: u64,
    canonical_scan: Duration,
    outpoint_sort: Duration,
    sorted_outpoints: ShardedSortedVariableValues<OUTPOINT_EVENT_KEY_BYTES>,
    outpoint_sort_evidence: VariableValueSortEvidence,
    outpoint_sort_durations: VariableValueSortDurations,
    block_hash_files: SstFileSet,
    block_hash_accumulators: crate::accumulator_hashing::FinishedBranchAccumulators,
    block_hash_evidence: WalletProjectionSstEvidence,
    #[cfg(test)]
    block_hash_write_duration: Duration,
    #[cfg(test)]
    block_hash_finish_duration: Duration,
}

#[derive(Default)]
struct ScanCounters {
    blocks: u64,
    transactions: u64,
    staged_outputs: u64,
    staged_spends: u64,
}

impl ScanCounters {
    fn combine(&mut self, other: &Self) -> Result<(), RocksDbWalletError> {
        self.blocks = add_counter(self.blocks, other.blocks)?;
        self.transactions = add_counter(self.transactions, other.transactions)?;
        self.staged_outputs = add_counter(self.staged_outputs, other.staged_outputs)?;
        self.staged_spends = add_counter(self.staged_spends, other.staged_spends)?;
        Ok(())
    }
}

struct CompletedReplayChunk {
    chunk_index: usize,
    first_block: BlockId,
    first_parent_hash: BlockHash,
    last_block: BlockId,
    counters: ScanCounters,
    digests: Vec<CanonicalBlockFactsDigest>,
    block_hashes_file: TempChunkBlockHashesFile,
}

struct ChunkWindowState {
    next_to_claim: usize,
    next_to_verify: usize,
    stopped: bool,
}

struct ChunkWindow {
    chunks: Vec<BlockHeightRange>,
    max_in_flight: usize,
    stop_flag: AtomicBool,
    #[cfg(test)]
    panic_on_chunk: std::sync::atomic::AtomicUsize,
    state: Mutex<ChunkWindowState>,
    available: Condvar,
}

impl ChunkWindow {
    fn new(chunks: Vec<BlockHeightRange>, max_in_flight: usize) -> Self {
        Self {
            chunks,
            max_in_flight: max_in_flight.max(1),
            stop_flag: AtomicBool::new(false),
            #[cfg(test)]
            panic_on_chunk: std::sync::atomic::AtomicUsize::new(
                NEXT_WINDOW_PANIC_CHUNK.take().unwrap_or(usize::MAX),
            ),
            state: Mutex::new(ChunkWindowState {
                next_to_claim: 0,
                next_to_verify: 0,
                stopped: false,
            }),
            available: Condvar::new(),
        }
    }

    fn is_stopped(&self) -> bool {
        self.stop_flag.load(Ordering::Relaxed)
    }

    fn stop(&self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        self.state.lock().stopped = true;
        self.available.notify_all();
    }

    fn claim_next(&self) -> Option<(usize, BlockHeightRange)> {
        let mut state = self.state.lock();
        loop {
            if state.stopped || state.next_to_claim >= self.chunks.len() {
                return None;
            }
            if state.next_to_claim.saturating_sub(state.next_to_verify) < self.max_in_flight {
                let chunk_index = state.next_to_claim;
                state.next_to_claim = chunk_index.saturating_add(1);
                drop(state);
                return Some((chunk_index, self.chunks[chunk_index]));
            }
            self.available.wait(&mut state);
        }
    }

    fn mark_verified(&self, next_to_verify: usize) {
        self.state.lock().next_to_verify = next_to_verify;
        self.available.notify_all();
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only counter: when `Some(n)`, the `n`th canonical-scan worker spawn
    /// on this thread returns a refused-thread error.
    static REFUSED_CANONICAL_SCAN_THREAD: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };

    /// Test-only one-shot arming slot consumed by [`ChunkWindow::new`] on the
    /// spawning thread so concurrent tests do not share a process-global hook.
    static NEXT_WINDOW_PANIC_CHUNK: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
struct PanicChunkArmGuard;

#[cfg(test)]
impl Drop for PanicChunkArmGuard {
    fn drop(&mut self) {
        NEXT_WINDOW_PANIC_CHUNK.set(None);
    }
}

#[cfg(test)]
fn arm_next_window_panic_chunk(chunk_index: usize) -> PanicChunkArmGuard {
    NEXT_WINDOW_PANIC_CHUNK.set(Some(chunk_index));
    PanicChunkArmGuard
}

#[cfg(test)]
impl ChunkWindow {
    #[expect(
        clippy::panic,
        reason = "intentional unwind injection for testing worker-panic recovery"
    )]
    fn maybe_inject_worker_unwind(&self, chunk_index: usize) {
        if self
            .panic_on_chunk
            .compare_exchange(chunk_index, usize::MAX, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            panic!("injected canonical scan worker unwind on chunk {chunk_index}");
        }
    }
}

/// Stops and wakes the shared [`ChunkWindow`] if a scan worker unwinds so
/// sibling workers exit instead of filling the claim window and deadlocking.
struct StopWindowOnUnwind<'a>(&'a ChunkWindow);

impl Drop for StopWindowOnUnwind<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.stop();
        }
    }
}

/// Spawns one scoped canonical-scan worker, converting OS thread-start
/// failures into [`RocksDbWalletError::CanonicalScanThreadUnavailable`].
fn spawn_canonical_scan_worker<'scope, 'env, F, T>(
    scope: &'scope std::thread::Scope<'scope, 'env>,
    worker_index: usize,
    task: F,
) -> Result<std::thread::ScopedJoinHandle<'scope, T>, RocksDbWalletError>
where
    F: FnOnce() -> T + Send + 'scope,
    T: Send + 'scope,
{
    #[cfg(test)]
    if let Some(remaining) = REFUSED_CANONICAL_SCAN_THREAD.get() {
        if remaining == 0 {
            REFUSED_CANONICAL_SCAN_THREAD.set(None);
            return Err(RocksDbWalletError::CanonicalScanThreadUnavailable {
                reason: "injected thread start refusal".to_owned(),
            });
        }
        REFUSED_CANONICAL_SCAN_THREAD.set(Some(remaining - 1));
    }
    std::thread::Builder::new()
        .name(format!("zinder-wallet-scan-{worker_index}"))
        .spawn_scoped(scope, task)
        .map_err(
            |source| RocksDbWalletError::CanonicalScanThreadUnavailable {
                reason: source.to_string(),
            },
        )
}

/// Splits `[first_height ..= settled_height]` into contiguous inclusive height
/// chunks of at most `chunk_blocks` blocks each.
fn partition_settled_height_chunks(
    first_height: BlockHeight,
    settled_height: BlockHeight,
    chunk_blocks: NonZeroU32,
) -> Vec<BlockHeightRange> {
    if first_height > settled_height {
        return Vec::new();
    }
    let span = chunk_blocks.get();
    let mut chunks = Vec::new();
    let mut current = first_height.value();
    let last = settled_height.value();
    loop {
        let chunk_end = current.saturating_add(span - 1).min(last);
        chunks.push(BlockHeightRange::inclusive(
            BlockHeight::new(current),
            BlockHeight::new(chunk_end),
        ));
        if chunk_end == last {
            break;
        }
        current = chunk_end + 1;
    }
    chunks
}

/// Effective worker count for parallel canonical replay scanning.
///
/// Caps workers by the configured worker limit, the number of settled chunks,
/// and the per-shard minimum sort memory so each worker can keep typical
/// transparent outputs buffered before spilling (while oversized outputs within
/// the global budget spill directly as single-record runs).
fn parallel_scan_worker_count(
    requested_workers: NonZeroU32,
    chunk_count: usize,
    max_outpoint_sort_memory_bytes: u64,
) -> NonZeroU32 {
    let max_for_memory = max_outpoint_sort_memory_bytes / MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES;
    let workers = u64::from(requested_workers.get())
        .min(u64::try_from(chunk_count).unwrap_or(u64::MAX))
        .min(max_for_memory)
        .min(u64::from(crate::MAX_ACCUMULATOR_HASHING_WORKERS));
    NonZeroU32::new(u32::try_from(workers).unwrap_or(1)).unwrap_or(NonZeroU32::MIN)
}

/// Writes all six wallet families from a replay source, scanning the settled
/// prefix in parallel height chunks when the source supports slices and more
/// than one worker is active.
pub(crate) fn write_projection_ssts_from_source<Source>(
    config: WalletProjectionLoadConfig<'_>,
    canonical_store: &Source,
) -> Result<PreparedWalletProjectionLoad, RocksDbWalletError>
where
    Source: WalletProjectionReplaySource + ?Sized,
{
    let shard_plan = resolve_build_shard_plan(config);
    if let Some(slice_source) = canonical_store.wallet_projection_slice_source() {
        let chunks = partition_settled_height_chunks(
            config.first_retained_block.height,
            config.settled_tip.height,
            config.scan_chunk_blocks,
        );
        let workers = parallel_scan_worker_count(
            config.accumulator_hashing_workers,
            chunks.len(),
            config.max_outpoint_sort_memory_bytes,
        );
        if workers.get() > 1 {
            let scanned = scan_canonical_replay_parallel(
                config,
                &slice_source,
                chunks,
                workers,
                shard_plan.outpoint_shards,
            )?;
            return finish_projection_ssts_with_plan(config, scanned, shard_plan);
        }
    }
    let blocks = canonical_store
        .wallet_projection_scan()
        .map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
    write_projection_ssts_with_plan(config, blocks, shard_plan)
}

/// Writes all six wallet families from one authenticated canonical replay scan.
#[cfg(test)]
pub(crate) fn write_projection_ssts(
    config: WalletProjectionLoadConfig<'_>,
    blocks: impl IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
) -> Result<PreparedWalletProjectionLoad, RocksDbWalletError> {
    let shard_plan = resolve_build_shard_plan(config);
    write_projection_ssts_with_plan(config, blocks, shard_plan)
}

fn write_projection_ssts_with_plan(
    config: WalletProjectionLoadConfig<'_>,
    blocks: impl IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
    shard_plan: BuildShardPlan,
) -> Result<PreparedWalletProjectionLoad, RocksDbWalletError> {
    let scanned = scan_canonical_replay_serial(config, blocks, shard_plan.outpoint_shards)?;
    finish_projection_ssts_with_plan(config, scanned, shard_plan)
}

struct CanonicalScanOutcome {
    first_block: BlockId,
    tip: BlockId,
    source_sequence_digest: CanonicalBlockFactsSequenceDigest,
    retained_undo: RetainedUndo,
    counters: ScanCounters,
    scan_started: Instant,
}

fn finish_canonical_scan(
    outcome: CanonicalScanOutcome,
    outpoint_sorter: ShardedKeyPrefixSorter<OUTPOINT_EVENT_KEY_BYTES>,
    block_hash_sink: BlockHashStageSink<'_>,
) -> Result<CompletedCanonicalScan, RocksDbWalletError> {
    let block_hash = block_hash_sink.finish()?;
    // Measure canonical_scan after flushing block_hash_by_height SSTs and
    // finalizing its branch accumulators so that work does not fall into an
    // untimed gap before outpoint_sort begins.
    let canonical_scan = outcome.scan_started.elapsed();
    let phase_started = Instant::now();
    let sorted_outpoints = outpoint_sorter.finish()?;
    let outpoint_sort_evidence = sorted_outpoints.evidence();
    let outpoint_sort_durations = sorted_outpoints.durations();
    let outpoint_sort = phase_started.elapsed();

    Ok(CompletedCanonicalScan {
        first_block: outcome.first_block,
        tip: outcome.tip,
        source_sequence_digest: outcome.source_sequence_digest,
        retained_undo: outcome.retained_undo,
        scanned_block_count: outcome.counters.blocks,
        scanned_transaction_count: outcome.counters.transactions,
        staged_output_count: outcome.counters.staged_outputs,
        staged_spend_count: outcome.counters.staged_spends,
        canonical_scan,
        outpoint_sort,
        sorted_outpoints,
        outpoint_sort_evidence,
        outpoint_sort_durations,
        block_hash_files: block_hash.files,
        block_hash_accumulators: block_hash.accumulators,
        block_hash_evidence: block_hash.evidence,
        #[cfg(test)]
        block_hash_write_duration: block_hash.write_duration,
        #[cfg(test)]
        block_hash_finish_duration: block_hash.finish_duration,
    })
}

fn scan_canonical_replay_serial(
    config: WalletProjectionLoadConfig<'_>,
    blocks: impl IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
    outpoint_shards: NonZeroU32,
) -> Result<CompletedCanonicalScan, RocksDbWalletError> {
    let scan_started = Instant::now();
    let outpoint_sorter = ShardedKeyPrefixSorter::<OUTPOINT_EVENT_KEY_BYTES>::new(
        config.staging_path,
        "wallet-outpoint-events",
        outpoint_shards,
        config.max_outpoint_sort_memory_bytes,
        config.max_temporary_file_bytes_per_sorter,
    )?;
    let mut retained_undo = RetainedUndo::new(
        config.supported_reorg_depth,
        config.settled_tip,
        config.max_accounted_reorg_undo_bytes,
    );
    let mut sequence_digest =
        CanonicalBlockFactsSequenceDigestBuilder::new(CanonicalBlockFactsSequenceDigestVersion::V1);
    let mut block_hash_sink = BlockHashStageSink::new(config)?;
    let mut first_block = None;
    let mut previous_block = None;
    let mut counters = ScanCounters::default();
    let mut outpoint_writer = outpoint_sorter.writer(NonZeroU32::MIN);

    for replay in blocks {
        let replay = replay.map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
        let block = stage_serial_block(
            config,
            replay,
            &mut first_block,
            previous_block,
            &mut sequence_digest,
            &mut retained_undo,
            &mut block_hash_sink,
            &mut outpoint_writer,
            &mut counters,
        )?;
        previous_block = Some(block);
    }
    outpoint_writer.flush()?;
    let first_block = first_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    let tip = previous_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    finish_canonical_scan(
        CanonicalScanOutcome {
            first_block,
            tip,
            source_sequence_digest: sequence_digest.finish(),
            retained_undo,
            counters,
            scan_started,
        },
        outpoint_sorter,
        block_hash_sink,
    )
}

struct ParallelSettledScan {
    first_block: Option<BlockId>,
    previous_block: Option<BlockId>,
    counters: ScanCounters,
}

fn scan_canonical_replay_parallel(
    config: WalletProjectionLoadConfig<'_>,
    slice_source: &CanonicalReplaySliceSource<'_>,
    chunks: Vec<BlockHeightRange>,
    worker_count: NonZeroU32,
    outpoint_shards: NonZeroU32,
) -> Result<CompletedCanonicalScan, RocksDbWalletError> {
    let scan_started = Instant::now();
    let outpoint_sorter = ShardedKeyPrefixSorter::<OUTPOINT_EVENT_KEY_BYTES>::new(
        config.staging_path,
        "wallet-outpoint-events",
        outpoint_shards,
        config.max_outpoint_sort_memory_bytes,
        config.max_temporary_file_bytes_per_sorter,
    )?;
    let mut retained_undo = RetainedUndo::new(
        config.supported_reorg_depth,
        config.settled_tip,
        config.max_accounted_reorg_undo_bytes,
    );
    let mut sequence_digest =
        CanonicalBlockFactsSequenceDigestBuilder::new(CanonicalBlockFactsSequenceDigestVersion::V1);
    let mut block_hash_sink = BlockHashStageSink::new(config)?;
    let mut settled = run_parallel_settled_chunks(
        &outpoint_sorter,
        config.staging_path,
        slice_source,
        chunks,
        worker_count,
        config.first_retained_block,
        &mut sequence_digest,
        &mut block_hash_sink,
    )?;
    if settled.previous_block != Some(config.settled_tip) {
        return Err(RocksDbWalletError::CanonicalSourceFenceMismatch {
            reason: "canonical replay settled-height block differs from the admitted settled tip",
        });
    }
    scan_unsettled_suffix(
        config,
        slice_source,
        &outpoint_sorter,
        (
            &mut settled,
            &mut sequence_digest,
            &mut retained_undo,
            &mut block_hash_sink,
        ),
    )?;
    let ParallelSettledScan {
        first_block,
        previous_block,
        counters,
    } = settled;
    let first_block = first_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    let tip = previous_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    let final_sequence_digest = sequence_digest.finish();
    slice_source
        .verify_complete_sequence(ObservedCanonicalReplaySequence {
            first_block,
            tip,
            block_count: counters.blocks,
            sequence_digest: final_sequence_digest,
        })
        .map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
    finish_canonical_scan(
        CanonicalScanOutcome {
            first_block,
            tip,
            source_sequence_digest: final_sequence_digest,
            retained_undo,
            counters,
            scan_started,
        },
        outpoint_sorter,
        block_hash_sink,
    )
}

fn scan_unsettled_suffix(
    config: WalletProjectionLoadConfig<'_>,
    slice_source: &CanonicalReplaySliceSource<'_>,
    outpoint_sorter: &ShardedKeyPrefixSorter<OUTPOINT_EVENT_KEY_BYTES>,
    suffix_state: (
        &mut ParallelSettledScan,
        &mut CanonicalBlockFactsSequenceDigestBuilder,
        &mut RetainedUndo,
        &mut BlockHashStageSink<'_>,
    ),
) -> Result<(), RocksDbWalletError> {
    let (settled, sequence_digest, retained_undo, block_hash_sink) = suffix_state;
    let visible_tip = slice_source.ready_evidence().visible_tip;
    let Some(suffix_start) = config.settled_tip.height.next() else {
        return Ok(());
    };
    if suffix_start > visible_tip.height {
        return Ok(());
    }
    let mut suffix_writer = outpoint_sorter.writer(NonZeroU32::MIN);
    let suffix_scan = slice_source
        .scan_slice(BlockHeightRange::inclusive(
            suffix_start,
            visible_tip.height,
        ))
        .map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
    for replay in suffix_scan {
        let replay = replay.map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
        let block = stage_serial_block(
            config,
            replay,
            &mut settled.first_block,
            settled.previous_block,
            sequence_digest,
            retained_undo,
            block_hash_sink,
            &mut suffix_writer,
            &mut settled.counters,
        )?;
        settled.previous_block = Some(block);
    }
    suffix_writer.flush()?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "coordinates scoped parallel scan workers and ordered sequence verification"
)]
fn run_parallel_settled_chunks(
    outpoint_sorter: &ShardedKeyPrefixSorter<OUTPOINT_EVENT_KEY_BYTES>,
    staging_path: &Path,
    slice_source: &CanonicalReplaySliceSource<'_>,
    chunks: Vec<BlockHeightRange>,
    worker_count: NonZeroU32,
    first_retained_block: BlockId,
    sequence_digest: &mut CanonicalBlockFactsSequenceDigestBuilder,
    block_hash_sink: &mut BlockHashStageSink<'_>,
) -> Result<ParallelSettledScan, RocksDbWalletError> {
    let total_chunks = chunks.len();
    let worker_threads = usize::try_from(worker_count.get()).unwrap_or(1);
    let max_in_flight = worker_threads
        .saturating_mul(4)
        .clamp(worker_threads, MAX_IN_FLIGHT_SCAN_CHUNKS);
    let window = ChunkWindow::new(chunks, max_in_flight);
    let window_ref = &window;
    let (completed_tx, completed_rx) = mpsc::sync_channel::<
        Result<CompletedReplayChunk, RocksDbWalletError>,
    >(max_in_flight.saturating_add(worker_threads));
    std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(worker_threads);
        let mut start_failure = None;
        for worker_index in 0..worker_threads {
            let sender = completed_tx.clone();
            match spawn_canonical_scan_worker(scope, worker_index, move || {
                let writer = outpoint_sorter.writer(worker_count);
                scan_worker_chunks(slice_source, window_ref, staging_path, writer, &sender)
            }) {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    window.stop();
                    start_failure = Some(error);
                    break;
                }
            }
        }
        drop(completed_tx);

        let verify_outcome = if start_failure.is_none() {
            verify_completed_chunks(
                &window,
                &completed_rx,
                (first_retained_block, total_chunks),
                sequence_digest,
                block_hash_sink,
            )
        } else {
            Ok((None, None, ScanCounters::default()))
        };
        if verify_outcome.is_err() {
            window.stop();
        }

        let mut worker_error = None;
        let mut worker_stopped = false;
        for worker in workers {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    if worker_error.is_none() {
                        worker_error = Some(error);
                    }
                }
                Err(_) => worker_stopped = true,
            }
        }
        if let Some(error) = start_failure {
            return Err(error);
        }
        let (first_block, previous_block, counters) = verify_outcome?;
        if let Some(error) = worker_error {
            return Err(error);
        }
        if worker_stopped {
            return Err(RocksDbWalletError::CanonicalScanWorkerStopped);
        }
        Ok(ParallelSettledScan {
            first_block,
            previous_block,
            counters,
        })
    })
}

fn scan_worker_chunks(
    slice_source: &CanonicalReplaySliceSource<'_>,
    window: &ChunkWindow,
    staging_path: &Path,
    mut writer: ShardedKeyPrefixWriter<'_, OUTPOINT_EVENT_KEY_BYTES>,
    completed_tx: &SyncSender<Result<CompletedReplayChunk, RocksDbWalletError>>,
) -> Result<(), RocksDbWalletError> {
    let _stop_on_unwind = StopWindowOnUnwind(window);
    while let Some((chunk_index, range)) = window.claim_next() {
        #[cfg(test)]
        window.maybe_inject_worker_unwind(chunk_index);
        let outcome = scan_one_settled_chunk(
            slice_source,
            window,
            staging_path,
            (chunk_index, range),
            &mut writer,
        );
        let failed = outcome.is_err();
        if failed {
            window.stop();
        }
        if completed_tx.send(outcome).is_err() || failed {
            break;
        }
    }
    writer.flush().map_err(RocksDbWalletError::from)
}

fn scan_one_settled_chunk(
    slice_source: &CanonicalReplaySliceSource<'_>,
    window: &ChunkWindow,
    staging_path: &Path,
    chunk: (usize, BlockHeightRange),
    sink: &mut impl OutpointEventSink,
) -> Result<CompletedReplayChunk, RocksDbWalletError> {
    let (chunk_index, range) = chunk;
    let expected_blocks =
        usize::try_from(u64::from(range.end.value() - range.start.value()) + 1).unwrap_or(0);
    let mut digests = Vec::with_capacity(expected_blocks);
    let mut first_block = None;
    let mut first_parent_hash = None;
    let mut last_block = None;
    let mut counters = ScanCounters::default();
    let chunk_hashes_path =
        staging_path.join(format!("wallet-block-hash-chunk-{chunk_index:08}.bin"));
    let hashes_file = fs::File::create(&chunk_hashes_path).map_err(|source| {
        RocksDbWalletError::PathUnavailable {
            path: chunk_hashes_path.clone(),
            source,
        }
    })?;
    let block_hashes_file = TempChunkBlockHashesFile::new_armed(chunk_hashes_path);
    let mut hashes_writer = BufWriter::new(hashes_file);
    let scan = slice_source
        .scan_slice(range)
        .map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
    for replay in scan {
        if window.is_stopped() {
            return Err(RocksDbWalletError::CanonicalScanWorkerStopped);
        }
        let replay = replay.map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
        let reference_digest = replay.reference_digest();
        let facts = replay.into_facts();
        let block = BlockId::new(facts.block_header.height, facts.block_header.block_hash);
        if first_block.is_none() {
            first_block = Some(block);
            first_parent_hash = Some(facts.block_header.parent_hash);
        }
        last_block = Some(block);
        let mut hash_record = [0_u8; BLOCK_HASH_CHUNK_RECORD_BYTES];
        hash_record[..4].copy_from_slice(&block.height.value().to_be_bytes());
        hash_record[4..].copy_from_slice(&block.hash.as_bytes());
        hashes_writer.write_all(&hash_record).map_err(|source| {
            RocksDbWalletError::PathUnavailable {
                path: block_hashes_file.path().to_path_buf(),
                source,
            }
        })?;
        digests.push(reference_digest);
        counters.blocks = increment(counters.blocks)?;
        stage_settled_block_transactions(&facts, block, sink, &mut counters)?;
    }
    hashes_writer
        .flush()
        .map_err(|source| RocksDbWalletError::PathUnavailable {
            path: block_hashes_file.path().to_path_buf(),
            source,
        })?;
    drop(hashes_writer);
    let first_block = first_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    let first_parent_hash = first_parent_hash.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    let last_block = last_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    Ok(CompletedReplayChunk {
        chunk_index,
        first_block,
        first_parent_hash,
        last_block,
        counters,
        digests,
        block_hashes_file,
    })
}

fn verify_completed_chunks(
    window: &ChunkWindow,
    completed_rx: &Receiver<Result<CompletedReplayChunk, RocksDbWalletError>>,
    bounds: (BlockId, usize),
    sequence_digest: &mut CanonicalBlockFactsSequenceDigestBuilder,
    block_hash_sink: &mut BlockHashStageSink<'_>,
) -> Result<(Option<BlockId>, Option<BlockId>, ScanCounters), RocksDbWalletError> {
    let (first_retained_block, total_chunks) = bounds;
    let mut reorder = BTreeMap::new();
    let mut next_to_verify = 0_usize;
    let mut first_block = None;
    let mut previous_block = None;
    let mut counters = ScanCounters::default();
    while next_to_verify < total_chunks {
        let received = completed_rx
            .recv()
            .map_err(|_| RocksDbWalletError::CanonicalScanWorkerStopped)??;
        reorder.insert(received.chunk_index, received);
        while let Some(ready) = reorder.remove(&next_to_verify) {
            validate_next_block(
                first_retained_block,
                previous_block,
                ready.first_block,
                ready.first_parent_hash,
            )?;
            first_block.get_or_insert(ready.first_block);
            let block_count = ready.digests.len();
            for digest in ready.digests {
                sequence_digest.try_append(digest)?;
            }
            stream_chunk_block_hashes(
                ready.block_hashes_file.path(),
                block_count,
                block_hash_sink,
            )?;
            ready.block_hashes_file.delete()?;
            counters.combine(&ready.counters)?;
            previous_block = Some(ready.last_block);
            next_to_verify = next_to_verify.saturating_add(1);
            window.mark_verified(next_to_verify);
        }
    }
    Ok((first_block, previous_block, counters))
}

fn stage_settled_block_transactions(
    facts: &CanonicalBlockFacts,
    block: BlockId,
    sink: &mut impl OutpointEventSink,
    counters: &mut ScanCounters,
) -> Result<(), RocksDbWalletError> {
    for (transaction_index, transaction) in facts.transactions.iter().enumerate() {
        let tx_index_in_block = u32::try_from(transaction_index)
            .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
        counters.transactions = increment(counters.transactions)?;
        let transaction_position = WalletStoredTransactionPosition::new(
            transaction.public_facts.transaction_id,
            tx_index_in_block,
            block.height,
        );
        for (input_position, input) in transaction.transparent_inputs.iter().enumerate() {
            let expected_input_index = u32::try_from(input_position)
                .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
            if input.input_index != expected_input_index {
                return Err(WalletProjectionContractError::FactIndexMismatch.into());
            }
            if input.spent_outpoint.is_coinbase_sentinel() {
                continue;
            }
            let outpoint_key = WalletOutpointKey::new(input.spent_outpoint);
            sink.push_event(
                outpoint_event_key(outpoint_key, SPEND_EVENT_TAG),
                &encode_spend_trailer(StagedSpend {
                    spent_at: transaction_position,
                    input_index: input.input_index,
                }),
            )?;
            counters.staged_spends = increment(counters.staged_spends)?;
        }
        for (output_position, output) in transaction.transparent_outputs.iter().enumerate() {
            let expected_output_index = u32::try_from(output_position)
                .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
            if output.output_index != expected_output_index {
                return Err(WalletProjectionContractError::FactIndexMismatch.into());
            }
            let outpoint = zinder_core::TransparentOutPoint::new(
                transaction.public_facts.transaction_id,
                output.output_index,
            );
            let outpoint_key = WalletOutpointKey::new(outpoint);
            let wallet_output = StoredWalletUnspentOutput::new(
                outpoint,
                output.address_script_hash,
                output.value_zat,
                output.script_pub_key.clone(),
                transaction_position,
            )?;
            sink.push_event(
                outpoint_event_key(outpoint_key, OUTPUT_EVENT_TAG),
                &wallet_output.encode_value()?,
            )?;
            counters.staged_outputs = increment(counters.staged_outputs)?;
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "serial block staging updates sequence digest, retained undo, block hashes, outpoint sink, and counters together"
)]
#[allow(
    clippy::too_many_lines,
    reason = "serial block staging keeps input/output validation and transient undo accounting in one loop"
)]
fn stage_serial_block(
    config: WalletProjectionLoadConfig<'_>,
    replay: ValidatedCanonicalBlockReplay,
    first_block: &mut Option<BlockId>,
    previous_block: Option<BlockId>,
    sequence_digest: &mut CanonicalBlockFactsSequenceDigestBuilder,
    retained_undo: &mut RetainedUndo,
    block_hash_sink: &mut BlockHashStageSink<'_>,
    sink: &mut impl OutpointEventSink,
    counters: &mut ScanCounters,
) -> Result<BlockId, RocksDbWalletError> {
    let reference_digest = replay.reference_digest();
    let facts = replay.into_facts();
    let block = BlockId::new(facts.block_header.height, facts.block_header.block_hash);
    validate_next_block(
        config.first_retained_block,
        previous_block,
        block,
        facts.block_header.parent_hash,
    )?;
    first_block.get_or_insert(block);
    block_hash_sink.append_block(block)?;
    let source_sequence_digest_before = sequence_digest.clone().finish();
    sequence_digest.try_append(reference_digest)?;
    let source_sequence_digest_after = sequence_digest.clone().finish();
    counters.blocks = increment(counters.blocks)?;

    retained_undo.begin_block(
        block,
        facts.block_header.parent_hash,
        source_sequence_digest_before,
        source_sequence_digest_after,
    )?;
    let mut block_created_outpoints = BTreeSet::new();
    for (transaction_index, transaction) in facts.transactions.iter().enumerate() {
        let tx_index_in_block = u32::try_from(transaction_index)
            .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
        counters.transactions = increment(counters.transactions)?;
        let transaction_position = WalletStoredTransactionPosition::new(
            transaction.public_facts.transaction_id,
            tx_index_in_block,
            block.height,
        );
        for (input_position, input) in transaction.transparent_inputs.iter().enumerate() {
            let expected_input_index = u32::try_from(input_position)
                .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
            if input.input_index != expected_input_index {
                return Err(WalletProjectionContractError::FactIndexMismatch.into());
            }
            if input.spent_outpoint.is_coinbase_sentinel() {
                continue;
            }
            let outpoint_key = WalletOutpointKey::new(input.spent_outpoint);
            sink.push_event(
                outpoint_event_key(outpoint_key, SPEND_EVENT_TAG),
                &encode_spend_trailer(StagedSpend {
                    spent_at: transaction_position,
                    input_index: input.input_index,
                }),
            )?;
            counters.staged_spends = increment(counters.staged_spends)?;
            if !block_created_outpoints.contains(&outpoint_key) {
                retained_undo.add_spent_outpoint(outpoint_key)?;
            }
        }
        for (output_position, output) in transaction.transparent_outputs.iter().enumerate() {
            let expected_output_index = u32::try_from(output_position)
                .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
            if output.output_index != expected_output_index {
                return Err(WalletProjectionContractError::FactIndexMismatch.into());
            }
            let outpoint = zinder_core::TransparentOutPoint::new(
                transaction.public_facts.transaction_id,
                output.output_index,
            );
            let outpoint_key = WalletOutpointKey::new(outpoint);
            let wallet_output = StoredWalletUnspentOutput::new(
                outpoint,
                output.address_script_hash,
                output.value_zat,
                output.script_pub_key.clone(),
                transaction_position,
            )?;
            sink.push_event(
                outpoint_event_key(outpoint_key, OUTPUT_EVENT_TAG),
                &wallet_output.encode_value()?,
            )?;
            counters.staged_outputs = increment(counters.staged_outputs)?;
            if config.supported_reorg_depth != 0 && !block_created_outpoints.contains(&outpoint_key)
            {
                retained_undo
                    .reserve_transient(u64::try_from(size_of::<WalletOutpointKey>()).map_err(
                        |_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow,
                    )?)?;
                block_created_outpoints.insert(outpoint_key);
            }
            retained_undo.add_created_outpoint(outpoint_key)?;
        }
    }
    retained_undo.release_transient(
        u64::try_from(block_created_outpoints.len())
            .ok()
            .and_then(|count| {
                u64::try_from(size_of::<WalletOutpointKey>())
                    .ok()
                    .and_then(|bytes| count.checked_mul(bytes))
            })
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
    )?;
    retained_undo.finish_block()?;
    Ok(block)
}

#[derive(Default)]
struct PreparedFamilyFiles {
    paths: Vec<PathBuf>,
    file_bytes: u64,
}

impl From<SstFileSet> for PreparedFamilyFiles {
    fn from(file_set: SstFileSet) -> Self {
        Self {
            paths: file_set.paths,
            file_bytes: file_set.file_bytes,
        }
    }
}

fn combine_sst_file_sets(
    file_sets: impl IntoIterator<Item = SstFileSet>,
) -> Result<PreparedFamilyFiles, RocksDbWalletError> {
    let mut combined = PreparedFamilyFiles::default();
    for file_set in file_sets {
        combined.file_bytes = combined
            .file_bytes
            .checked_add(file_set.file_bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        combined.paths.extend(file_set.paths);
    }
    Ok(combined)
}

fn shard_staging_path(
    base_staging_path: &Path,
    phase_tag: &str,
    shard_index: usize,
    shard_count: usize,
) -> Result<PathBuf, RocksDbWalletError> {
    if shard_count <= 1 {
        return Ok(base_staging_path.to_path_buf());
    }
    let path = base_staging_path.join(format!("{phase_tag}-shard-{shard_index:03}"));
    fs::create_dir_all(&path).map_err(|source| RocksDbWalletError::PathUnavailable {
        path: path.clone(),
        source,
    })?;
    Ok(path)
}

struct OutpointDrainShardOutput {
    unspent_files: SstFileSet,
    spent_files: SstFileSet,
    unspent_write: Duration,
    spent_write: Duration,
    utxo_count: u64,
    total_value_zat: u64,
    accumulators: crate::accumulator_hashing::FinishedBranchAccumulators,
    evidence: WalletProjectionSstEvidence,
}

enum OutpointShardOutcome {
    Completed(Box<OutpointDrainShardOutput>),
    SiblingAborted,
    Failed(RocksDbWalletError),
}

struct OutpointDrainSummary {
    unspent_files: PreparedFamilyFiles,
    spent_files: PreparedFamilyFiles,
    unspent_write: Duration,
    spent_write: Duration,
    utxo_count: u64,
    total_value_zat: u64,
    sst_evidence: WalletProjectionSstEvidence,
}

#[allow(
    clippy::too_many_lines,
    reason = "the loader keeps the external drains and evidence fence in visible order"
)]
fn finish_projection_ssts_with_plan(
    config: WalletProjectionLoadConfig<'_>,
    scanned: CompletedCanonicalScan,
    shard_plan: BuildShardPlan,
) -> Result<PreparedWalletProjectionLoad, RocksDbWalletError> {
    let CompletedCanonicalScan {
        first_block,
        tip,
        source_sequence_digest,
        mut retained_undo,
        scanned_block_count,
        scanned_transaction_count,
        staged_output_count,
        staged_spend_count,
        canonical_scan,
        outpoint_sort,
        sorted_outpoints,
        outpoint_sort_evidence,
        outpoint_sort_durations,
        block_hash_files,
        block_hash_accumulators,
        block_hash_evidence,
        #[cfg(test)]
            block_hash_write_duration: _,
        #[cfg(test)]
            block_hash_finish_duration: _,
    } = scanned;

    let phase_started = Instant::now();
    let address_transaction_sort_memory = address_transaction_sort_memory_bytes(
        config.max_outpoint_sort_memory_bytes,
        config.max_secondary_sort_memory_bytes_per_sorter,
    );
    let address_index_sorter =
        ShardedKeyPrefixSorter::<ADDRESS_UNSPENT_KEY_BYTES>::new_with_budget(
            config.staging_path,
            "wallet-address-unspent",
            ShardedSortBudget {
                shard_count: shard_plan.address_index_push_shards,
                max_accounted_memory_bytes: config.max_secondary_sort_memory_bytes_per_sorter,
                max_temporary_file_bytes: config.max_temporary_file_bytes_per_sorter,
                // Descriptor fan-in is resolved at the start of secondary derivation once
                // `sorted_outpoints` closes its open run readers.
                per_shard_merge_fan_in_cap: None,
            },
        )?;
    let address_transaction_sorter =
        ShardedKeyPrefixSorter::<ADDRESS_TRANSACTION_KEY_BYTES>::new_with_budget(
            config.staging_path,
            "wallet-address-transactions",
            ShardedSortBudget {
                shard_count: shard_plan.address_transaction_shards,
                max_accounted_memory_bytes: address_transaction_sort_memory,
                max_temporary_file_bytes: config.max_temporary_file_bytes_per_sorter,
                per_shard_merge_fan_in_cap: None,
            },
        )?;
    let mut accumulators =
        ConstructionAccumulators::start(config.network, config.accumulator_hashing_workers)?;
    accumulators.combine_branch(&block_hash_accumulators)?;
    let branch_pool = accumulators.take_branch_hashing_pool()?;
    let outpoint_summary = drain_outpoint_shards(
        config,
        sorted_outpoints,
        (&address_index_sorter, &address_transaction_sorter),
        branch_pool.as_ref(),
        &mut accumulators,
    )?;
    let mut sst_evidence = outpoint_summary.sst_evidence;
    sst_evidence.merge(&block_hash_evidence)?;
    let outpoint_merge = phase_started.elapsed();

    let phase_started = Instant::now();
    let secondary = derive_secondary_address_artifacts(
        config,
        SecondaryDerivationSorters {
            shard_plan,
            address_index: address_index_sorter,
            address_transaction: address_transaction_sorter,
        },
        &mut retained_undo,
        SecondaryAccumulators {
            branch_pool: branch_pool.as_ref(),
            accumulators: &mut accumulators,
            sst_evidence: &mut sst_evidence,
        },
    )?;
    if let Some(pool) = branch_pool {
        accumulators.restore_branch_hashing_pool(pool);
    }

    let step_started = Instant::now();
    let mut reorg_undo_writer = ordered_writer(
        config,
        WalletProjectionRowFamily::ReorgUndo,
        "wallet-reorg-undo",
    )?;
    for mut undo in retained_undo.records {
        undo.address_transaction_keys.sort_unstable();
        let key = undo.encode_key();
        let encoded_value = undo.encode_value()?;
        append_row(
            &mut reorg_undo_writer,
            &mut accumulators,
            &mut sst_evidence,
            WalletProjectionRowFamily::ReorgUndo,
            &key,
            &encoded_value,
        )?;
    }
    let (reorg_undo_files, reorg_undo_write) = reorg_undo_writer.finish()?;
    let reorg_undo_write_step = step_started.elapsed();
    let secondary_row_derivation = phase_started.elapsed();

    let phase_started = Instant::now();
    let (digest, commitment) = accumulators.finish()?;
    let row_counts = digest.row_counts();
    let (projection_accumulator, projection_digest) = digest.finish_with_accumulator();
    let logical_evidence = phase_started.elapsed();
    let artifacts = [
        (
            TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
            outpoint_summary.unspent_files,
        ),
        (
            TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
            PreparedFamilyFiles::from(secondary.address_index_files),
        ),
        (
            TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
            outpoint_summary.spent_files,
        ),
        (
            TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
            secondary.address_transaction_files,
        ),
        (
            TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
            PreparedFamilyFiles::from(secondary.address_balance_files),
        ),
        (
            REORG_UNDO_COLUMN_FAMILY,
            PreparedFamilyFiles::from(reorg_undo_files),
        ),
        (
            BLOCK_HASH_BY_HEIGHT_COLUMN_FAMILY,
            PreparedFamilyFiles::from(block_hash_files),
        ),
    ];
    let (families, sst_file_bytes, sst_file_count) = prepare_families(artifacts)?;

    Ok(PreparedWalletProjectionLoad {
        network: config.network,
        supported_reorg_depth: config.supported_reorg_depth,
        first_block,
        tip,
        source_sequence_digest,
        projection_digest,
        projection_accumulator,
        row_counts,
        utxo_summary: WalletUtxoSetSummary {
            utxo_count: outpoint_summary.utxo_count,
            total_value_zat: outpoint_summary.total_value_zat,
            commitment,
        },
        counters: WalletProjectionLoadCounters {
            scanned_block_count,
            scanned_transaction_count,
            staged_output_count,
            staged_spend_count,
            historical_prevout_read_count: 0,
            peak_accounted_reorg_undo_bytes: retained_undo.peak_accounted_bytes,
            max_accounted_reorg_undo_bytes: retained_undo.max_accounted_bytes,
        },
        phase_durations: WalletProjectionLoadPhaseDurations {
            canonical_scan,
            outpoint_sort,
            outpoint_merge,
            secondary_row_derivation,
            secondary_row_derivation_phases: WalletSecondaryRowDerivationPhaseDurations {
                address_index_sort: secondary.address_index_sort,
                address_index_drain: secondary.address_index_drain,
                address_transaction_sort: secondary.address_transaction_sort,
                address_transaction_drain: secondary.address_transaction_drain,
                concurrent_address_derivation: secondary.concurrent_address_derivation,
                reorg_undo_write: reorg_undo_write_step,
            },
            sst_write: WalletSstWriteDurations {
                primary_outputs: outpoint_summary
                    .unspent_write
                    .saturating_add(outpoint_summary.spent_write),
                address_index_and_balance: secondary
                    .address_index_write
                    .saturating_add(secondary.address_balance_write),
                address_transaction: secondary.address_transaction_write,
                reorg_undo: reorg_undo_write,
            },
            logical_evidence,
        },
        outpoint_sort_evidence,
        address_index_sort_evidence: secondary.address_index_sort_evidence,
        address_transaction_sort_evidence: secondary.address_transaction_sort_evidence,
        outpoint_sort_durations,
        address_index_sort_durations: secondary.address_index_sort_durations,
        address_transaction_sort_durations: secondary.address_transaction_sort_durations,
        logical_row_bytes: sst_evidence.logical_row_bytes,
        sst_file_bytes,
        sst_file_count,
        families,
    })
}

struct OutpointShardSpec {
    shard_index: usize,
    shard_count: usize,
    branch_hashing: Option<crate::accumulator_hashing::BranchHashingConfig>,
}

fn drain_outpoint_shards(
    config: WalletProjectionLoadConfig<'_>,
    sorted_outpoints: ShardedSortedVariableValues<OUTPOINT_EVENT_KEY_BYTES>,
    secondary_sorters: (
        &ShardedKeyPrefixSorter<ADDRESS_UNSPENT_KEY_BYTES>,
        &ShardedKeyPrefixSorter<ADDRESS_TRANSACTION_KEY_BYTES>,
    ),
    branch_pool: Option<&crate::accumulator_hashing::BranchHashingPool>,
    accumulators: &mut ConstructionAccumulators,
) -> Result<OutpointDrainSummary, RocksDbWalletError> {
    let shards = sorted_outpoints.into_shards();
    let shard_count = shards.len();
    let aborted = AtomicBool::new(false);
    if shard_count <= 1 {
        let Some(stream) = shards.into_iter().next() else {
            return Err(RocksDbWalletError::CanonicalScanWorkerStopped);
        };
        let branch_hashing = branch_pool.and_then(|pool| pool.split_many(1).pop().flatten());
        let output = match drain_single_outpoint_shard(
            config,
            stream,
            OutpointShardSpec {
                shard_index: 0,
                shard_count: 1,
                branch_hashing,
            },
            secondary_sorters,
            &aborted,
        ) {
            OutpointShardOutcome::Completed(completed) => *completed,
            OutpointShardOutcome::Failed(error) => return Err(error),
            OutpointShardOutcome::SiblingAborted => {
                return Err(RocksDbWalletError::CanonicalScanWorkerStopped);
            }
        };
        return combine_outpoint_shard_outputs(config, vec![output], accumulators);
    }

    let branch_hashing = branch_pool.map_or_else(
        || (0..shard_count).map(|_| None).collect(),
        |pool| pool.split_many(shard_count),
    );
    let outcomes =
        run_parallel_outpoint_shards(config, shards, secondary_sorters, branch_hashing, &aborted)?;
    combine_outpoint_shard_outputs(config, outcomes, accumulators)
}

fn run_parallel_outpoint_shards(
    config: WalletProjectionLoadConfig<'_>,
    shards: Vec<SortedVariableValues<OUTPOINT_EVENT_KEY_BYTES>>,
    secondary_sorters: (
        &ShardedKeyPrefixSorter<ADDRESS_UNSPENT_KEY_BYTES>,
        &ShardedKeyPrefixSorter<ADDRESS_TRANSACTION_KEY_BYTES>,
    ),
    branch_hashing: Vec<Option<crate::accumulator_hashing::BranchHashingConfig>>,
    aborted: &AtomicBool,
) -> Result<Vec<OutpointDrainShardOutput>, RocksDbWalletError> {
    let shard_count = shards.len();
    let shard_configs = branch_hashing
        .into_iter()
        .chain(std::iter::repeat_with(|| None));
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(shard_count);
        let mut start_error = None;
        for (shard_index, (stream, shard_hashing)) in
            shards.into_iter().zip(shard_configs).enumerate()
        {
            match std::thread::Builder::new()
                .name(format!("zinder-outpoint-{shard_index}"))
                .spawn_scoped(scope, move || {
                    drain_single_outpoint_shard(
                        config,
                        stream,
                        OutpointShardSpec {
                            shard_index,
                            shard_count,
                            branch_hashing: shard_hashing,
                        },
                        secondary_sorters,
                        aborted,
                    )
                }) {
                Ok(handle) => handles.push(handle),
                Err(source) => {
                    aborted.store(true, Ordering::Release);
                    start_error = Some(RocksDbWalletError::CanonicalScanThreadUnavailable {
                        reason: source.to_string(),
                    });
                    break;
                }
            }
        }
        let mut completed = Vec::with_capacity(shard_count);
        let mut first_error = start_error;
        let mut worker_stopped = false;
        for handle in handles {
            match handle.join() {
                Ok(OutpointShardOutcome::Completed(output)) => completed.push(*output),
                Ok(OutpointShardOutcome::Failed(error)) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Ok(OutpointShardOutcome::SiblingAborted) | Err(_) => worker_stopped = true,
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        if worker_stopped {
            return Err(RocksDbWalletError::CanonicalScanWorkerStopped);
        }
        Ok(completed)
    })
}

fn drain_single_outpoint_shard(
    config: WalletProjectionLoadConfig<'_>,
    stream: SortedVariableValues<OUTPOINT_EVENT_KEY_BYTES>,
    shard_spec: OutpointShardSpec,
    secondary_sorters: (
        &ShardedKeyPrefixSorter<ADDRESS_UNSPENT_KEY_BYTES>,
        &ShardedKeyPrefixSorter<ADDRESS_TRANSACTION_KEY_BYTES>,
    ),
    aborted: &AtomicBool,
) -> OutpointShardOutcome {
    let _abort_on_unwind = SetAbortOnUnwind(aborted);
    if aborted.load(Ordering::Acquire) {
        return OutpointShardOutcome::SiblingAborted;
    }
    match run_outpoint_shard_pipeline(config, stream, shard_spec, secondary_sorters, aborted) {
        Ok(Some(completed)) => OutpointShardOutcome::Completed(Box::new(completed)),
        Ok(None) => OutpointShardOutcome::SiblingAborted,
        Err(error) => {
            aborted.store(true, Ordering::Release);
            OutpointShardOutcome::Failed(error)
        }
    }
}

fn run_outpoint_shard_pipeline(
    config: WalletProjectionLoadConfig<'_>,
    mut stream: SortedVariableValues<OUTPOINT_EVENT_KEY_BYTES>,
    shard_spec: OutpointShardSpec,
    secondary_sorters: (
        &ShardedKeyPrefixSorter<ADDRESS_UNSPENT_KEY_BYTES>,
        &ShardedKeyPrefixSorter<ADDRESS_TRANSACTION_KEY_BYTES>,
    ),
    aborted: &AtomicBool,
) -> Result<Option<OutpointDrainShardOutput>, RocksDbWalletError> {
    let OutpointShardSpec {
        shard_index,
        shard_count,
        branch_hashing,
    } = shard_spec;
    let (address_index_sorter, address_transaction_sorter) = secondary_sorters;
    let shard_staging =
        shard_staging_path(config.staging_path, "outpoint", shard_index, shard_count)?;
    let active_writers = shard_count.saturating_mul(2);
    let mut unspent_writer = shard_projection_writer(
        config,
        &shard_staging,
        WalletProjectionRowFamily::TransparentUnspentOutput,
        "wallet-unspent-output",
        active_writers,
    )?;
    let mut spent_writer = shard_projection_writer(
        config,
        &shard_staging,
        WalletProjectionRowFamily::TransparentSpentOutput,
        "wallet-spent-output",
        active_writers,
    )?;
    let mut accumulators =
        ConstructionAccumulators::start_optional_branch(config.network, branch_hashing);
    let writer_count =
        NonZeroU32::new(u32::try_from(shard_count).unwrap_or(1)).unwrap_or(NonZeroU32::MIN);
    let mut address_index_writer = address_index_sorter.writer(writer_count);
    let mut address_transaction_writer = address_transaction_sorter.writer(writer_count);
    let mut evidence = WalletProjectionSstEvidence::default();
    let mut utxo_count = 0_u64;
    let mut total_value_zat = 0_u64;
    let mut targets = OutpointDrainTargets {
        unspent_writer: &mut unspent_writer,
        spent_writer: &mut spent_writer,
        address_index_writer: &mut address_index_writer,
        address_transaction_writer: &mut address_transaction_writer,
        accumulators: &mut accumulators,
        sst_evidence: &mut evidence,
        utxo_count: &mut utxo_count,
        total_value_zat: &mut total_value_zat,
        aborted,
    };
    if !drain_outpoint_events(&mut stream, &mut targets)? {
        return Ok(None);
    }
    address_index_writer.flush()?;
    address_transaction_writer.flush()?;
    let finished_accumulators = accumulators.finish_branch()?;
    let (unspent_files, unspent_write) = unspent_writer.finish()?;
    let (spent_files, spent_write) = spent_writer.finish()?;
    Ok(Some(OutpointDrainShardOutput {
        unspent_files,
        spent_files,
        unspent_write,
        spent_write,
        utxo_count,
        total_value_zat,
        accumulators: finished_accumulators,
        evidence,
    }))
}

fn combine_outpoint_shard_outputs(
    _config: WalletProjectionLoadConfig<'_>,
    outputs: Vec<OutpointDrainShardOutput>,
    accumulators: &mut ConstructionAccumulators,
) -> Result<OutpointDrainSummary, RocksDbWalletError> {
    let mut unspent_sets = Vec::with_capacity(outputs.len());
    let mut spent_sets = Vec::with_capacity(outputs.len());
    let mut unspent_write = Duration::ZERO;
    let mut spent_write = Duration::ZERO;
    let mut utxo_count = 0_u64;
    let mut total_value_zat = 0_u64;
    let mut sst_evidence = WalletProjectionSstEvidence::default();
    for output in outputs {
        unspent_sets.push(output.unspent_files);
        spent_sets.push(output.spent_files);
        if output.unspent_write.saturating_add(output.spent_write)
            > unspent_write.saturating_add(spent_write)
        {
            unspent_write = output.unspent_write;
            spent_write = output.spent_write;
        }
        utxo_count = add_counter(utxo_count, output.utxo_count)?;
        total_value_zat = total_value_zat
            .checked_add(output.total_value_zat)
            .ok_or(WalletProjectionContractError::UtxoValueOverflow)?;
        accumulators.combine_branch(&output.accumulators)?;
        sst_evidence.merge(&output.evidence)?;
    }
    accumulators.settle()?;
    Ok(OutpointDrainSummary {
        unspent_files: combine_sst_file_sets(unspent_sets)?,
        spent_files: combine_sst_file_sets(spent_sets)?,
        unspent_write,
        spent_write,
        utxo_count,
        total_value_zat,
        sst_evidence,
    })
}

struct SecondaryDerivationSorters {
    shard_plan: BuildShardPlan,
    address_index: ShardedKeyPrefixSorter<ADDRESS_UNSPENT_KEY_BYTES>,
    address_transaction: ShardedKeyPrefixSorter<ADDRESS_TRANSACTION_KEY_BYTES>,
}

struct AddressIndexBranchOutput {
    sort_evidence: VariableValueSortEvidence,
    sort_durations: VariableValueSortDurations,
    sort_duration: Duration,
    drain_duration: Duration,
    index_files: SstFileSet,
    index_write: Duration,
    balance_files: SstFileSet,
    balance_write: Duration,
    accumulators: crate::accumulator_hashing::FinishedBranchAccumulators,
    evidence: WalletProjectionSstEvidence,
}

struct AddressTransactionBranchOutput {
    sort_evidence: VariableValueSortEvidence,
    sort_durations: VariableValueSortDurations,
    sort_duration: Duration,
    drain_duration: Duration,
    transaction_files: PreparedFamilyFiles,
    transaction_write: Duration,
    accumulators: crate::accumulator_hashing::FinishedBranchAccumulators,
    evidence: WalletProjectionSstEvidence,
}

struct AddressTransactionShardOutput {
    transaction_files: SstFileSet,
    transaction_write: Duration,
    accumulators: crate::accumulator_hashing::FinishedBranchAccumulators,
    evidence: WalletProjectionSstEvidence,
}

struct SecondaryAddressDerivationArtifacts {
    concurrent_address_derivation: Duration,
    address_index_sort: Duration,
    address_index_drain: Duration,
    address_index_sort_evidence: VariableValueSortEvidence,
    address_index_sort_durations: VariableValueSortDurations,
    address_index_files: SstFileSet,
    address_index_write: Duration,
    address_balance_files: SstFileSet,
    address_balance_write: Duration,
    address_transaction_sort: Duration,
    address_transaction_drain: Duration,
    address_transaction_sort_evidence: VariableValueSortEvidence,
    address_transaction_sort_durations: VariableValueSortDurations,
    address_transaction_files: PreparedFamilyFiles,
    address_transaction_write: Duration,
}

enum SecondaryBranchOutcome<T> {
    Completed(Box<T>),
    SiblingAborted,
    Failed(RocksDbWalletError),
}

struct SetAbortOnUnwind<'a>(&'a AtomicBool);

impl Drop for SetAbortOnUnwind<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Release);
        }
    }
}

struct SecondaryAccumulators<'a> {
    branch_pool: Option<&'a crate::accumulator_hashing::BranchHashingPool>,
    accumulators: &'a mut ConstructionAccumulators,
    sst_evidence: &'a mut WalletProjectionSstEvidence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SecondaryPhaseFanIn {
    overlap_secondary: bool,
    address_index_per_shard_fan_in: usize,
    address_transaction_per_shard_fan_in: usize,
}

fn invalid_secondary_fan_in(reason: &str) -> RocksDbWalletError {
    BulkLoadError::InvalidInput {
        reason: reason.to_owned(),
    }
    .into()
}

fn resolve_secondary_phase_fan_in(
    address_index_push_shards: NonZeroU32,
    address_transaction_shards: NonZeroU32,
    overlap_requested: bool,
    active_fan_in: usize,
) -> Result<SecondaryPhaseFanIn, RocksDbWalletError> {
    const MIN_FAN_IN: usize = 2;
    let idx_shards = usize::try_from(address_index_push_shards.get())
        .map_err(|_| invalid_secondary_fan_in("address index shard count exceeds usize::MAX"))?;
    let tx_shards = usize::try_from(address_transaction_shards.get()).map_err(|_| {
        invalid_secondary_fan_in("address transaction shard count exceeds usize::MAX")
    })?;
    let min_idx_total = idx_shards
        .checked_mul(MIN_FAN_IN)
        .ok_or_else(|| invalid_secondary_fan_in("address index fan-in overflows usize"))?;
    let min_tx_total = tx_shards
        .checked_mul(MIN_FAN_IN)
        .ok_or_else(|| invalid_secondary_fan_in("address transaction fan-in overflows usize"))?;
    if overlap_requested && min_idx_total.saturating_add(min_tx_total) <= active_fan_in {
        let idx_budget = (active_fan_in / 4)
            .max(min_idx_total)
            .min(active_fan_in.saturating_sub(min_tx_total));
        let tx_budget = active_fan_in.saturating_sub(idx_budget);
        return Ok(SecondaryPhaseFanIn {
            overlap_secondary: true,
            address_index_per_shard_fan_in: (idx_budget / idx_shards).max(MIN_FAN_IN),
            address_transaction_per_shard_fan_in: (tx_budget / tx_shards).max(MIN_FAN_IN),
        });
    }
    let idx_per_shard = active_fan_in / idx_shards.max(1);
    let tx_per_shard = active_fan_in / tx_shards.max(1);
    if idx_per_shard < MIN_FAN_IN || tx_per_shard < MIN_FAN_IN {
        return Err(invalid_secondary_fan_in(
            "secondary sharded sort exceeds active merge file-descriptor fan-in",
        ));
    }
    Ok(SecondaryPhaseFanIn {
        overlap_secondary: false,
        address_index_per_shard_fan_in: idx_per_shard,
        address_transaction_per_shard_fan_in: tx_per_shard,
    })
}

struct ConcurrentSecondaryHashing {
    index: Option<crate::accumulator_hashing::BranchHashingConfig>,
    tx: Vec<Option<crate::accumulator_hashing::BranchHashingConfig>>,
    active_writers: usize,
    fan_in: SecondaryPhaseFanIn,
}

struct AddressIndexBranchBudget {
    branch_hashing: Option<crate::accumulator_hashing::BranchHashingConfig>,
    active_writers: usize,
    merge_fan_in: usize,
}

struct AddressTxHashingBudget {
    configs: Vec<Option<crate::accumulator_hashing::BranchHashingConfig>>,
    active_writers: usize,
    merge_fan_in: usize,
}

struct AddressTxShardSpec {
    shard_index: usize,
    shard_count: usize,
    active_writers: usize,
    branch_hashing: Option<crate::accumulator_hashing::BranchHashingConfig>,
}

fn derive_secondary_address_artifacts(
    config: WalletProjectionLoadConfig<'_>,
    sorters: SecondaryDerivationSorters,
    retained_undo: &mut RetainedUndo,
    shared: SecondaryAccumulators<'_>,
) -> Result<SecondaryAddressDerivationArtifacts, RocksDbWalletError> {
    let SecondaryAccumulators {
        branch_pool,
        accumulators,
        sst_evidence,
    } = shared;
    let step_started = Instant::now();
    let aborted = AtomicBool::new(false);
    let undo_collector = RetainedUndoCollector::from_retained_undo(retained_undo);
    let tx_shards_usize = usize::try_from(sorters.shard_plan.address_transaction_shards.get())
        .unwrap_or(1)
        .max(1);
    let secondary_fan_in = resolve_secondary_phase_fan_in(
        sorters.shard_plan.address_index_push_shards,
        sorters.shard_plan.address_transaction_shards,
        sorters.shard_plan.overlap_secondary,
        active_merge_fan_in(),
    )?;
    let (index_branch, tx_branch) = if secondary_fan_in.overlap_secondary {
        let active_writers = 2_usize.saturating_add(tx_shards_usize);
        let (index_hashing, tx_hashing) = branch_pool.map_or_else(
            || (None, (0..tx_shards_usize).map(|_| None).collect()),
            |pool| pool.split_secondary(tx_shards_usize),
        );
        run_concurrent_secondary_branches(
            config,
            sorters,
            &undo_collector,
            ConcurrentSecondaryHashing {
                index: index_hashing,
                tx: tx_hashing,
                active_writers,
                fan_in: secondary_fan_in,
            },
            &aborted,
        )?
    } else {
        run_sequential_secondary_branches(
            config,
            sorters,
            &undo_collector,
            (tx_shards_usize, secondary_fan_in),
            &aborted,
        )?
    };
    undo_collector.apply_collected(retained_undo)?;
    accumulators.combine_branch(&index_branch.accumulators)?;
    accumulators.combine_branch(&tx_branch.accumulators)?;
    sst_evidence.merge(&index_branch.evidence)?;
    sst_evidence.merge(&tx_branch.evidence)?;
    let concurrent_address_derivation = step_started.elapsed();
    Ok(SecondaryAddressDerivationArtifacts {
        concurrent_address_derivation,
        address_index_sort: index_branch.sort_duration,
        address_index_drain: index_branch.drain_duration,
        address_index_sort_evidence: index_branch.sort_evidence,
        address_index_sort_durations: index_branch.sort_durations,
        address_index_files: index_branch.index_files,
        address_index_write: index_branch.index_write,
        address_balance_files: index_branch.balance_files,
        address_balance_write: index_branch.balance_write,
        address_transaction_sort: tx_branch.sort_duration,
        address_transaction_drain: tx_branch.drain_duration,
        address_transaction_sort_evidence: tx_branch.sort_evidence,
        address_transaction_sort_durations: tx_branch.sort_durations,
        address_transaction_files: tx_branch.transaction_files,
        address_transaction_write: tx_branch.transaction_write,
    })
}

fn run_sequential_secondary_branches(
    config: WalletProjectionLoadConfig<'_>,
    sorters: SecondaryDerivationSorters,
    undo_collector: &RetainedUndoCollector,
    tx_budget: (usize, SecondaryPhaseFanIn),
    aborted: &AtomicBool,
) -> Result<
    (
        Box<AddressIndexBranchOutput>,
        Box<AddressTransactionBranchOutput>,
    ),
    RocksDbWalletError,
> {
    let (tx_shards_usize, secondary_fan_in) = tx_budget;
    let index_branch = match run_address_index_branch(
        config,
        sorters.address_index,
        AddressIndexBranchBudget {
            branch_hashing: None,
            active_writers: 2,
            merge_fan_in: secondary_fan_in.address_index_per_shard_fan_in,
        },
        aborted,
    ) {
        SecondaryBranchOutcome::Completed(completed) => completed,
        SecondaryBranchOutcome::Failed(error) => return Err(error),
        SecondaryBranchOutcome::SiblingAborted => {
            return Err(RocksDbWalletError::SecondaryDerivationWorkerStopped);
        }
    };
    let tx_branch = match run_address_transaction_branch(
        config,
        sorters.address_transaction,
        undo_collector,
        AddressTxHashingBudget {
            configs: (0..tx_shards_usize).map(|_| None).collect(),
            active_writers: tx_shards_usize,
            merge_fan_in: secondary_fan_in.address_transaction_per_shard_fan_in,
        },
        aborted,
    ) {
        SecondaryBranchOutcome::Completed(completed) => completed,
        SecondaryBranchOutcome::Failed(error) => return Err(error),
        SecondaryBranchOutcome::SiblingAborted => {
            return Err(RocksDbWalletError::SecondaryDerivationWorkerStopped);
        }
    };
    Ok((index_branch, tx_branch))
}

fn run_concurrent_secondary_branches(
    config: WalletProjectionLoadConfig<'_>,
    sorters: SecondaryDerivationSorters,
    undo_collector: &RetainedUndoCollector,
    hashing: ConcurrentSecondaryHashing,
    aborted: &AtomicBool,
) -> Result<
    (
        Box<AddressIndexBranchOutput>,
        Box<AddressTransactionBranchOutput>,
    ),
    RocksDbWalletError,
> {
    let ConcurrentSecondaryHashing {
        index: index_hashing,
        tx: tx_hashing,
        active_writers,
        fan_in,
    } = hashing;
    std::thread::scope(|scope| {
        let tx_handle = std::thread::Builder::new()
            .name("zinder-addr-tx".to_owned())
            .spawn_scoped(scope, || {
                run_address_transaction_branch(
                    config,
                    sorters.address_transaction,
                    undo_collector,
                    AddressTxHashingBudget {
                        configs: tx_hashing,
                        active_writers,
                        merge_fan_in: fan_in.address_transaction_per_shard_fan_in,
                    },
                    aborted,
                )
            })
            .map_err(
                |source| RocksDbWalletError::SecondaryDerivationThreadUnavailable {
                    reason: source.to_string(),
                },
            )?;
        let index_outcome = run_address_index_branch(
            config,
            sorters.address_index,
            AddressIndexBranchBudget {
                branch_hashing: index_hashing,
                active_writers,
                merge_fan_in: fan_in.address_index_per_shard_fan_in,
            },
            aborted,
        );
        let tx_join = tx_handle.join();
        resolve_secondary_branch_outcomes(index_outcome, tx_join)
    })
}

fn resolve_secondary_branch_outcomes(
    index_outcome: SecondaryBranchOutcome<AddressIndexBranchOutput>,
    tx_join: std::thread::Result<SecondaryBranchOutcome<AddressTransactionBranchOutput>>,
) -> Result<
    (
        Box<AddressIndexBranchOutput>,
        Box<AddressTransactionBranchOutput>,
    ),
    RocksDbWalletError,
> {
    match (index_outcome, tx_join) {
        (SecondaryBranchOutcome::Failed(error), _)
        | (_, Ok(SecondaryBranchOutcome::Failed(error))) => Err(error),
        (_, Err(_)) => Err(RocksDbWalletError::SecondaryDerivationWorkerStopped),
        (
            SecondaryBranchOutcome::Completed(index_branch),
            Ok(SecondaryBranchOutcome::Completed(tx_branch)),
        ) => Ok((index_branch, tx_branch)),
        (SecondaryBranchOutcome::SiblingAborted, Ok(_))
        | (_, Ok(SecondaryBranchOutcome::SiblingAborted)) => {
            Err(RocksDbWalletError::SecondaryDerivationWorkerStopped)
        }
    }
}

fn run_address_index_branch(
    config: WalletProjectionLoadConfig<'_>,
    sorter: ShardedKeyPrefixSorter<ADDRESS_UNSPENT_KEY_BYTES>,
    budget: AddressIndexBranchBudget,
    aborted: &AtomicBool,
) -> SecondaryBranchOutcome<AddressIndexBranchOutput> {
    let _abort_on_unwind = SetAbortOnUnwind(aborted);
    if aborted.load(Ordering::Acquire) {
        return SecondaryBranchOutcome::SiblingAborted;
    }
    match run_address_index_branch_inner(config, sorter, budget, aborted) {
        Ok(Some(completed)) => SecondaryBranchOutcome::Completed(Box::new(completed)),
        Ok(None) => SecondaryBranchOutcome::SiblingAborted,
        Err(error) => {
            aborted.store(true, Ordering::Release);
            SecondaryBranchOutcome::Failed(error)
        }
    }
}

fn run_address_index_branch_inner(
    config: WalletProjectionLoadConfig<'_>,
    sorter: ShardedKeyPrefixSorter<ADDRESS_UNSPENT_KEY_BYTES>,
    budget: AddressIndexBranchBudget,
    aborted: &AtomicBool,
) -> Result<Option<AddressIndexBranchOutput>, RocksDbWalletError> {
    let AddressIndexBranchBudget {
        branch_hashing,
        active_writers,
        merge_fan_in,
    } = budget;
    let sort_started = Instant::now();
    let sorted_rows = sorter.finish_with_per_shard_merge_fan_in(merge_fan_in)?;
    let sort_evidence = sorted_rows.evidence();
    let sort_durations = sorted_rows.durations();
    let sort_duration = sort_started.elapsed();
    if aborted.load(Ordering::Acquire) {
        return Ok(None);
    }
    let drain_started = Instant::now();
    let mut index_writer = shard_projection_writer(
        config,
        config.staging_path,
        WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
        "wallet-address-unspent-index",
        active_writers,
    )?;
    let mut balance_writer = shard_projection_writer(
        config,
        config.staging_path,
        WalletProjectionRowFamily::TransparentAddressBalance,
        "wallet-address-balance",
        active_writers,
    )?;
    let mut branch_accumulators =
        ConstructionAccumulators::start_optional_branch(config.network, branch_hashing);
    let mut branch_evidence = WalletProjectionSstEvidence::default();
    let mut targets = AddressIndexTargets {
        index_writer: &mut index_writer,
        balance_writer: &mut balance_writer,
        accumulators: &mut branch_accumulators,
        evidence: &mut branch_evidence,
        aborted,
    };
    for mut shard_stream in sorted_rows.into_shards() {
        if !drain_address_index(&mut shard_stream, &mut targets)? {
            return Ok(None);
        }
    }
    let finished_accumulators = branch_accumulators.finish_branch()?;
    let (index_files, index_write) = index_writer.finish()?;
    let (balance_files, balance_write) = balance_writer.finish()?;
    let drain_duration = drain_started.elapsed();
    Ok(Some(AddressIndexBranchOutput {
        sort_evidence,
        sort_durations,
        sort_duration,
        drain_duration,
        index_files,
        index_write,
        balance_files,
        balance_write,
        accumulators: finished_accumulators,
        evidence: branch_evidence,
    }))
}

struct AddressTxShardRunMeta {
    hashing_configs: Vec<Option<crate::accumulator_hashing::BranchHashingConfig>>,
    active_writers: usize,
    sort_evidence: VariableValueSortEvidence,
    sort_durations: VariableValueSortDurations,
    sort_duration: Duration,
    drain_started: Instant,
}

fn run_address_transaction_branch(
    config: WalletProjectionLoadConfig<'_>,
    sorter: ShardedKeyPrefixSorter<ADDRESS_TRANSACTION_KEY_BYTES>,
    undo_collector: &RetainedUndoCollector,
    hashing_budget: AddressTxHashingBudget,
    aborted: &AtomicBool,
) -> SecondaryBranchOutcome<AddressTransactionBranchOutput> {
    let _abort_on_unwind = SetAbortOnUnwind(aborted);
    if aborted.load(Ordering::Acquire) {
        return SecondaryBranchOutcome::SiblingAborted;
    }
    let AddressTxHashingBudget {
        configs: hashing_configs,
        active_writers,
        merge_fan_in,
    } = hashing_budget;
    let sort_started = Instant::now();
    let sorted_rows = match sorter.finish_with_per_shard_merge_fan_in(merge_fan_in) {
        Ok(finished_rows) => finished_rows,
        Err(error) => {
            aborted.store(true, Ordering::Release);
            return SecondaryBranchOutcome::Failed(error.into());
        }
    };
    let sort_evidence = sorted_rows.evidence();
    let sort_durations = sorted_rows.durations();
    let sort_duration = sort_started.elapsed();
    if aborted.load(Ordering::Acquire) {
        return SecondaryBranchOutcome::SiblingAborted;
    }
    let drain_started = Instant::now();
    let shards = sorted_rows.into_shards();
    let shard_count = shards.len();
    if shard_count <= 1 {
        let Some(stream) = shards.into_iter().next() else {
            return SecondaryBranchOutcome::Failed(
                RocksDbWalletError::SecondaryDerivationWorkerStopped,
            );
        };
        let branch_hashing = hashing_configs.into_iter().next().flatten();
        return match drain_single_address_tx_shard(
            config,
            stream,
            undo_collector,
            AddressTxShardSpec {
                shard_index: 0,
                shard_count: 1,
                active_writers,
                branch_hashing,
            },
            aborted,
        ) {
            SecondaryBranchOutcome::Completed(shard_out) => {
                let drain_duration = drain_started.elapsed();
                SecondaryBranchOutcome::Completed(Box::new(AddressTransactionBranchOutput {
                    sort_evidence,
                    sort_durations,
                    sort_duration,
                    drain_duration,
                    transaction_files: PreparedFamilyFiles::from(shard_out.transaction_files),
                    transaction_write: shard_out.transaction_write,
                    accumulators: shard_out.accumulators,
                    evidence: shard_out.evidence,
                }))
            }
            SecondaryBranchOutcome::SiblingAborted => SecondaryBranchOutcome::SiblingAborted,
            SecondaryBranchOutcome::Failed(error) => {
                aborted.store(true, Ordering::Release);
                SecondaryBranchOutcome::Failed(error)
            }
        };
    }

    run_parallel_address_tx_shards(
        config,
        shards,
        undo_collector,
        AddressTxShardRunMeta {
            hashing_configs,
            active_writers,
            sort_evidence,
            sort_durations,
            sort_duration,
            drain_started,
        },
        aborted,
    )
}

fn run_parallel_address_tx_shards(
    config: WalletProjectionLoadConfig<'_>,
    shards: Vec<SortedVariableValues<ADDRESS_TRANSACTION_KEY_BYTES>>,
    undo_collector: &RetainedUndoCollector,
    meta: AddressTxShardRunMeta,
    aborted: &AtomicBool,
) -> SecondaryBranchOutcome<AddressTransactionBranchOutput> {
    let shard_count = shards.len();
    let active_writers = meta.active_writers;
    let shard_configs = meta
        .hashing_configs
        .into_iter()
        .chain(std::iter::repeat_with(|| None));
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(shard_count);
        let mut start_error = None;
        for (shard_index, (stream, branch_hashing)) in
            shards.into_iter().zip(shard_configs).enumerate()
        {
            match std::thread::Builder::new()
                .name(format!("zinder-addr-tx-{shard_index}"))
                .spawn_scoped(scope, move || {
                    drain_single_address_tx_shard(
                        config,
                        stream,
                        undo_collector,
                        AddressTxShardSpec {
                            shard_index,
                            shard_count,
                            active_writers,
                            branch_hashing,
                        },
                        aborted,
                    )
                }) {
                Ok(handle) => handles.push(handle),
                Err(source) => {
                    aborted.store(true, Ordering::Release);
                    start_error = Some(RocksDbWalletError::SecondaryDerivationThreadUnavailable {
                        reason: source.to_string(),
                    });
                    break;
                }
            }
        }
        let mut completed = Vec::with_capacity(shard_count);
        let mut first_error = start_error;
        let mut sibling_aborted = false;
        for handle in handles {
            match handle.join() {
                Ok(SecondaryBranchOutcome::Completed(output)) => completed.push(*output),
                Ok(SecondaryBranchOutcome::Failed(error)) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Ok(SecondaryBranchOutcome::SiblingAborted) => sibling_aborted = true,
                Err(_) => {
                    aborted.store(true, Ordering::Release);
                    if first_error.is_none() {
                        first_error = Some(RocksDbWalletError::SecondaryDerivationWorkerStopped);
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return SecondaryBranchOutcome::Failed(error);
        }
        if sibling_aborted {
            return SecondaryBranchOutcome::SiblingAborted;
        }
        let drain_duration = meta.drain_started.elapsed();
        match merge_address_tx_shard_outputs(
            config,
            completed,
            meta.sort_evidence,
            meta.sort_durations,
            (meta.sort_duration, drain_duration),
        ) {
            Ok(merged) => SecondaryBranchOutcome::Completed(Box::new(merged)),
            Err(error) => {
                aborted.store(true, Ordering::Release);
                SecondaryBranchOutcome::Failed(error)
            }
        }
    })
}

fn drain_single_address_tx_shard(
    config: WalletProjectionLoadConfig<'_>,
    mut stream: SortedVariableValues<ADDRESS_TRANSACTION_KEY_BYTES>,
    undo_collector: &RetainedUndoCollector,
    shard_spec: AddressTxShardSpec,
    aborted: &AtomicBool,
) -> SecondaryBranchOutcome<AddressTransactionShardOutput> {
    let _abort_on_unwind = SetAbortOnUnwind(aborted);
    if aborted.load(Ordering::Acquire) {
        return SecondaryBranchOutcome::SiblingAborted;
    }
    let AddressTxShardSpec {
        shard_index,
        shard_count,
        active_writers,
        branch_hashing,
    } = shard_spec;
    let outcome = (|| -> Result<Option<AddressTransactionShardOutput>, RocksDbWalletError> {
        let shard_staging =
            shard_staging_path(config.staging_path, "address-tx", shard_index, shard_count)?;
        let mut writer = shard_projection_writer(
            config,
            &shard_staging,
            WalletProjectionRowFamily::TransparentAddressTransaction,
            "wallet-address-transaction",
            active_writers,
        )?;
        let mut branch_accumulators =
            ConstructionAccumulators::start_optional_branch(config.network, branch_hashing);
        let mut branch_evidence = WalletProjectionSstEvidence::default();
        let mut targets = AddressTransactionTargets {
            writer: &mut writer,
            undo_collector,
            accumulators: &mut branch_accumulators,
            evidence: &mut branch_evidence,
            aborted,
        };
        if !drain_address_transactions(&mut stream, &mut targets)? {
            return Ok(None);
        }
        let finished_accumulators = branch_accumulators.finish_branch()?;
        let (transaction_files, transaction_write) = writer.finish()?;
        Ok(Some(AddressTransactionShardOutput {
            transaction_files,
            transaction_write,
            accumulators: finished_accumulators,
            evidence: branch_evidence,
        }))
    })();
    match outcome {
        Ok(Some(completed)) => SecondaryBranchOutcome::Completed(Box::new(completed)),
        Ok(None) => SecondaryBranchOutcome::SiblingAborted,
        Err(error) => {
            aborted.store(true, Ordering::Release);
            SecondaryBranchOutcome::Failed(error)
        }
    }
}

fn merge_address_tx_shard_outputs(
    config: WalletProjectionLoadConfig<'_>,
    outputs: Vec<AddressTransactionShardOutput>,
    sort_evidence: VariableValueSortEvidence,
    sort_durations: VariableValueSortDurations,
    phase_durations: (Duration, Duration),
) -> Result<AddressTransactionBranchOutput, RocksDbWalletError> {
    let mut tx_sets = Vec::with_capacity(outputs.len());
    let mut transaction_write = Duration::ZERO;
    let mut accumulators = ConstructionAccumulators::start_optional_branch(config.network, None);
    let mut evidence = WalletProjectionSstEvidence::default();
    for output in outputs {
        tx_sets.push(output.transaction_files);
        transaction_write = transaction_write.max(output.transaction_write);
        accumulators.combine_branch(&output.accumulators)?;
        evidence.merge(&output.evidence)?;
    }
    let finished_accumulators = accumulators.finish_branch()?;
    Ok(AddressTransactionBranchOutput {
        sort_evidence,
        sort_durations,
        sort_duration: phase_durations.0,
        drain_duration: phase_durations.1,
        transaction_files: combine_sst_file_sets(tx_sets)?,
        transaction_write,
        accumulators: finished_accumulators,
        evidence,
    })
}

const fn address_transaction_sort_memory_bytes(
    max_outpoint_sort_memory_bytes: u64,
    max_secondary_sort_memory_bytes_per_sorter: u64,
) -> u64 {
    let recycled_outpoint_budget = max_outpoint_sort_memory_bytes / 2;
    if max_secondary_sort_memory_bytes_per_sorter >= recycled_outpoint_budget {
        max_secondary_sort_memory_bytes_per_sorter
    } else {
        recycled_outpoint_budget
    }
}

struct OutpointDrainTargets<'a, 'options, 'sorter> {
    unspent_writer: &'a mut TimedSstWriter<'options>,
    spent_writer: &'a mut TimedSstWriter<'options>,
    address_index_writer: &'a mut ShardedKeyPrefixWriter<'sorter, ADDRESS_UNSPENT_KEY_BYTES>,
    address_transaction_writer:
        &'a mut ShardedKeyPrefixWriter<'sorter, ADDRESS_TRANSACTION_KEY_BYTES>,
    accumulators: &'a mut ConstructionAccumulators,
    sst_evidence: &'a mut WalletProjectionSstEvidence,
    utxo_count: &'a mut u64,
    total_value_zat: &'a mut u64,
    aborted: &'a AtomicBool,
}

fn drain_outpoint_events(
    sorted: &mut SortedVariableValues<OUTPOINT_EVENT_KEY_BYTES>,
    targets: &mut OutpointDrainTargets<'_, '_, '_>,
) -> Result<bool, RocksDbWalletError> {
    let mut pending = sorted.next_record()?;
    while let Some(first) = pending.take() {
        if targets.aborted.load(Ordering::Acquire) {
            return Ok(false);
        }
        let outpoint_key = WalletOutpointKey::decode(&first.key[..36])?;
        let mut output = None;
        let mut spend = None;
        accept_outpoint_event(&first.key, &first.encoded_value, &mut output, &mut spend)?;
        loop {
            let next = sorted.next_record()?;
            let Some(next_record) = next else {
                pending = None;
                break;
            };
            if next_record.key[..36] != first.key[..36] {
                pending = Some(next_record);
                break;
            }
            accept_outpoint_event(
                &next_record.key,
                &next_record.encoded_value,
                &mut output,
                &mut spend,
            )?;
        }
        let output = output.ok_or(WalletProjectionContractError::MissingTransparentPredecessor)?;
        stage_address_transaction(
            output.address_script_hash,
            output.created_at,
            targets.address_transaction_writer,
        )?;
        if let Some(spend) = spend {
            if !precedes(output.created_at, spend.spent_at) {
                return Err(WalletProjectionContractError::MissingTransparentPredecessor.into());
            }
            let spent_output =
                StoredWalletSpentOutput::new(output, spend.spent_at, spend.input_index);
            stage_address_transaction(
                spent_output.output.address_script_hash,
                spent_output.spent_at,
                targets.address_transaction_writer,
            )?;
            let encoded_value = spent_output.encode_value()?;
            append_row(
                targets.spent_writer,
                targets.accumulators,
                targets.sst_evidence,
                WalletProjectionRowFamily::TransparentSpentOutput,
                outpoint_key.as_bytes(),
                &encoded_value,
            )?;
        } else {
            let address_key = WalletAddressUnspentOutputKey::from_parts(
                output.address_script_hash,
                output.created_at.block_height,
                output.outpoint,
            );
            targets
                .address_index_writer
                .push(*address_key.as_bytes(), &output.value_zat.to_be_bytes())?;
            *targets.utxo_count = increment(*targets.utxo_count)?;
            *targets.total_value_zat = targets
                .total_value_zat
                .checked_add(output.value_zat)
                .ok_or(WalletProjectionContractError::UtxoValueOverflow)?;
            targets.accumulators.insert_stored_unspent_output(&output)?;
            let encoded_value = output.encode_value()?;
            append_row(
                targets.unspent_writer,
                targets.accumulators,
                targets.sst_evidence,
                WalletProjectionRowFamily::TransparentUnspentOutput,
                outpoint_key.as_bytes(),
                &encoded_value,
            )?;
        }
    }
    Ok(true)
}

fn accept_outpoint_event(
    key: &[u8; OUTPOINT_EVENT_KEY_BYTES],
    encoded_value: &[u8],
    output: &mut Option<StoredWalletUnspentOutput>,
    spend: &mut Option<StagedSpend>,
) -> Result<(), RocksDbWalletError> {
    let outpoint_key = WalletOutpointKey::decode(&key[..36])?;
    match key[36] {
        OUTPUT_EVENT_TAG => {
            if output.is_some() {
                return Err(WalletProjectionContractError::DuplicateOutput.into());
            }
            *output = Some(StoredWalletUnspentOutput::decode_value(
                outpoint_key,
                encoded_value,
            )?);
        }
        SPEND_EVENT_TAG => {
            if spend.is_some() {
                return Err(WalletProjectionContractError::DuplicateSpend.into());
            }
            *spend = Some(decode_spend_trailer(encoded_value)?);
        }
        _ => {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "outpoint event sort contains an unsupported tag",
            });
        }
    }
    Ok(())
}

struct AddressIndexTargets<'a, 'options> {
    index_writer: &'a mut TimedSstWriter<'options>,
    balance_writer: &'a mut TimedSstWriter<'options>,
    accumulators: &'a mut ConstructionAccumulators,
    evidence: &'a mut WalletProjectionSstEvidence,
    aborted: &'a AtomicBool,
}

struct AddressTransactionTargets<'a, 'options> {
    writer: &'a mut TimedSstWriter<'options>,
    undo_collector: &'a RetainedUndoCollector,
    accumulators: &'a mut ConstructionAccumulators,
    evidence: &'a mut WalletProjectionSstEvidence,
    aborted: &'a AtomicBool,
}

fn drain_address_index(
    sorted: &mut SortedVariableValues<ADDRESS_UNSPENT_KEY_BYTES>,
    targets: &mut AddressIndexTargets<'_, '_>,
) -> Result<bool, RocksDbWalletError> {
    let mut current_address = None;
    let mut current_balance = 0_u64;
    while let Some(record) = sorted.next_record()? {
        if targets.aborted.load(Ordering::Acquire) {
            return Ok(false);
        }
        if record.encoded_value.len() != 8 {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "address-index staging value is not an exact u64",
            });
        }
        let address_key = WalletAddressUnspentOutputKey::decode(&record.key)?;
        let address = address_key.address_script_hash();
        if current_address.is_some_and(|current| current != address) {
            write_balance(
                current_address.ok_or(WalletProjectionContractError::AddressBalanceUnderflow)?,
                current_balance,
                targets.balance_writer,
                targets.accumulators,
                targets.evidence,
            )?;
            current_balance = 0;
        }
        current_address = Some(address);
        let value_zat = u64::from_be_bytes(record.encoded_value.try_into().map_err(|_| {
            RocksDbWalletError::AdmissionChanged {
                reason: "address-index staging value changed after length validation",
            }
        })?);
        current_balance = current_balance
            .checked_add(value_zat)
            .ok_or(WalletProjectionContractError::AddressBalanceOverflow)?;
        append_row(
            targets.index_writer,
            targets.accumulators,
            targets.evidence,
            WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
            &record.key,
            &[],
        )?;
    }
    if let Some(address) = current_address {
        write_balance(
            address,
            current_balance,
            targets.balance_writer,
            targets.accumulators,
            targets.evidence,
        )?;
    }
    Ok(true)
}

fn write_balance(
    address: zinder_core::TransparentAddressScriptHash,
    balance_zat: u64,
    writer: &mut TimedSstWriter<'_>,
    accumulators: &mut ConstructionAccumulators,
    evidence: &mut WalletProjectionSstEvidence,
) -> Result<(), RocksDbWalletError> {
    if balance_zat == 0 {
        return Ok(());
    }
    let balance = WalletAddressBalance {
        address_script_hash: address,
        balance_zat,
    };
    append_row(
        writer,
        accumulators,
        evidence,
        WalletProjectionRowFamily::TransparentAddressBalance,
        &balance.encode_key(),
        &balance.encode_value(),
    )
}

fn drain_address_transactions(
    sorted: &mut SortedVariableValues<ADDRESS_TRANSACTION_KEY_BYTES>,
    targets: &mut AddressTransactionTargets<'_, '_>,
) -> Result<bool, RocksDbWalletError> {
    let mut pending = sorted.next_record()?;
    while let Some(first) = pending.take() {
        if targets.aborted.load(Ordering::Acquire) {
            return Ok(false);
        }
        if first.encoded_value.len() != 32 {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "address-transaction staging value is not exact32",
            });
        }
        let mut canonical_value = first.encoded_value;
        loop {
            let next = sorted.next_record()?;
            let Some(next_record) = next else {
                pending = None;
                break;
            };
            if next_record.key != first.key {
                pending = Some(next_record);
                break;
            }
            if next_record.encoded_value != canonical_value {
                return Err(RocksDbWalletError::AddressTransactionConflict);
            }
            canonical_value = next_record.encoded_value;
        }
        let key = WalletAddressTransactionKey::decode(&first.key)?;
        WalletAddressTransaction::decode_stored_value(key, &canonical_value)?;
        targets.undo_collector.try_collect(key)?;
        append_row(
            targets.writer,
            targets.accumulators,
            targets.evidence,
            WalletProjectionRowFamily::TransparentAddressTransaction,
            &first.key,
            &canonical_value,
        )?;
    }
    Ok(true)
}

fn stage_address_transaction(
    address_script_hash: zinder_core::TransparentAddressScriptHash,
    position: WalletStoredTransactionPosition,
    tx_writer: &mut ShardedKeyPrefixWriter<'_, ADDRESS_TRANSACTION_KEY_BYTES>,
) -> Result<(), RocksDbWalletError> {
    let key = WalletAddressTransactionKey::new(
        address_script_hash,
        position.block_height,
        position.tx_index_in_block,
    );
    tx_writer.push(*key.as_bytes(), &position.transaction_id.as_bytes())?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "one row must update its physical writer and both pieces of logical evidence together"
)]
fn append_row(
    writer: &mut TimedSstWriter<'_>,
    accumulators: &mut ConstructionAccumulators,
    evidence: &mut WalletProjectionSstEvidence,
    family: WalletProjectionRowFamily,
    key: &[u8],
    encoded_value: &[u8],
) -> Result<(), RocksDbWalletError> {
    writer.put(key, encoded_value)?;
    accumulators.append_row(family, key, encoded_value)?;
    evidence.add_row(key, encoded_value)
}

fn shard_projection_writer<'options>(
    config: WalletProjectionLoadConfig<'options>,
    staging_path: &'options Path,
    family: WalletProjectionRowFamily,
    artifact_label: &'static str,
    active_concurrent_writers: usize,
) -> Result<TimedSstWriter<'options>, RocksDbWalletError> {
    let writer = if config.accumulator_hashing_workers.get() > 1
        && let Some(queue_budget) = scaled_pipelined_sst_queue_budget_bytes(
            config.sst_target_logical_bytes,
            active_concurrent_writers,
        ) {
        OrderedSstWriter::new_pipelined(
            staging_path,
            artifact_label,
            config.options.for_bulk_sst(family),
            config.sst_target_logical_bytes,
            queue_budget,
        )?
    } else {
        OrderedSstWriter::new(
            staging_path,
            artifact_label,
            config.options.for_bulk_sst(family),
            config.sst_target_logical_bytes,
        )?
    };
    Ok(TimedSstWriter {
        writer,
        write_duration: Duration::ZERO,
    })
}

fn ordered_writer<'options>(
    config: WalletProjectionLoadConfig<'options>,
    family: WalletProjectionRowFamily,
    artifact_label: &'static str,
) -> Result<TimedSstWriter<'options>, RocksDbWalletError> {
    Ok(TimedSstWriter {
        writer: OrderedSstWriter::new(
            config.staging_path,
            artifact_label,
            config.options.for_bulk_sst(family),
            config.sst_target_logical_bytes,
        )?,
        write_duration: Duration::ZERO,
    })
}

/// Ordered SST writer that keeps the wall time spent inside it.
///
/// The time covers each row's encoding, file rotation, and the final file
/// close, so the build can separate SST writing from the drain that feeds it.
struct TimedSstWriter<'options> {
    writer: OrderedSstWriter<'options>,
    write_duration: Duration,
}

impl TimedSstWriter<'_> {
    fn put(&mut self, key: &[u8], encoded_value: &[u8]) -> Result<(), BulkLoadError> {
        let started = Instant::now();
        let outcome = self.writer.put(key, encoded_value);
        self.write_duration = self.write_duration.saturating_add(started.elapsed());
        outcome
    }

    fn finish(self) -> Result<(SstFileSet, Duration), BulkLoadError> {
        let started = Instant::now();
        let files = self.writer.finish()?;
        Ok((files, self.write_duration.saturating_add(started.elapsed())))
    }
}

fn prepare_families(
    artifacts: [(&'static str, PreparedFamilyFiles); 7],
) -> Result<(Vec<PreparedWalletColumnFamily>, u64, u64), RocksDbWalletError> {
    let mut families = Vec::with_capacity(artifacts.len());
    let mut sst_file_bytes = 0_u64;
    let mut sst_file_count = 0_u64;
    for (name, files) in artifacts {
        sst_file_bytes = sst_file_bytes
            .checked_add(files.file_bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        sst_file_count = sst_file_count
            .checked_add(
                u64::try_from(files.paths.len())
                    .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
            )
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        families.push(PreparedWalletColumnFamily {
            name,
            paths: files.paths,
        });
    }
    Ok((families, sst_file_bytes, sst_file_count))
}

fn validate_next_block(
    first_retained_block: BlockId,
    previous: Option<BlockId>,
    block: BlockId,
    parent_hash: BlockHash,
) -> Result<(), WalletProjectionContractError> {
    match previous {
        None if block == first_retained_block => Ok(()),
        Some(previous)
            if previous.height.next() == Some(block.height) && previous.hash == parent_hash =>
        {
            Ok(())
        }
        None | Some(_) => Err(WalletProjectionContractError::NonContiguousBlock),
    }
}

fn precedes(
    created_at: WalletStoredTransactionPosition,
    spent_at: WalletStoredTransactionPosition,
) -> bool {
    if created_at.block_height < spent_at.block_height {
        return true;
    }
    created_at.block_height == spent_at.block_height
        && created_at.tx_index_in_block < spent_at.tx_index_in_block
}

fn outpoint_event_key(key: WalletOutpointKey, tag: u8) -> [u8; OUTPOINT_EVENT_KEY_BYTES] {
    let mut encoded = [0; OUTPOINT_EVENT_KEY_BYTES];
    encoded[..36].copy_from_slice(key.as_bytes());
    encoded[36] = tag;
    encoded
}

fn encode_spend_trailer(spend: StagedSpend) -> [u8; SPEND_TRAILER_BYTES] {
    let mut encoded = [0; SPEND_TRAILER_BYTES];
    encoded[..32].copy_from_slice(&spend.spent_at.transaction_id.as_bytes());
    encoded[32..36].copy_from_slice(&spend.input_index.to_be_bytes());
    encoded[36..40].copy_from_slice(&spend.spent_at.tx_index_in_block.to_be_bytes());
    encoded[40..44].copy_from_slice(&spend.spent_at.block_height.value().to_be_bytes());
    encoded
}

fn decode_spend_trailer(encoded: &[u8]) -> Result<StagedSpend, RocksDbWalletError> {
    let encoded: [u8; SPEND_TRAILER_BYTES] =
        encoded
            .try_into()
            .map_err(|_| RocksDbWalletError::AdmissionChanged {
                reason: "spend staging value is not the exact version-2 44-byte trailer",
            })?;
    let mut transaction_id = [0; 32];
    transaction_id.copy_from_slice(&encoded[..32]);
    let mut input_index = [0; 4];
    input_index.copy_from_slice(&encoded[32..36]);
    let mut tx_index = [0; 4];
    tx_index.copy_from_slice(&encoded[36..40]);
    let mut height = [0; 4];
    height.copy_from_slice(&encoded[40..44]);
    Ok(StagedSpend {
        spent_at: WalletStoredTransactionPosition::new(
            TransactionId::from_bytes(transaction_id),
            u32::from_be_bytes(tx_index),
            BlockHeight::new(u32::from_be_bytes(height)),
        ),
        input_index: u32::from_be_bytes(input_index),
    })
}

fn increment(counter: u64) -> Result<u64, RocksDbWalletError> {
    add_counter(counter, 1)
}

fn add_counter(counter: u64, delta: u64) -> Result<u64, RocksDbWalletError> {
    counter
        .checked_add(delta)
        .ok_or(RocksDbWalletError::BuildCounterOverflow)
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::Path};

    use rust_rocksdb::Cache;
    use tempfile::TempDir;
    use zinder_core::{
        BlockHeaderArtifact, CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
        CanonicalBlockReplayFormatVersion, CanonicalTransactionFacts, LockTime, PrivacyShape,
        SerializedBytesDigest, TransactionComponentCounts, TransactionIntrinsicValueBalances,
        TransactionPublicFacts, TransactionVersion, TransparentAddressScriptHash,
        TransparentInputFact, TransparentOutPoint, TransparentOutputFact,
        decode_canonical_block_replay, encode_canonical_block_replay,
    };
    use zinder_rocksdb_bulk_load::BulkLoadError;
    use zinder_store::RocksDbResourceBudget;
    use zinder_wallet_projection::WalletProjectionSerialOracle;

    use super::*;

    const TEST_SORT_MEMORY_BYTES: u64 = 1024 * 1024;
    const TEST_TEMPORARY_FILE_BYTES: u64 = 16 * 1024 * 1024;
    const TEST_SST_LOGICAL_BYTES: u64 = 1024 * 1024;
    const TEST_REORG_UNDO_BYTES: u64 = 1024 * 1024;
    const TEST_BLOCK_CACHE_BYTES: usize = 1024 * 1024;

    #[derive(Clone, Copy)]
    struct TestByteLimits {
        outpoint_sort: u64,
        secondary_sort_per_sorter: u64,
        temporary_files_per_sorter: u64,
    }

    impl Default for TestByteLimits {
        fn default() -> Self {
            Self {
                outpoint_sort: TEST_SORT_MEMORY_BYTES,
                secondary_sort_per_sorter: TEST_SORT_MEMORY_BYTES,
                temporary_files_per_sorter: TEST_TEMPORARY_FILE_BYTES,
            }
        }
    }

    #[test]
    fn retained_undo_accounting_bounds_a_thousand_block_unsettled_suffix()
    -> Result<(), Box<dyn Error>> {
        const DEPTH: u32 = 1_000;
        // The projector's default `build.max_accounted_reorg_undo_bytes`.
        const DEFAULT_UNDO_CAP_BYTES: u64 = 512 * 1024 * 1024;
        // Created and spent outpoints per block in a transparent-heavy suffix.
        const OUTPOINTS_PER_BLOCK: u32 = 64;
        let settled_tip = BlockId::new(BlockHeight::new(10_000), BlockHash::from_bytes([1; 32]));
        let digest = CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            CanonicalBlockFactsSequenceDigestVersion::V1,
            1,
            [2; 32],
        );
        let fill = |undo: &mut RetainedUndo, block_count: u32| {
            for offset in 1..=block_count {
                let height = BlockHeight::new(settled_tip.height.value() + offset);
                let hash = BlockHash::from_bytes([u8::try_from(offset % 251).unwrap_or(0); 32]);
                undo.begin_block(BlockId::new(height, hash), hash, digest, digest)?;
                for output_index in 0..OUTPOINTS_PER_BLOCK {
                    let mut transaction_id = [0; 32];
                    transaction_id[..4].copy_from_slice(&offset.to_be_bytes());
                    undo.add_created_outpoint(WalletOutpointKey::new(TransparentOutPoint::new(
                        TransactionId::from_bytes(transaction_id),
                        output_index,
                    )))?;
                    transaction_id[4] = 1;
                    undo.add_spent_outpoint(WalletOutpointKey::new(TransparentOutPoint::new(
                        TransactionId::from_bytes(transaction_id),
                        output_index,
                    )))?;
                }
                undo.finish_block()?;
            }
            Ok::<_, RocksDbWalletError>(())
        };
        let per_block_bytes = u64::try_from(
            size_of::<WalletReorgUndo>()
                + 2 * usize::try_from(OUTPOINTS_PER_BLOCK)? * size_of::<WalletOutpointKey>(),
        )?;
        let suffix_bytes = u64::from(DEPTH) * per_block_bytes;

        let mut undo = RetainedUndo::new(DEPTH, settled_tip, DEFAULT_UNDO_CAP_BYTES);
        fill(&mut undo, DEPTH)?;
        assert_eq!(undo.records.len(), usize::try_from(DEPTH)?);
        assert_eq!(undo.accounted_bytes, suffix_bytes);
        assert_eq!(undo.peak_accounted_bytes, suffix_bytes);
        assert!(suffix_bytes < DEFAULT_UNDO_CAP_BYTES / 64);
        let next_height = BlockHeight::new(settled_tip.height.value() + DEPTH + 1);
        assert!(matches!(
            undo.begin_block(
                BlockId::new(next_height, BlockHash::from_bytes([0xfe; 32])),
                BlockHash::from_bytes([0xfd; 32]),
                digest,
                digest,
            ),
            Err(RocksDbWalletError::WalletProjectionRebuildRequired { .. })
        ));

        // A cap below the complete suffix fails closed instead of growing.
        let mut capped = RetainedUndo::new(DEPTH, settled_tip, suffix_bytes - 1);
        let error = fill(&mut capped, DEPTH)
            .err()
            .ok_or("a suffix above the accounting cap must be refused")?;
        assert!(
            matches!(
                error,
                RocksDbWalletError::AccountedReorgUndoMemoryLimit {
                    limit_bytes,
                    required_bytes,
                } if limit_bytes == suffix_bytes - 1 && required_bytes == suffix_bytes
            ),
            "{error:?}"
        );
        Ok(())
    }

    #[test]
    fn external_loader_accepts_an_authenticated_checkpoint_successor_as_its_first_block()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let first_retained = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xc2; 32]));
        let tip = BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([0xc3; 32]));
        let blocks = vec![
            block_facts(2, [0xc1; 32], [0xc2; 32], Vec::new()),
            block_facts(3, [0xc2; 32], [0xc3; 32], Vec::new()),
        ];

        let prepared = write_projection_ssts(
            test_config_with_bounds(
                temporary.path(),
                &options,
                TestByteLimits::default(),
                first_retained,
                tip,
            ),
            canonical_scan(validated_replays(&blocks)?),
        )?;

        assert_eq!(prepared.first_block, first_retained);
        assert_eq!(prepared.tip, tip);
        assert_eq!(prepared.counters.scanned_block_count, 2);
        Ok(())
    }

    #[test]
    fn external_loader_rejects_a_first_block_outside_the_authenticated_retained_anchor()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let expected_first = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xd2; 32]));
        let observed_first = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xc2; 32]));
        let blocks = vec![block_facts(2, [0xc1; 32], [0xc2; 32], Vec::new())];

        let result = write_projection_ssts(
            test_config_with_bounds(
                temporary.path(),
                &options,
                TestByteLimits::default(),
                expected_first,
                observed_first,
            ),
            canonical_scan(validated_replays(&blocks)?),
        );

        assert!(matches!(
            result,
            Err(RocksDbWalletError::Contract(
                WalletProjectionContractError::NonContiguousBlock
            ))
        ));
        Ok(())
    }

    #[test]
    fn external_loader_rejects_duplicate_output_and_spend_keys() -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let transaction = TransactionId::from_bytes([0xb1; 32]);
        let outpoint = TransparentOutPoint::new(transaction, 0);
        let duplicate_outputs = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![
                transaction_facts(
                    transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(0, 1, [0x51], address)],
                ),
                transaction_facts(
                    transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(0, 1, [0x51], address)],
                ),
            ],
        )];
        let duplicate_output_replays = validated_replays(&duplicate_outputs)?;
        let duplicate_output_result = write_projection_ssts(
            test_config(temporary.path(), &options, TestByteLimits::default()),
            canonical_scan(duplicate_output_replays),
        );
        assert!(matches!(
            duplicate_output_result,
            Err(RocksDbWalletError::Contract(
                WalletProjectionContractError::DuplicateOutput
            ))
        ));

        let duplicate_spends = vec![
            block_facts(
                1,
                [0; 32],
                [0xc1; 32],
                vec![transaction_facts(
                    transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(0, 1, [0x51], address)],
                )],
            ),
            block_facts(
                2,
                [0xc1; 32],
                [0xc2; 32],
                vec![transaction_facts(
                    TransactionId::from_bytes([0xb2; 32]),
                    vec![
                        TransparentInputFact::new(0, outpoint),
                        TransparentInputFact::new(1, outpoint),
                    ],
                    Vec::new(),
                )],
            ),
        ];
        let duplicate_spend_replays = validated_replays(&duplicate_spends)?;
        let duplicate_spend_result = write_projection_ssts(
            test_config_with_settled_tip(
                temporary.path(),
                &options,
                TestByteLimits::default(),
                BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xc2; 32])),
            ),
            canonical_scan(duplicate_spend_replays),
        );
        assert!(matches!(
            duplicate_spend_result,
            Err(RocksDbWalletError::Contract(
                WalletProjectionContractError::DuplicateSpend
            ))
        ));
        Ok(())
    }

    #[test]
    fn external_loader_rejects_missing_and_future_predecessors() -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let missing = TransparentOutPoint::new(TransactionId::from_bytes([0xee; 32]), 0);
        let missing_blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb1; 32]),
                vec![TransparentInputFact::new(0, missing)],
                Vec::new(),
            )],
        )];
        assert_missing_predecessor(
            temporary.path(),
            &options,
            validated_replays(&missing_blocks)?,
        );

        let later_transaction = TransactionId::from_bytes([0xb3; 32]);
        let future = TransparentOutPoint::new(later_transaction, 0);
        let future_blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![
                transaction_facts(
                    TransactionId::from_bytes([0xb2; 32]),
                    vec![TransparentInputFact::new(0, future)],
                    Vec::new(),
                ),
                transaction_facts(
                    later_transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(
                        0,
                        1,
                        [0x51],
                        TransparentAddressScriptHash::from_bytes([0xa1; 32]),
                    )],
                ),
            ],
        )];
        assert_missing_predecessor(
            temporary.path(),
            &options,
            validated_replays(&future_blocks)?,
        );
        Ok(())
    }

    #[test]
    fn external_loader_preserves_zero_only_and_mixed_zero_balances() -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let zero_blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb1; 32]),
                Vec::new(),
                vec![TransparentOutputFact::new(0, 0, [0x51], address)],
            )],
        )];
        let zero = write_projection_ssts(
            test_config(temporary.path(), &options, TestByteLimits::default()),
            canonical_scan(validated_replays(&zero_blocks)?),
        )?;
        let zero_oracle = oracle_for(&zero_blocks)?;
        assert_eq!(zero.row_counts, zero_oracle.row_counts());
        assert_eq!(zero.utxo_summary, zero_oracle.utxo_summary());
        assert_eq!(zero.projection_digest, zero_oracle.projection_digest()?);
        assert_eq!(
            zero.projection_accumulator,
            zero_oracle.projection_accumulator()?
        );
        assert_eq!(zero.row_counts.transparent_unspent_output_count, 1);
        assert_eq!(zero.row_counts.transparent_address_balance_count, 0);

        let mixed_blocks = vec![block_facts(
            1,
            [0; 32],
            [0xd1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb2; 32]),
                Vec::new(),
                vec![
                    TransparentOutputFact::new(0, 0, [0x51], address),
                    TransparentOutputFact::new(1, 7, [0x52], address),
                ],
            )],
        )];
        let mixed_tip = BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xd1; 32]));
        let mixed = write_projection_ssts(
            test_config_with_bounds(
                temporary.path(),
                &options,
                TestByteLimits::default(),
                mixed_tip,
                mixed_tip,
            ),
            canonical_scan(validated_replays(&mixed_blocks)?),
        )?;
        let mixed_oracle = oracle_for(&mixed_blocks)?;
        assert_eq!(mixed.row_counts, mixed_oracle.row_counts());
        assert_eq!(mixed.utxo_summary, mixed_oracle.utxo_summary());
        assert_eq!(mixed.projection_digest, mixed_oracle.projection_digest()?);
        assert_eq!(
            mixed.projection_accumulator,
            mixed_oracle.projection_accumulator()?
        );
        assert_eq!(mixed.row_counts.transparent_unspent_output_count, 2);
        assert_eq!(mixed.row_counts.transparent_address_balance_count, 1);
        assert_eq!(mixed.utxo_summary.total_value_zat, 7);
        Ok(())
    }

    #[test]
    fn external_loader_merges_multiple_outpoint_runs() -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb1; 32]),
                Vec::new(),
                vec![
                    TransparentOutputFact::new(0, 1, [0x51], address),
                    TransparentOutputFact::new(1, 2, [0x52], address),
                    TransparentOutputFact::new(2, 3, [0x53], address),
                ],
            )],
        )];
        let prepared = write_projection_ssts(
            test_config(
                temporary.path(),
                &options,
                TestByteLimits {
                    outpoint_sort: 160,
                    ..TestByteLimits::default()
                },
            ),
            canonical_scan(validated_replays(&blocks)?),
        )?;

        assert!(prepared.outpoint_sort_evidence.initial_run_count > 1);
        // At most 256 runs merge inside the reader without a written pass.
        assert_eq!(prepared.outpoint_sort_evidence.merge_pass_count, 0);
        assert_eq!(prepared.row_counts.transparent_unspent_output_count, 3);
        assert_eq!(prepared.utxo_summary.total_value_zat, 6);
        Ok(())
    }

    /// Three blocks whose outputs go to two addresses, with spends of
    /// outputs from earlier blocks.
    fn three_block_spend_chain() -> Vec<CanonicalBlockFacts> {
        let first = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let second = TransparentAddressScriptHash::from_bytes([0xa2; 32]);
        let funding = TransactionId::from_bytes([0xb1; 32]);
        let change = TransactionId::from_bytes([0xb2; 32]);
        vec![
            block_facts(
                1,
                [0; 32],
                [0xc1; 32],
                vec![transaction_facts(
                    funding,
                    Vec::new(),
                    vec![
                        TransparentOutputFact::new(0, 10, [0x51], first),
                        TransparentOutputFact::new(1, 20, [0x52, 0x53], second),
                        TransparentOutputFact::new(2, 30, [0x54], first),
                    ],
                )],
            ),
            block_facts(
                2,
                [0xc1; 32],
                [0xc2; 32],
                vec![transaction_facts(
                    change,
                    vec![TransparentInputFact::new(
                        0,
                        TransparentOutPoint::new(funding, 0),
                    )],
                    vec![
                        TransparentOutputFact::new(0, 4, [0x55], second),
                        TransparentOutputFact::new(1, 6, [0x56], first),
                    ],
                )],
            ),
            block_facts(
                3,
                [0xc2; 32],
                [0xc3; 32],
                vec![transaction_facts(
                    TransactionId::from_bytes([0xb3; 32]),
                    vec![
                        TransparentInputFact::new(0, TransparentOutPoint::new(funding, 1)),
                        TransparentInputFact::new(1, TransparentOutPoint::new(change, 0)),
                    ],
                    vec![TransparentOutputFact::new(0, 24, [0x57], first)],
                )],
            ),
        ]
    }

    #[test]
    fn every_hashing_worker_count_publishes_the_serial_oracle_evidence()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let blocks = three_block_spend_chain();
        let oracle = oracle_for(&blocks)?;
        let tip = BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([0xc3; 32]));
        for workers in [1, 2, 4, 8] {
            let config = WalletProjectionLoadConfig {
                accumulator_hashing_workers: NonZeroU32::new(workers).ok_or("nonzero workers")?,
                ..test_config_with_settled_tip(
                    temporary.path(),
                    &options,
                    TestByteLimits::default(),
                    tip,
                )
            };
            let prepared =
                write_projection_ssts(config, canonical_scan(validated_replays(&blocks)?))?;
            assert_eq!(
                prepared.row_counts,
                oracle.row_counts(),
                "{workers} workers"
            );
            assert_eq!(
                prepared.utxo_summary,
                oracle.utxo_summary(),
                "{workers} workers"
            );
            assert_eq!(
                prepared.projection_digest,
                oracle.projection_digest()?,
                "{workers} workers"
            );
            assert_eq!(
                prepared.projection_accumulator,
                oracle.projection_accumulator()?,
                "{workers} workers"
            );
            assert_eq!(prepared.row_counts.transparent_spent_output_count, 3);
            assert_eq!(prepared.utxo_summary.total_value_zat, 60);
        }
        Ok(())
    }

    #[test]
    fn external_loader_refuses_memory_and_temporary_file_limit_crossings()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb1; 32]),
                Vec::new(),
                vec![TransparentOutputFact::new(
                    0,
                    1,
                    [0x51],
                    TransparentAddressScriptHash::from_bytes([0xa1; 32]),
                )],
            )],
        )];
        let memory_result = write_projection_ssts(
            test_config(
                temporary.path(),
                &options,
                TestByteLimits {
                    outpoint_sort: 1,
                    ..TestByteLimits::default()
                },
            ),
            canonical_scan(validated_replays(&blocks)?),
        );
        assert!(matches!(
            memory_result,
            Err(RocksDbWalletError::BulkLoad(
                BulkLoadError::AccountedMemoryLimit {
                    limit_bytes: 1,
                    required_bytes: _
                }
            ))
        ));

        let disk_result = write_projection_ssts(
            test_config(
                temporary.path(),
                &options,
                TestByteLimits {
                    temporary_files_per_sorter: 1,
                    ..TestByteLimits::default()
                },
            ),
            canonical_scan(validated_replays(&blocks)?),
        );
        assert!(matches!(
            disk_result,
            Err(RocksDbWalletError::BulkLoad(
                BulkLoadError::TemporaryFileLimit {
                    limit_bytes: 1,
                    required_bytes: _
                }
            ))
        ));
        Ok(())
    }

    fn test_column_family_options() -> WalletColumnFamilyOptions {
        WalletColumnFamilyOptions::new(
            &Cache::new_lru_cache(TEST_BLOCK_CACHE_BYTES),
            RocksDbResourceBudget::for_local_tests(),
        )
    }

    fn test_config<'options>(
        staging_path: &'options Path,
        options: &'options WalletColumnFamilyOptions,
        limits: TestByteLimits,
    ) -> WalletProjectionLoadConfig<'options> {
        test_config_with_settled_tip(
            staging_path,
            options,
            limits,
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xc1; 32])),
        )
    }

    fn test_config_with_settled_tip<'options>(
        staging_path: &'options Path,
        options: &'options WalletColumnFamilyOptions,
        limits: TestByteLimits,
        settled_tip: BlockId,
    ) -> WalletProjectionLoadConfig<'options> {
        test_config_with_bounds(
            staging_path,
            options,
            limits,
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xc1; 32])),
            settled_tip,
        )
    }

    fn test_config_with_bounds<'options>(
        staging_path: &'options Path,
        options: &'options WalletColumnFamilyOptions,
        limits: TestByteLimits,
        first_retained_block: BlockId,
        settled_tip: BlockId,
    ) -> WalletProjectionLoadConfig<'options> {
        WalletProjectionLoadConfig {
            staging_path,
            options,
            network: Network::ZcashRegtest,
            first_retained_block,
            settled_tip,
            supported_reorg_depth: 0,
            max_outpoint_sort_memory_bytes: limits.outpoint_sort,
            max_secondary_sort_memory_bytes_per_sorter: limits.secondary_sort_per_sorter,
            max_temporary_file_bytes_per_sorter: limits.temporary_files_per_sorter,
            sst_target_logical_bytes: TEST_SST_LOGICAL_BYTES,
            max_accounted_reorg_undo_bytes: TEST_REORG_UNDO_BYTES,
            accumulator_hashing_workers: NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
            scan_chunk_blocks: DEFAULT_CANONICAL_SCAN_CHUNK_BLOCKS,
        }
    }

    fn canonical_scan(
        replays: Vec<ValidatedCanonicalBlockReplay>,
    ) -> impl Iterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>> {
        replays.into_iter().map(Ok)
    }

    fn validated_replays(
        blocks: &[CanonicalBlockFacts],
    ) -> Result<Vec<ValidatedCanonicalBlockReplay>, Box<dyn Error>> {
        blocks
            .iter()
            .map(|facts| {
                let envelope = encode_canonical_block_replay(
                    facts,
                    CanonicalBlockReplayFormatVersion::V1,
                    CanonicalBlockFactsDigestVersion::V1,
                );
                decode_canonical_block_replay(envelope.as_bytes())
                    .map_err(|error| Box::new(error) as Box<dyn Error>)
            })
            .collect()
    }

    fn oracle_for(
        blocks: &[CanonicalBlockFacts],
    ) -> Result<WalletProjectionSerialOracle, WalletProjectionContractError> {
        let mut oracle = WalletProjectionSerialOracle::new(Network::ZcashRegtest);
        for block in blocks {
            oracle.apply_block(block)?;
        }
        Ok(oracle)
    }

    fn assert_missing_predecessor(
        staging_path: &Path,
        options: &WalletColumnFamilyOptions,
        replays: Vec<ValidatedCanonicalBlockReplay>,
    ) {
        assert!(matches!(
            write_projection_ssts(
                test_config(staging_path, options, TestByteLimits::default()),
                canonical_scan(replays),
            ),
            Err(RocksDbWalletError::Contract(
                WalletProjectionContractError::MissingTransparentPredecessor
            ))
        ));
    }

    fn block_facts(
        height: u32,
        parent_hash: [u8; 32],
        block_hash: [u8; 32],
        transactions: Vec<CanonicalTransactionFacts>,
    ) -> CanonicalBlockFacts {
        CanonicalBlockFacts {
            block_header: BlockHeaderArtifact::new(
                BlockHeight::new(height),
                BlockHash::from_bytes(block_hash),
                BlockHash::from_bytes(parent_hash),
                [0; 32],
                [0; 32],
                i64::from(height),
                0,
                [0; 32],
                0,
                0,
            ),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&block_hash),
            transactions,
        }
    }

    fn transaction_facts(
        transaction_id: TransactionId,
        transparent_inputs: Vec<TransparentInputFact>,
        transparent_outputs: Vec<TransparentOutputFact>,
    ) -> CanonicalTransactionFacts {
        let counts = TransactionComponentCounts {
            transparent_input_count: u32::try_from(transparent_inputs.len()).unwrap_or(u32::MAX),
            transparent_output_count: u32::try_from(transparent_outputs.len()).unwrap_or(u32::MAX),
            ..TransactionComponentCounts::EMPTY
        };
        CanonicalTransactionFacts {
            public_facts: TransactionPublicFacts {
                transaction_id,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::V4,
                consensus_branch_id: None,
                lock_time: LockTime::Unlocked,
                expiry_height: None,
                size_bytes: 32,
                counts,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                privacy_shape: PrivacyShape::Unclassified,
                is_coinbase: false,
                unsupported_sections: Vec::new(),
            },
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                &transaction_id.as_bytes(),
            ),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs,
            transparent_outputs,
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "test fixture constructs a complete certified canonical store in one helper"
    )]
    fn build_ready_test_canonical_store(
        path: &Path,
        blocks: &[CanonicalBlockFacts],
        settled_index: usize,
        reorg_window: u32,
    ) -> Result<zinder_store::RocksDbCanonicalStore, Box<dyn Error>> {
        use zinder_core::{
            ChainTipMetadata, CommitmentTreeCheckpoint, CommitmentTreeFrontiers,
            CompactBlockArtifact, CompactChainMetadata, ConsensusBranchId,
            NetworkUpgradeActivation, NetworkUpgradeActivations, UnixTimestampMillis,
        };
        use zinder_store::{
            CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalReorgPolicy,
            CanonicalStoreBuildPlan, RawBlobRetention, RocksDbCanonicalBuilder,
        };
        let activations_vec = [
            "Overwinter",
            "Sapling",
            "Blossom",
            "Heartwood",
            "Canopy",
            "NU5",
            "NU6",
            "NU6.1",
            "NU6.2",
            "NU6.3",
        ]
        .into_iter()
        .enumerate()
        .map(|(index, name)| NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(u32::try_from(index).unwrap_or(u32::MAX) + 1),
            activation_height: BlockHeight::new(100 + u32::try_from(index).unwrap_or(u32::MAX)),
            name: name.to_owned(),
        })
        .collect();
        let activations = NetworkUpgradeActivations::new(Network::ZcashRegtest, activations_vec)?;
        let tip_facts = blocks.last().ok_or("test blocks must not be empty")?;
        let tip = BlockId::new(
            tip_facts.block_header.height,
            tip_facts.block_header.block_hash,
        );
        let settled_facts = blocks
            .get(settled_index)
            .ok_or("settled index out of range")?;
        let settled_tip = BlockId::new(
            settled_facts.block_header.height,
            settled_facts.block_header.block_hash,
        );
        let plan = CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            tip,
            RawBlobRetention::None,
            CanonicalReorgPolicy::new(reorg_window)?,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            path,
            plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let total = blocks.len();
        let build_blocks = blocks.iter().enumerate().map(|(index, facts)| {
            let height = facts.block_header.height;
            let block_hash = facts.block_header.block_hash;
            let parent_hash = facts.block_header.parent_hash;
            let is_tip = index + 1 == total;
            let compact_block = CompactBlockArtifact::empty(
                BlockId::new(height, block_hash),
                parent_hash,
                u32::try_from(facts.block_header.block_time).unwrap_or(u32::MAX),
                CompactChainMetadata {
                    sapling_commitment_tree_size: 0,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            );
            let tree_state_checkpoint = is_tip.then(|| {
                CommitmentTreeCheckpoint::new(
                    BlockId::new(height, block_hash),
                    u32::try_from(facts.block_header.block_time).unwrap_or(u32::MAX),
                    CommitmentTreeFrontiers::default(),
                )
            });
            let replay_envelope = encode_canonical_block_replay(
                facts,
                CanonicalBlockReplayFormatVersion::V1,
                CanonicalBlockFactsDigestVersion::V1,
            );
            Ok::<_, std::io::Error>(CanonicalBuildBlock {
                facts: facts.clone(),
                replay_envelope,
                compact_block,
                tip_metadata: ChainTipMetadata::new(0, 0, 0),
                tree_state_checkpoint,
                transaction_blobs: Vec::new(),
                block_blob: None,
            })
        });
        builder.bulk_load_blocks(&activations, build_blocks)?;
        builder.load_subtree_roots(std::iter::empty())?;
        let tip_checkpoint = CommitmentTreeCheckpoint::new(
            tip,
            u32::try_from(tip_facts.block_header.block_time).unwrap_or(u32::MAX),
            CommitmentTreeFrontiers::default(),
        );
        builder.confirm_source_tip_checkpoint(&tip_checkpoint)?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            settled_tip,
            UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        Ok(validated.publish_baseline(publication)?)
    }

    fn multi_chunk_canonical_blocks() -> Vec<CanonicalBlockFacts> {
        let mut blocks = Vec::new();
        let mut parent_hash = Network::ZcashRegtest.genesis_hash().as_bytes();
        let mut created_outpoints = Vec::new();
        for height in 1_u32..=15 {
            let mut block_hash = [0_u8; 32];
            block_hash[0] = u8::try_from(height).unwrap_or(0);
            block_hash[31] = 0xAA;
            let mut tx_id_bytes = [0_u8; 32];
            tx_id_bytes[0] = u8::try_from(height).unwrap_or(0);
            tx_id_bytes[1] = 0x10;
            let tx_id = TransactionId::from_bytes(tx_id_bytes);
            let mut address_bytes = [0_u8; 32];
            address_bytes[0] = u8::try_from(height % 4).unwrap_or(0);
            let address = TransparentAddressScriptHash::from_bytes(address_bytes);
            let mut coinbase = transaction_facts(
                tx_id,
                vec![TransparentInputFact::new(
                    0,
                    TransparentOutPoint::COINBASE_SENTINEL,
                )],
                vec![TransparentOutputFact::new(
                    0,
                    1_000 * u64::from(height),
                    vec![0x76, 0xa9],
                    address,
                )],
            );
            coinbase.public_facts.is_coinbase = true;
            created_outpoints.push(TransparentOutPoint::new(tx_id, 0));
            let mut transactions = vec![coinbase];
            // Spend an output from 3 blocks earlier (exercises cross-chunk and
            // settled-to-unsettled spends) on every height >= 4.
            if height >= 4 {
                let mut spend_tx_bytes = [0_u8; 32];
                spend_tx_bytes[0] = u8::try_from(height).unwrap_or(0);
                spend_tx_bytes[1] = 0x20;
                let spend_tx_id = TransactionId::from_bytes(spend_tx_bytes);
                let spent = created_outpoints[usize::try_from(height - 4).unwrap_or(0)];
                let spend_tx = transaction_facts(
                    spend_tx_id,
                    vec![TransparentInputFact::new(0, spent)],
                    vec![TransparentOutputFact::new(
                        0,
                        500 * u64::from(height),
                        vec![0x51],
                        TransparentAddressScriptHash::from_bytes([0xbb; 32]),
                    )],
                );
                transactions.push(spend_tx);
            }
            blocks.push(block_facts(height, parent_hash, block_hash, transactions));
            parent_hash = block_hash;
        }
        blocks
    }

    #[test]
    fn partition_settled_height_chunks_covers_every_height_once() -> Result<(), Box<dyn Error>> {
        let empty = partition_settled_height_chunks(
            BlockHeight::new(5),
            BlockHeight::new(4),
            NonZeroU32::new(4).ok_or("zero chunk")?,
        );
        assert_eq!(empty, Vec::new());

        for span in [1_u32, 2, 3, 4, 7, 16] {
            let chunks = partition_settled_height_chunks(
                BlockHeight::new(1),
                BlockHeight::new(15),
                NonZeroU32::new(span).ok_or("zero chunk")?,
            );
            assert_eq!(
                chunks.first().map(|chunk| chunk.start),
                Some(BlockHeight::new(1))
            );
            assert_eq!(
                chunks.last().map(|chunk| chunk.end),
                Some(BlockHeight::new(15))
            );
            for pair in chunks.windows(2) {
                assert_eq!(pair[0].end.next(), Some(pair[1].start));
            }
            for chunk in &chunks {
                let len = u64::from(chunk.end.value() - chunk.start.value()) + 1;
                assert!(len >= 1 && len <= u64::from(span));
            }
        }

        // u32::MAX upper bound must not overflow.
        let top_chunks = partition_settled_height_chunks(
            BlockHeight::new(u32::MAX - 2),
            BlockHeight::new(u32::MAX),
            NonZeroU32::new(2).ok_or("zero chunk")?,
        );
        assert_eq!(
            top_chunks,
            vec![
                BlockHeightRange::inclusive(
                    BlockHeight::new(u32::MAX - 2),
                    BlockHeight::new(u32::MAX - 1),
                ),
                BlockHeightRange::inclusive(BlockHeight::new(u32::MAX), BlockHeight::new(u32::MAX),),
            ]
        );
        Ok(())
    }

    #[test]
    fn parallel_scan_worker_count_respects_chunks_workers_and_memory_floor()
    -> Result<(), Box<dyn Error>> {
        let four = NonZeroU32::new(4).ok_or("zero")?;
        let eight = NonZeroU32::new(8).ok_or("zero")?;
        assert_eq!(
            parallel_scan_worker_count(four, 10, 4 * MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES).get(),
            4
        );
        assert_eq!(
            parallel_scan_worker_count(eight, 3, 16 * MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES).get(),
            3
        );
        assert_eq!(
            parallel_scan_worker_count(eight, 10, 2 * MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES).get(),
            2
        );
        assert_eq!(
            parallel_scan_worker_count(eight, 10, MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES - 1).get(),
            1
        );
        assert_eq!(
            parallel_scan_worker_count(eight, 0, 16 * MIN_SHARD_OUTPOINT_SORT_MEMORY_BYTES).get(),
            1
        );
        Ok(())
    }

    #[test]
    fn parallel_canonical_scan_matches_serial_with_settled_and_unsettled_suffix()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let blocks = multi_chunk_canonical_blocks();
        let canonical =
            build_ready_test_canonical_store(&temporary.path().join("canonical"), &blocks, 11, 5)?;
        let ready = canonical.ready_evidence();
        let options = test_column_family_options();
        let serial_staging = TempDir::new()?;
        let mut serial_config = test_config_with_bounds(
            serial_staging.path(),
            &options,
            TestByteLimits::default(),
            ready.first_retained_block,
            ready.sequence_checkpoint.through(),
        );
        serial_config.supported_reorg_depth = 5;
        serial_config.accumulator_hashing_workers = NonZeroU32::MIN;
        let serial = write_projection_ssts_from_source(serial_config, &canonical)?;

        for workers in [2_u32, 4, 8] {
            for chunk_blocks in [2_u32, 3, 5] {
                let staging = TempDir::new()?;
                let mut config = serial_config;
                config.staging_path = staging.path();
                config.accumulator_hashing_workers = NonZeroU32::new(workers).ok_or("zero")?;
                config.scan_chunk_blocks = NonZeroU32::new(chunk_blocks).ok_or("zero")?;
                let parallel = write_projection_ssts_from_source(config, &canonical)?;
                assert_eq!(parallel.first_block, serial.first_block);
                assert_eq!(parallel.tip, serial.tip);
                assert_eq!(
                    parallel.source_sequence_digest,
                    serial.source_sequence_digest
                );
                assert_eq!(parallel.projection_digest, serial.projection_digest);
                assert_eq!(
                    parallel.projection_accumulator,
                    serial.projection_accumulator
                );
                assert_eq!(parallel.row_counts, serial.row_counts);
                assert_eq!(parallel.utxo_summary, serial.utxo_summary);
                assert_eq!(parallel.counters, serial.counters);
                assert_eq!(parallel.logical_row_bytes, serial.logical_row_bytes);
                assert_eq!(
                    parallel.row_counts.reorg_undo_count, 3,
                    "workers={workers} chunk={chunk_blocks}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn parallel_canonical_scan_rejects_refused_worker_thread_with_typed_error()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let blocks = multi_chunk_canonical_blocks();
        let canonical =
            build_ready_test_canonical_store(&temporary.path().join("canonical"), &blocks, 11, 5)?;
        let ready = canonical.ready_evidence();
        let options = test_column_family_options();
        for refused_index in [0_usize, 1, 3] {
            let staging = TempDir::new()?;
            let mut config = test_config_with_bounds(
                staging.path(),
                &options,
                TestByteLimits::default(),
                ready.first_retained_block,
                ready.sequence_checkpoint.through(),
            );
            config.supported_reorg_depth = 5;
            config.accumulator_hashing_workers = NonZeroU32::new(4).ok_or("zero")?;
            config.scan_chunk_blocks = NonZeroU32::new(2).ok_or("zero")?;
            REFUSED_CANONICAL_SCAN_THREAD.set(Some(refused_index));
            let outcome = write_projection_ssts_from_source(config, &canonical);
            REFUSED_CANONICAL_SCAN_THREAD.set(None);
            assert!(
                matches!(
                    outcome,
                    Err(RocksDbWalletError::CanonicalScanThreadUnavailable { .. })
                ),
                "refused index {refused_index}"
            );
        }
        Ok(())
    }

    #[test]
    fn parallel_canonical_scan_stops_window_when_worker_unwinds_past_window_capacity()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let blocks = multi_chunk_canonical_blocks();
        // 12 settled blocks with chunk_blocks=1 gives 12 chunks, exceeding the
        // 2-worker claim window (max_in_flight = 8) so siblings would deadlock
        // if an unwinding worker on chunk 0 did not stop and wake the window.
        let canonical =
            build_ready_test_canonical_store(&temporary.path().join("canonical"), &blocks, 12, 5)?;
        let ready = canonical.ready_evidence();
        let options = test_column_family_options();
        let staging = TempDir::new()?;
        let mut config = test_config_with_bounds(
            staging.path(),
            &options,
            TestByteLimits::default(),
            ready.first_retained_block,
            ready.sequence_checkpoint.through(),
        );
        config.supported_reorg_depth = 5;
        config.accumulator_hashing_workers = NonZeroU32::new(2).ok_or("zero")?;
        config.scan_chunk_blocks = NonZeroU32::MIN;
        let _panic_guard = arm_next_window_panic_chunk(0);
        let outcome = write_projection_ssts_from_source(config, &canonical);
        assert!(
            matches!(outcome, Err(RocksDbWalletError::CanonicalScanWorkerStopped)),
            "expected CanonicalScanWorkerStopped after worker unwind, got {:?}",
            outcome.err()
        );
        Ok(())
    }

    #[test]
    fn parallel_canonical_scan_admits_script_larger_than_single_shard_budget()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let mut blocks = multi_chunk_canonical_blocks();
        // Give one settled output a 100 KiB script: exceeds one 64 KiB shard
        // (256 KiB / 4 workers) while fitting well inside the 256 KiB global
        // outpoint-sort memory limit that the serial path accepts.
        blocks[2].transactions[0].transparent_outputs[0].script_pub_key = vec![0x6a_u8; 100 * 1024];
        let canonical =
            build_ready_test_canonical_store(&temporary.path().join("canonical"), &blocks, 12, 5)?;
        let ready = canonical.ready_evidence();
        let options = test_column_family_options();
        let limits = TestByteLimits {
            outpoint_sort: 256 * 1024,
            ..TestByteLimits::default()
        };
        let serial_staging = TempDir::new()?;
        let mut serial_config = test_config_with_bounds(
            serial_staging.path(),
            &options,
            limits,
            ready.first_retained_block,
            ready.sequence_checkpoint.through(),
        );
        serial_config.supported_reorg_depth = 5;
        serial_config.accumulator_hashing_workers = NonZeroU32::MIN;
        let serial = write_projection_ssts_from_source(serial_config, &canonical)?;

        let parallel_staging = TempDir::new()?;
        let mut parallel_config = serial_config;
        parallel_config.staging_path = parallel_staging.path();
        parallel_config.accumulator_hashing_workers = NonZeroU32::new(4).ok_or("zero")?;
        parallel_config.scan_chunk_blocks = NonZeroU32::new(3).ok_or("zero")?;
        let parallel = write_projection_ssts_from_source(parallel_config, &canonical)?;
        assert_eq!(parallel.projection_digest, serial.projection_digest);
        assert_eq!(
            parallel.projection_accumulator,
            serial.projection_accumulator
        );
        assert_eq!(
            parallel.source_sequence_digest,
            serial.source_sequence_digest
        );
        assert_eq!(parallel.row_counts, serial.row_counts);
        assert_eq!(parallel.utxo_summary, serial.utxo_summary);
        assert!(
            parallel.outpoint_sort_evidence.peak_accounted_memory_bytes <= limits.outpoint_sort
        );
        Ok(())
    }

    #[test]
    fn address_transaction_sorter_recycles_outpoint_sort_headroom() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            address_transaction_sort_memory_bytes(4 * 1024 * 1024 * 1024, 1024 * 1024 * 1024),
            2 * 1024 * 1024 * 1024
        );
        assert_eq!(
            address_transaction_sort_memory_bytes(1024 * 1024 * 1024, 256 * 1024 * 1024),
            512 * 1024 * 1024
        );
        assert_eq!(
            address_transaction_sort_memory_bytes(4 * 1024 * 1024 * 1024, 3 * 1024 * 1024 * 1024),
            3 * 1024 * 1024 * 1024
        );

        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let funding = TransactionId::from_bytes([0xb1; 32]);
        let spending = TransactionId::from_bytes([0xb2; 32]);
        // Every created output is spent before the tip, so `address_index_sorter`
        // initializes within `secondary_sort_per_sorter` (>= 88 bytes) while
        // `address_transaction_sorter` recycles half of `max_outpoint_sort_memory_bytes`.
        let blocks = vec![
            block_facts(
                1,
                [0; 32],
                [0xc1; 32],
                vec![transaction_facts(
                    funding,
                    Vec::new(),
                    vec![TransparentOutputFact::new(0, 10, [0x51], address)],
                )],
            ),
            block_facts(
                2,
                [0xc1; 32],
                [0xc2; 32],
                vec![transaction_facts(
                    spending,
                    vec![TransparentInputFact::new(
                        0,
                        TransparentOutPoint::new(funding, 0),
                    )],
                    Vec::new(),
                )],
            ),
        ];
        let tip = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xc2; 32]));
        let prepared = write_projection_ssts(
            test_config_with_settled_tip(
                temporary.path(),
                &options,
                TestByteLimits {
                    outpoint_sort: 4_096,
                    secondary_sort_per_sorter: 96,
                    ..TestByteLimits::default()
                },
                tip,
            ),
            canonical_scan(validated_replays(&blocks)?),
        )?;
        assert_eq!(prepared.row_counts.transparent_unspent_output_count, 0);
        assert_eq!(prepared.row_counts.transparent_spent_output_count, 1);
        assert_eq!(prepared.row_counts.transparent_address_transaction_count, 2);
        let index_evidence = prepared.address_index_sort_evidence;
        let tx_evidence = prepared.address_transaction_sort_evidence;
        assert_eq!(index_evidence.max_accounted_memory_bytes, 96);
        assert_eq!(tx_evidence.max_accounted_memory_bytes, 2_048);
        assert_eq!(tx_evidence.merge_pass_count, 0);
        Ok(())
    }

    #[test]
    fn pipelined_and_concurrent_secondary_sst_parity_matches_serial() -> Result<(), Box<dyn Error>>
    {
        let temporary = TempDir::new()?;
        let blocks = multi_chunk_canonical_blocks();
        let canonical =
            build_ready_test_canonical_store(&temporary.path().join("canonical"), &blocks, 11, 5)?;
        let ready = canonical.ready_evidence();
        let options = test_column_family_options();
        let serial_staging = TempDir::new()?;
        let mut serial_config = test_config_with_bounds(
            serial_staging.path(),
            &options,
            TestByteLimits::default(),
            ready.first_retained_block,
            ready.sequence_checkpoint.through(),
        );
        serial_config.supported_reorg_depth = 5;
        serial_config.sst_target_logical_bytes = 256;
        serial_config.accumulator_hashing_workers = NonZeroU32::MIN;
        let serial = write_projection_ssts_from_source(serial_config, &canonical)?;

        for workers in [2_u32, 3, 4] {
            let staging = TempDir::new()?;
            let mut config = serial_config;
            config.staging_path = staging.path();
            config.accumulator_hashing_workers = NonZeroU32::new(workers).ok_or("zero")?;
            let parallel = write_projection_ssts_from_source(config, &canonical)?;

            assert_eq!(parallel.projection_digest, serial.projection_digest);
            assert_eq!(
                parallel.projection_accumulator,
                serial.projection_accumulator
            );
            assert_eq!(parallel.row_counts, serial.row_counts);
            assert_eq!(parallel.utxo_summary, serial.utxo_summary);
            assert_eq!(parallel.logical_row_bytes, serial.logical_row_bytes);
            assert!(parallel.sst_file_bytes > 0);
            assert_eq!(parallel.sst_file_count, serial.sst_file_count);
            assert_eq!(parallel.families.len(), serial.families.len());
            for (parallel_family, serial_family) in
                parallel.families.iter().zip(serial.families.iter())
            {
                assert_sst_family_entries_match(parallel_family, serial_family)?;
            }

            let secondary = parallel.phase_durations.secondary_row_derivation_phases;
            assert!(
                secondary.address_index_sort + secondary.address_index_drain
                    <= secondary.concurrent_address_derivation
            );
            assert!(
                secondary.address_transaction_sort + secondary.address_transaction_drain
                    <= secondary.concurrent_address_derivation
            );
            assert!(
                secondary.concurrent_address_derivation + secondary.reorg_undo_write
                    <= parallel.phase_durations.secondary_row_derivation
            );
        }
        Ok(())
    }

    fn assert_sst_family_entries_match(
        parallel_family: &PreparedWalletColumnFamily,
        serial_family: &PreparedWalletColumnFamily,
    ) -> Result<(), Box<dyn Error>> {
        assert_eq!(parallel_family.name, serial_family.name);
        assert_eq!(parallel_family.paths.len(), serial_family.paths.len());
        for (parallel_path, serial_path) in
            parallel_family.paths.iter().zip(serial_family.paths.iter())
        {
            assert_eq!(parallel_path.file_name(), serial_path.file_name());
            assert!(std::fs::metadata(parallel_path)?.len() > 0);
        }
        let mut db_options = rust_rocksdb::Options::default();
        db_options.create_if_missing(true);
        let ingest_opts = rust_rocksdb::IngestExternalFileOptions::default();
        let serial_db_dir = TempDir::new()?;
        let parallel_db_dir = TempDir::new()?;
        let serial_db = rust_rocksdb::DB::open(&db_options, serial_db_dir.path())?;
        let parallel_db = rust_rocksdb::DB::open(&db_options, parallel_db_dir.path())?;
        if !serial_family.paths.is_empty() {
            serial_db.ingest_external_file_opts(&ingest_opts, serial_family.paths.clone())?;
        }
        if !parallel_family.paths.is_empty() {
            parallel_db.ingest_external_file_opts(&ingest_opts, parallel_family.paths.clone())?;
        }
        let serial_entries = serial_db
            .iterator(rust_rocksdb::IteratorMode::Start)
            .collect::<Result<Vec<_>, _>>()?;
        let parallel_entries = parallel_db
            .iterator(rust_rocksdb::IteratorMode::Start)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(parallel_entries, serial_entries);
        Ok(())
    }

    #[test]
    fn pipelined_sst_queue_budget_stays_bounded() {
        assert_eq!(pipelined_sst_queue_budget_bytes(0), 64 * 1024);
        assert_eq!(pipelined_sst_queue_budget_bytes(512 * 1024), 64 * 1024);
        assert_eq!(
            pipelined_sst_queue_budget_bytes(8 * 1024 * 1024),
            1024 * 1024
        );
        assert_eq!(
            pipelined_sst_queue_budget_bytes(128 * 1024 * 1024),
            4 * 1024 * 1024
        );
    }

    #[test]
    fn resolve_secondary_branch_outcomes_preserves_root_failure() {
        let index_failed = resolve_secondary_branch_outcomes(
            SecondaryBranchOutcome::Failed(RocksDbWalletError::EmptyCanonicalHistory),
            Ok(SecondaryBranchOutcome::SiblingAborted),
        );
        assert!(matches!(
            index_failed,
            Err(RocksDbWalletError::EmptyCanonicalHistory)
        ));

        let tx_failed = resolve_secondary_branch_outcomes(
            SecondaryBranchOutcome::SiblingAborted,
            Ok(SecondaryBranchOutcome::Failed(
                RocksDbWalletError::AddressTransactionConflict,
            )),
        );
        assert!(matches!(
            tx_failed,
            Err(RocksDbWalletError::AddressTransactionConflict)
        ));

        let tx_stopped = resolve_secondary_branch_outcomes(
            SecondaryBranchOutcome::SiblingAborted,
            Err(Box::new(())),
        );
        assert!(matches!(
            tx_stopped,
            Err(RocksDbWalletError::SecondaryDerivationWorkerStopped)
        ));
    }

    fn sharded_parity_test_blocks() -> Vec<CanonicalBlockFacts> {
        let addr_low = TransparentAddressScriptHash::from_bytes([0x05; 32]);
        let addr_mid = TransparentAddressScriptHash::from_bytes([0x80; 32]);
        let addr_high = TransparentAddressScriptHash::from_bytes([0xf8; 32]);
        let tx_low = TransactionId::from_bytes([0x01; 32]);
        let tx_mid = TransactionId::from_bytes([0x82; 32]);
        let tx_high = TransactionId::from_bytes([0xfe; 32]);
        let tx_unsettled = TransactionId::from_bytes([0x40; 32]);
        let tx_spend_unsettled = TransactionId::from_bytes([0xc0; 32]);
        vec![
            block_facts(
                1,
                [0; 32],
                [0xc1; 32],
                vec![
                    transaction_facts(
                        tx_low,
                        Vec::new(),
                        vec![
                            TransparentOutputFact::new(0, 100, [0x51], addr_low),
                            TransparentOutputFact::new(1, 200, [0x52], addr_high),
                        ],
                    ),
                    transaction_facts(
                        tx_high,
                        Vec::new(),
                        vec![TransparentOutputFact::new(0, 300, [0x53], addr_mid)],
                    ),
                ],
            ),
            block_facts(
                2,
                [0xc1; 32],
                [0xc2; 32],
                vec![transaction_facts(
                    tx_mid,
                    vec![TransparentInputFact::new(
                        0,
                        TransparentOutPoint::new(tx_low, 0),
                    )],
                    vec![TransparentOutputFact::new(0, 90, [0x54], addr_mid)],
                )],
            ),
            block_facts(
                3,
                [0xc2; 32],
                [0xc3; 32],
                vec![transaction_facts(
                    tx_unsettled,
                    Vec::new(),
                    vec![TransparentOutputFact::new(0, 50, [0x55], addr_high)],
                )],
            ),
            block_facts(
                4,
                [0xc3; 32],
                [0xc4; 32],
                vec![transaction_facts(
                    tx_spend_unsettled,
                    vec![TransparentInputFact::new(
                        0,
                        TransparentOutPoint::new(tx_high, 0),
                    )],
                    vec![TransparentOutputFact::new(0, 290, [0x56], addr_low)],
                )],
            ),
        ]
    }

    fn assert_prepared_load_ingests_match_serial(
        serial: &PreparedWalletProjectionLoad,
        sharded: &PreparedWalletProjectionLoad,
        shard_target: u32,
    ) -> Result<(), Box<dyn Error>> {
        assert_eq!(sharded.projection_digest, serial.projection_digest);
        assert_eq!(
            sharded.projection_accumulator,
            serial.projection_accumulator
        );
        assert_eq!(sharded.row_counts, serial.row_counts);
        assert_eq!(sharded.utxo_summary, serial.utxo_summary);
        assert_eq!(sharded.logical_row_bytes, serial.logical_row_bytes);
        assert_eq!(
            sharded.counters.peak_accounted_reorg_undo_bytes,
            serial.counters.peak_accounted_reorg_undo_bytes
        );
        assert_eq!(sharded.families.len(), serial.families.len());
        for (serial_family, sharded_family) in serial.families.iter().zip(sharded.families.iter()) {
            assert_eq!(serial_family.name, sharded_family.name);
            let mut db_options = rust_rocksdb::Options::default();
            db_options.create_if_missing(true);
            let ingest_opts = rust_rocksdb::IngestExternalFileOptions::default();
            let serial_db_dir = TempDir::new()?;
            let sharded_db_dir = TempDir::new()?;
            let serial_db = rust_rocksdb::DB::open(&db_options, serial_db_dir.path())?;
            let sharded_db = rust_rocksdb::DB::open(&db_options, sharded_db_dir.path())?;
            if !serial_family.paths.is_empty() {
                serial_db.ingest_external_file_opts(&ingest_opts, serial_family.paths.clone())?;
            }
            if !sharded_family.paths.is_empty() {
                sharded_db.ingest_external_file_opts(&ingest_opts, sharded_family.paths.clone())?;
            }
            let serial_entries = serial_db
                .iterator(rust_rocksdb::IteratorMode::Start)
                .collect::<Result<Vec<_>, _>>()?;
            let sharded_entries = sharded_db
                .iterator(rust_rocksdb::IteratorMode::Start)
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(
                sharded_entries, serial_entries,
                "family {} differed for {shard_target} shards",
                serial_family.name
            );
        }
        Ok(())
    }

    #[test]
    fn sharded_outpoint_and_address_tx_builds_match_serial_and_ingest_into_rocksdb()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let blocks = sharded_parity_test_blocks();
        let first = BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xc1; 32]));
        let settled_tip = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xc2; 32]));
        let mut serial_cfg = test_config_with_bounds(
            temporary.path(),
            &options,
            TestByteLimits::default(),
            first,
            settled_tip,
        );
        serial_cfg.supported_reorg_depth = 5;
        let serial = write_projection_ssts_with_plan(
            serial_cfg,
            canonical_scan(validated_replays(&blocks)?),
            resolve_build_shard_plan_for(
                NonZeroU32::MIN,
                serial_cfg.max_outpoint_sort_memory_bytes,
                serial_cfg.max_secondary_sort_memory_bytes_per_sorter,
                active_merge_fan_in(),
                Some(NonZeroU32::MIN),
            ),
        )?;

        for shard_target in [2_u32, 3, 4, 8] {
            let forced = NonZeroU32::new(shard_target).ok_or("nonzero")?;
            let sharded_dir = temporary.path().join(format!("sharded-{shard_target}"));
            fs::create_dir_all(&sharded_dir)?;
            let mut sharded_cfg = test_config_with_bounds(
                &sharded_dir,
                &options,
                TestByteLimits::default(),
                first,
                settled_tip,
            );
            sharded_cfg.supported_reorg_depth = 5;
            sharded_cfg.accumulator_hashing_workers =
                NonZeroU32::new(4).ok_or("nonzero workers")?;
            let plan = resolve_build_shard_plan_for(
                sharded_cfg.accumulator_hashing_workers,
                sharded_cfg.max_outpoint_sort_memory_bytes,
                sharded_cfg.max_secondary_sort_memory_bytes_per_sorter,
                active_merge_fan_in(),
                Some(forced),
            );
            let sharded = write_projection_ssts_with_plan(
                sharded_cfg,
                canonical_scan(validated_replays(&blocks)?),
                plan,
            )?;
            assert_prepared_load_ingests_match_serial(&serial, &sharded, shard_target)?;
        }
        Ok(())
    }

    fn assert_total_secondary_fan_in(
        plan: BuildShardPlan,
        active_fan_in: usize,
    ) -> Result<(), Box<dyn Error>> {
        let idx_readers = usize::try_from(plan.address_index_push_shards.get())?;
        let tx_readers = usize::try_from(plan.address_transaction_shards.get())?;
        let total = if plan.overlap_secondary {
            idx_readers * plan.address_index_per_shard_fan_in
                + tx_readers * plan.address_transaction_per_shard_fan_in
        } else {
            (idx_readers * plan.address_index_per_shard_fan_in)
                .max(tx_readers * plan.address_transaction_per_shard_fan_in)
        };
        assert!(
            total <= active_fan_in,
            "total concurrent secondary fan-in {total} exceeds active_fan_in {active_fan_in} for {plan:?}"
        );
        Ok(())
    }

    #[test]
    fn build_shard_plan_clamps_small_containers_and_bounds_fan_in_and_queues()
    -> Result<(), Box<dyn Error>> {
        const MIB: u64 = 1024 * 1024;
        const GIB: u64 = 1024 * MIB;
        let workers_eight = NonZeroU32::new(8).ok_or("nonzero")?;
        let workers_one = NonZeroU32::MIN;

        let sync_plan = resolve_build_shard_plan_for(workers_one, 4 * GIB, GIB, 256, None);
        assert_eq!(sync_plan.outpoint_shards.get(), 1);
        assert_eq!(sync_plan.address_transaction_shards.get(), 1);
        assert!(!sync_plan.overlap_secondary);

        let scaled_budget = |limit: u64, percent: u64, min_floor: u64, default: u64| {
            (limit.saturating_mul(percent) / 100).clamp(min_floor, default)
        };

        // 2 GiB container clamps shards safely below MIN_PRODUCTION_SHARD_SORT_MEMORY_BYTES.
        let out_2g = scaled_budget(2 * GIB, 40, 64 * MIB, 4 * GIB);
        let sec_2g = scaled_budget(2 * GIB, 10, 32 * MIB, GIB);
        let plan_2g = resolve_build_shard_plan_for(workers_eight, out_2g, sec_2g, 256, None);
        assert_eq!(plan_2g.outpoint_shards.get(), 3);
        assert_eq!(plan_2g.address_transaction_shards.get(), 1);
        assert!(plan_2g.overlap_secondary);
        assert_total_secondary_fan_in(plan_2g, 256)?;

        // 6 GiB and 10 GiB containers never exceed fan-in or 8-shard cap.
        for container in [6 * GIB, 10 * GIB] {
            let out_mem = scaled_budget(container, 40, 64 * MIB, 4 * GIB);
            let sec_mem = scaled_budget(container, 10, 32 * MIB, GIB);
            let plan = resolve_build_shard_plan_for(workers_eight, out_mem, sec_mem, 256, None);
            assert!(plan.outpoint_shards.get() <= MAX_KEY_PREFIX_DRAIN_SHARDS);
            assert!(plan.address_transaction_shards.get() <= MAX_KEY_PREFIX_DRAIN_SHARDS);
            assert!(
                out_mem / u64::from(plan.outpoint_shards.get())
                    >= MIN_PRODUCTION_SHARD_SORT_MEMORY_BYTES
            );
            assert_total_secondary_fan_in(plan, 256)?;
        }

        // Low NOFILE budget and forced shard overrides bound concurrent readers strictly <= safe_fan_in.
        let tight_nofile = resolve_build_shard_plan_for(workers_eight, 4 * GIB, GIB, 6, None);
        assert_eq!(tight_nofile.outpoint_shards.get(), 1);
        assert_eq!(tight_nofile.address_index_push_shards.get(), 1);
        assert_eq!(tight_nofile.address_transaction_shards.get(), 1);
        assert_total_secondary_fan_in(tight_nofile, 6)?;

        let capped_override =
            resolve_build_shard_plan_for(workers_eight, 4 * GIB, GIB, 16, Some(workers_eight));
        assert_total_secondary_fan_in(capped_override, 16)?;

        // Cumulative SST pipeline queue stays <= 16 MiB across any writer count.
        for open_writers in [1_usize, 2, 4, 8, 16, 64] {
            if let Some(budget) = scaled_pipelined_sst_queue_budget_bytes(128 * MIB, open_writers) {
                assert!(budget >= MIN_PIPELINED_SST_QUEUE_BUDGET_BYTES);
                assert!(
                    budget * u64::try_from(open_writers)? <= PIPELINED_SST_TOTAL_QUEUE_CAP_BYTES
                );
            }
        }
        assert_eq!(
            scaled_pipelined_sst_queue_budget_bytes(128 * MIB, 512),
            None
        );
        Ok(())
    }

    #[test]
    fn resolve_secondary_phase_fan_in_adapts_to_post_outpoint_headroom()
    -> Result<(), Box<dyn Error>> {
        let one = NonZeroU32::MIN;
        let two = NonZeroU32::new(2).ok_or("nonzero")?;

        let asymmetric = resolve_secondary_phase_fan_in(one, one, true, 48)?;
        assert!(asymmetric.overlap_secondary);
        assert_eq!(asymmetric.address_index_per_shard_fan_in, 12);
        assert_eq!(asymmetric.address_transaction_per_shard_fan_in, 36);

        let tight_overlap = resolve_secondary_phase_fan_in(one, two, true, 6)?;
        assert!(tight_overlap.overlap_secondary);
        assert_eq!(tight_overlap.address_index_per_shard_fan_in, 2);
        assert_eq!(tight_overlap.address_transaction_per_shard_fan_in, 2);

        let fallback_seq = resolve_secondary_phase_fan_in(two, two, true, 6)?;
        assert!(!fallback_seq.overlap_secondary);
        assert_eq!(fallback_seq.address_index_per_shard_fan_in, 3);
        assert_eq!(fallback_seq.address_transaction_per_shard_fan_in, 3);

        let too_small = resolve_secondary_phase_fan_in(two, two, true, 3);
        assert!(matches!(
            too_small,
            Err(RocksDbWalletError::BulkLoad(
                BulkLoadError::InvalidInput { .. }
            ))
        ));
        Ok(())
    }

    fn low_nofile_blocks(output_count: usize) -> Result<Vec<CanonicalBlockFacts>, Box<dyn Error>> {
        let mut txs = Vec::with_capacity(output_count);
        for index in 0..output_count {
            let low = u8::try_from(index)?;
            let mut tx_bytes = [0_u8; 32];
            tx_bytes[0] = low.wrapping_mul(7);
            tx_bytes[1] = low;
            let mut addr_bytes = [0_u8; 32];
            addr_bytes[0] = low.wrapping_mul(11);
            addr_bytes[1] = 0x50_u8.wrapping_add(low);
            txs.push(transaction_facts(
                TransactionId::from_bytes(tx_bytes),
                Vec::new(),
                vec![TransparentOutputFact::new(
                    0,
                    100 + u64::from(low),
                    [0x51],
                    TransparentAddressScriptHash::from_bytes(addr_bytes),
                )],
            ));
        }
        Ok(vec![
            block_facts(1, [0; 32], [0xc1; 32], txs),
            block_facts(2, [0xc1; 32], [0xc2; 32], Vec::new()),
        ])
    }

    #[cfg(unix)]
    fn run_low_nofile_child_builds() -> Result<(), Box<dyn Error>> {
        const GIB: u64 = 1024 * 1024 * 1024;
        let initial_fan_in = active_merge_fan_in();
        assert!(initial_fan_in >= 12);
        assert!(initial_fan_in < 128);
        let run_target = initial_fan_in.saturating_sub(6).clamp(16, 40);
        let blocks = low_nofile_blocks(run_target)?;
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let limits = TestByteLimits {
            outpoint_sort: 192,
            secondary_sort_per_sorter: TEST_SORT_MEMORY_BYTES,
            temporary_files_per_sorter: TEST_TEMPORARY_FILE_BYTES,
        };
        let first = BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xc1; 32]));
        let settled_tip = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xc2; 32]));

        let run_with_plan = |subdir: &str, workers: NonZeroU32, plan: BuildShardPlan| {
            let dir = temporary.path().join(subdir);
            fs::create_dir_all(&dir)?;
            let mut cfg = test_config_with_bounds(&dir, &options, limits, first, settled_tip);
            cfg.accumulator_hashing_workers = workers;
            write_projection_ssts_with_plan(cfg, canonical_scan(validated_replays(&blocks)?), plan)
                .map_err(|err| Box::new(err) as Box<dyn Error>)
        };

        let serial_plan = resolve_build_shard_plan_for(
            NonZeroU32::MIN,
            4 * GIB,
            GIB,
            initial_fan_in,
            Some(NonZeroU32::MIN),
        );
        let serial = run_with_plan("serial", NonZeroU32::MIN, serial_plan)?;
        assert!(serial.outpoint_sort_evidence.initial_run_count >= 16);

        let w_four = NonZeroU32::new(4).ok_or("nonzero workers")?;
        let prod_plan = resolve_build_shard_plan_for(w_four, 4 * GIB, GIB, initial_fan_in, None);
        let prod = run_with_plan("prod", w_four, prod_plan)?;
        assert_eq!(prod.projection_digest, serial.projection_digest);

        let three = NonZeroU32::new(3).ok_or("nonzero shards")?;
        let sharded_plan =
            resolve_build_shard_plan_for(w_four, 4 * GIB, GIB, initial_fan_in, Some(three));
        let sharded = run_with_plan("sharded", w_four, sharded_plan)?;
        assert_eq!(sharded.projection_digest, serial.projection_digest);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn sharded_projection_build_succeeds_under_low_file_descriptor_limit()
    -> Result<(), Box<dyn Error>> {
        const CHILD_ENV: &str = "ZINDER_WALLET_TEST_LOW_NOFILE_CHILD";
        if std::env::var_os(CHILD_ENV).is_some() {
            return run_low_nofile_child_builds();
        }
        let binary = std::env::current_exe()?;
        let status = std::process::Command::new("sh")
            .args([
                "-c",
                "cur=\"$(ulimit -n)\"; if [ \"$cur\" = \"unlimited\" ] || [ \"$cur\" -gt 112 ]; then ulimit -n 112; fi; exec \"$@\"",
                "sh",
            ])
            .arg(&binary)
            .args([
                "--exact",
                "projection_load::tests::sharded_projection_build_succeeds_under_low_file_descriptor_limit",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .status()?;
        assert!(status.success(), "child process failed with {status}");
        Ok(())
    }

    fn build_test_retained_undo_suffix(
        settled_tip: BlockId,
        limit_bytes: u64,
    ) -> Result<RetainedUndo, Box<dyn Error>> {
        let digest = CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            CanonicalBlockFactsSequenceDigestVersion::V1,
            1,
            [2; 32],
        );
        let mut undo = RetainedUndo::new(4, settled_tip, limit_bytes);
        for offset in 1..=4_u32 {
            let height = BlockHeight::new(settled_tip.height.value() + offset);
            let hash = BlockHash::from_bytes([u8::try_from(offset)?; 32]);
            undo.begin_block(BlockId::new(height, hash), hash, digest, digest)?;
            undo.finish_block()?;
        }
        Ok(undo)
    }

    fn assert_tight_undo_collector_budget_clamp_and_refusal(
        settled_tip: BlockId,
        key_bytes: u64,
    ) -> Result<(), Box<dyn Error>> {
        let base_only = 4 * u64::try_from(size_of::<WalletReorgUndo>())?;
        let tight_limit = base_only + 3 * key_bytes;
        let mut tight_retained = build_test_retained_undo_suffix(settled_tip, tight_limit)?;
        let tight_collector = RetainedUndoCollector::from_retained_undo(&mut tight_retained);
        let height = BlockHeight::new(settled_tip.height.value() + 1);
        for seq in 0..3_u32 {
            tight_collector.try_collect(WalletAddressTransactionKey::new(
                TransparentAddressScriptHash::from_bytes([0x44; 32]),
                height,
                seq,
            ))?;
        }
        {
            let first_slot = tight_collector.slots[0].lock();
            assert_eq!(first_slot.len(), 3);
            assert_eq!(first_slot.capacity(), 3);
        }
        assert_eq!(
            tight_collector.accounted_bytes.load(Ordering::Acquire),
            tight_limit
        );
        let refused = tight_collector.try_collect(WalletAddressTransactionKey::new(
            TransparentAddressScriptHash::from_bytes([0x44; 32]),
            height,
            3,
        ));
        assert!(matches!(
            refused,
            Err(RocksDbWalletError::AccountedReorgUndoMemoryLimit {
                limit_bytes,
                required_bytes,
            }) if limit_bytes == tight_limit && required_bytes == tight_limit + key_bytes
        ));
        let first_slot = tight_collector.slots[0].lock();
        assert_eq!(first_slot.len(), 3);
        assert_eq!(first_slot.capacity(), 3);
        drop(first_slot);
        tight_collector.apply_collected(&mut tight_retained)?;
        assert_eq!(tight_retained.peak_accounted_bytes, tight_limit);
        Ok(())
    }

    #[test]
    fn retained_undo_collector_accounts_capacity_with_zero_handoff_overlap()
    -> Result<(), Box<dyn Error>> {
        let settled_tip = BlockId::new(BlockHeight::new(100), BlockHash::from_bytes([1; 32]));
        let mut retained = build_test_retained_undo_suffix(settled_tip, 1024 * 1024)?;
        let base_bytes = retained.accounted_bytes;
        let collector = RetainedUndoCollector::from_retained_undo(&mut retained);
        let key_bytes = u64::try_from(size_of::<WalletAddressTransactionKey>())?;

        std::thread::scope(|scope| -> Result<(), Box<dyn Error>> {
            let mut handles = Vec::new();
            for worker_id in 0..4_u32 {
                let collector_ref = &collector;
                handles.push(scope.spawn(move || -> Result<(), RocksDbWalletError> {
                    for offset in 1..=4_u32 {
                        let height = BlockHeight::new(settled_tip.height.value() + offset);
                        for seq in 0..10_u32 {
                            let tx_index = worker_id * 10 + seq;
                            let mut addr = [0_u8; 32];
                            addr[0] = u8::try_from(offset).unwrap_or(0);
                            addr[1] = u8::try_from(worker_id).unwrap_or(0);
                            collector_ref.try_collect(WalletAddressTransactionKey::new(
                                TransparentAddressScriptHash::from_bytes(addr),
                                height,
                                tx_index,
                            ))?;
                        }
                    }
                    collector_ref.try_collect(WalletAddressTransactionKey::new(
                        TransparentAddressScriptHash::from_bytes([0xff; 32]),
                        settled_tip.height,
                        worker_id,
                    ))
                }));
            }
            for handle in handles {
                handle.join().map_err(|_| "collector worker panicked")??;
            }
            Ok(())
        })?;

        let mut slot_cap_bytes = 0_u64;
        let mut slot_layouts = Vec::with_capacity(collector.slots.len());
        for slot_lock in &collector.slots {
            let slot = slot_lock.lock();
            slot_cap_bytes =
                slot_cap_bytes.saturating_add(u64::try_from(slot.capacity())? * key_bytes);
            slot_layouts.push((slot.as_ptr(), slot.len(), slot.capacity()));
        }
        assert_eq!(
            collector.accounted_bytes.load(Ordering::Acquire),
            base_bytes + slot_cap_bytes
        );
        let peak_before_apply = collector.peak_accounted_bytes.load(Ordering::Acquire);
        assert_eq!(peak_before_apply, base_bytes + slot_cap_bytes);
        assert!(peak_before_apply <= retained.max_accounted_bytes);

        collector.apply_collected(&mut retained)?;
        assert_eq!(retained.peak_accounted_bytes, peak_before_apply);
        assert_eq!(retained.accounted_bytes, base_bytes + slot_cap_bytes);
        for (undo, (ptr, len, cap)) in retained.records.iter().zip(slot_layouts) {
            assert_eq!(undo.address_transaction_keys.as_ptr(), ptr);
            assert_eq!(undo.address_transaction_keys.len(), len);
            assert_eq!(undo.address_transaction_keys.capacity(), cap);
            assert_eq!(len, 40);
        }

        assert_tight_undo_collector_budget_clamp_and_refusal(settled_tip, key_bytes)
    }

    #[test]
    fn canonical_scan_duration_includes_block_hash_sst_flush() -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = test_column_family_options();
        let first = BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xc1; 32]));
        let settled_tip = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xc2; 32]));
        let config = test_config_with_bounds(
            temporary.path(),
            &options,
            TestByteLimits::default(),
            first,
            settled_tip,
        );
        let blocks = vec![
            block_facts(1, [0; 32], [0xc1; 32], Vec::new()),
            block_facts(2, [0xc1; 32], [0xc2; 32], Vec::new()),
        ];

        let scanned = scan_canonical_replay_serial(
            config,
            canonical_scan(validated_replays(&blocks)?),
            NonZeroU32::MIN,
        )?;
        assert_ne!(scanned.block_hash_files.paths, [] as [PathBuf; 0]);
        assert!(scanned.block_hash_write_duration <= scanned.canonical_scan);
        assert!(scanned.block_hash_finish_duration <= scanned.canonical_scan);
        assert!(scanned.outpoint_sort_durations.initial_run_spill <= scanned.canonical_scan);

        let finish_staging = TempDir::new()?;
        let finish_config = test_config_with_bounds(
            finish_staging.path(),
            &options,
            TestByteLimits::default(),
            first,
            settled_tip,
        );
        let mut sink = BlockHashStageSink::new(finish_config)?;
        for offset in 1..=64_u32 {
            sink.append_block(BlockId::new(
                BlockHeight::new(offset),
                BlockHash::from_bytes([u8::try_from(offset % 251)?; 32]),
            ))?;
        }
        let sorter = ShardedKeyPrefixSorter::<OUTPOINT_EVENT_KEY_BYTES>::new(
            finish_staging.path(),
            "wallet-outpoint-events-flush-timing",
            NonZeroU32::MIN,
            TEST_SORT_MEMORY_BYTES,
            TEST_TEMPORARY_FILE_BYTES,
        )?;
        let digest = CanonicalBlockFactsSequenceDigestBuilder::new(
            CanonicalBlockFactsSequenceDigestVersion::V1,
        )
        .finish();
        let scan_started = Instant::now();
        let completed = finish_canonical_scan(
            CanonicalScanOutcome {
                first_block: first,
                tip: settled_tip,
                source_sequence_digest: digest,
                retained_undo: RetainedUndo::new(0, settled_tip, TEST_REORG_UNDO_BYTES),
                counters: ScanCounters::default(),
                scan_started,
            },
            sorter,
            sink,
        )?;
        assert_ne!(completed.block_hash_files.paths, [] as [PathBuf; 0]);
        assert!(completed.block_hash_finish_duration <= completed.canonical_scan);
        Ok(())
    }
}
