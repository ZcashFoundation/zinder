//! Spike-only: runs the wallet store over the canonical database.
//!
//! Throwaway code for the combined-commit benchmark. The six wallet row
//! families live in the canonical database next to the canonical families,
//! and the wallet control record moves to its own column family because the
//! canonical admission owns the default family's `store_control` key.

use std::num::NonZeroU64;

use rust_rocksdb::{Cache, ColumnFamilyDescriptor, IteratorMode, WriteBatch};
use zinder_core::{BlockId, Network, ValidatedCanonicalBlockReplay};
use zinder_store::{
    BoundedRocksDbOpen, CanonicalEventFence, CanonicalRetainedEvent, CanonicalStoreError,
    RocksDbResourceBudget,
};
use zinder_wallet_projection::{
    WALLET_STORE_CONTROL_KEY, WalletCanonicalSourceIdentity, WalletProjectionBuildState,
    WalletProjectionReadyEvidence, WalletStoreControlRecord,
};

use crate::{
    RocksDbWalletError, RocksDbWalletStore,
    store::{
        WALLET_DATA_COLUMN_FAMILIES, WalletColdValidationConfig, column_family,
        validate_ready_rows_by_readback, wallet_column_family_descriptors, wallet_data_options,
    },
    transition::SpikeWalletPlanOps,
};

/// Column family holding the wallet control record in a combined database.
pub const SPIKE_WALLET_CONTROL_COLUMN_FAMILY: &str = "wallet_store_control";

/// The six wallet row families.
#[must_use]
pub const fn spike_wallet_data_column_family_names() -> [&'static str; 6] {
    WALLET_DATA_COLUMN_FAMILIES
}

/// Every wallet column family a combined database adds (rows plus control).
#[must_use]
pub fn spike_wallet_column_family_names() -> Vec<String> {
    WALLET_DATA_COLUMN_FAMILIES
        .into_iter()
        .chain(std::iter::once(SPIKE_WALLET_CONTROL_COLUMN_FAMILY))
        .map(str::to_owned)
        .collect()
}

/// Production wallet column-family options for every combined wallet family.
#[must_use]
pub fn spike_wallet_column_family_descriptors(
    block_cache: &Cache,
    wallet_budget: RocksDbResourceBudget,
) -> Vec<ColumnFamilyDescriptor> {
    let mut descriptors = wallet_column_family_descriptors(block_cache, wallet_budget);
    descriptors.push(ColumnFamilyDescriptor::new(
        SPIKE_WALLET_CONTROL_COLUMN_FAMILY,
        wallet_data_options(block_cache, wallet_budget),
    ));
    descriptors
}

/// Production wallet column-family options, for mirror families.
#[must_use]
pub fn spike_wallet_data_options(
    block_cache: &Cache,
    wallet_budget: RocksDbResourceBudget,
) -> rust_rocksdb::Options {
    wallet_data_options(block_cache, wallet_budget)
}

/// Decodes the singleton wallet control record from `family_name`.
pub fn spike_decode_wallet_control(
    bounded_open: &BoundedRocksDbOpen,
    family_name: &'static str,
) -> Result<WalletStoreControlRecord, RocksDbWalletError> {
    decode_control_in_family(bounded_open, family_name)
}

pub(crate) fn decode_control_in_family(
    bounded_open: &BoundedRocksDbOpen,
    family_name: &'static str,
) -> Result<WalletStoreControlRecord, RocksDbWalletError> {
    let family = column_family(bounded_open, family_name)?;
    let mut iterator = bounded_open.db.iterator_cf(&family, IteratorMode::Start);
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

/// A planned wallet transition sharing the caller's batch.
pub struct SpikePlannedWalletTransition {
    /// Caller batch extended with every wallet row and the next control.
    pub batch: WriteBatch,
    /// State to adopt after the batch is durably written.
    pub pending: SpikePendingWalletPublication,
    /// Plan size and read evidence.
    pub stats: SpikeWalletPlanStats,
    /// Recorded operations when requested.
    pub ops: Option<SpikeWalletPlanOps>,
}

/// Next control and READY evidence of a planned transition.
pub struct SpikePendingWalletPublication {
    next_control: WalletStoreControlRecord,
    ready_evidence: WalletProjectionReadyEvidence,
}

/// Logical plan bytes as accounted against the transition ceiling.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpikeWalletPlanStats {
    /// Keys plus values written to the batch, including the control record.
    pub batch_logical_bytes: u64,
    /// Keys plus values held by the planner overlay.
    pub overlay_logical_bytes: u64,
    /// Point reads not served by the overlay.
    pub point_reads: u64,
}

/// Cold row readback evidence for a combined wallet.
#[derive(Clone, Debug)]
pub struct SpikeColdReadback {
    /// READY evidence whose digest, counts, and UTXO summary matched the rows.
    pub ready_evidence: WalletProjectionReadyEvidence,
    /// Wall time of the readback.
    pub elapsed: std::time::Duration,
}

impl RocksDbWalletStore {
    /// Admits a READY wallet whose families live in an already open database.
    pub fn spike_open_shared(
        bounded_open: BoundedRocksDbOpen,
        expected_network: Network,
    ) -> Result<Self, RocksDbWalletError> {
        for name in WALLET_DATA_COLUMN_FAMILIES {
            column_family(&bounded_open, name)?;
        }
        let control = decode_control_in_family(&bounded_open, SPIKE_WALLET_CONTROL_COLUMN_FAMILY)?;
        if control.network != expected_network {
            return Err(RocksDbWalletError::NetworkMismatch {
                expected: expected_network,
                observed: control.network,
            });
        }
        let WalletProjectionBuildState::Ready(ready_evidence) = &control.build_state else {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "combined wallet control is not READY",
            });
        };
        Ok(Self {
            bounded_open,
            ready_evidence: ready_evidence.clone(),
            control,
            control_family: Some(SPIKE_WALLET_CONTROL_COLUMN_FAMILY),
        })
    }

    /// Plans one retained canonical event into the caller's batch.
    ///
    /// Runs the same planner as `apply_canonical_event_range`; nothing is
    /// written. The caller adds its canonical rows, writes the batch synced,
    /// then calls [`Self::spike_complete_planned_transition`].
    #[allow(
        clippy::too_many_arguments,
        reason = "spike mirrors the public transition boundary"
    )]
    pub fn spike_plan_canonical_event_range<I>(
        &self,
        expected_source: WalletCanonicalSourceIdentity,
        event: CanonicalRetainedEvent,
        resulting_fence: CanonicalEventFence,
        resulting_settled_tip: BlockId,
        max_logical_bytes: NonZeroU64,
        replay_rows: I,
        batch: WriteBatch,
        record_ops: bool,
    ) -> Result<SpikePlannedWalletTransition, RocksDbWalletError>
    where
        I: IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
    {
        let planned = self.plan_canonical_event_range(
            expected_source,
            event,
            resulting_fence,
            resulting_settled_tip,
            max_logical_bytes,
            replay_rows,
            batch,
            record_ops,
        )?;
        let (batch, next_control, logical_bytes) =
            self.prepare_publication(planned.batch, &planned.ready_evidence, planned.logical_bytes)?;
        Ok(SpikePlannedWalletTransition {
            batch,
            pending: SpikePendingWalletPublication {
                next_control,
                ready_evidence: planned.ready_evidence,
            },
            stats: SpikeWalletPlanStats {
                batch_logical_bytes: logical_bytes.batch,
                overlay_logical_bytes: logical_bytes.overlay,
                point_reads: planned.point_reads,
            },
            ops: planned.ops,
        })
    }

    /// Adopts a planned transition after its batch was durably written.
    pub fn spike_complete_planned_transition(
        &mut self,
        pending: SpikePendingWalletPublication,
    ) -> Result<(), RocksDbWalletError> {
        self.finish_publication(pending.next_control, pending.ready_evidence)
    }

    /// Recomputes digest, row counts, and UTXO summary from the rows and
    /// requires them to equal the READY evidence.
    pub fn spike_cold_readback(
        &self,
        max_accounted_reorg_undo_bytes: u64,
    ) -> Result<SpikeColdReadback, RocksDbWalletError> {
        let started = std::time::Instant::now();
        validate_ready_rows_by_readback(
            &self.bounded_open,
            self.control.network,
            &self.ready_evidence,
            WalletColdValidationConfig {
                max_accounted_reorg_undo_bytes,
            },
        )?;
        Ok(SpikeColdReadback {
            ready_evidence: self.ready_evidence.clone(),
            elapsed: started.elapsed(),
        })
    }

    /// Supported reorg depth recorded in the control record.
    #[must_use]
    pub const fn spike_supported_reorg_depth(&self) -> u32 {
        self.control.supported_reorg_depth
    }
}
