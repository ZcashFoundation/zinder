//! In-process materialized-view replay driven by canonical storage.
//!
//! The tailer opens a materialized-view store as a primary, follows an
//! in-process canonical secondary, hydrates each transition's committed block
//! contexts, and hands those contexts to
//! [`zinder_materialized_views::MaterializedViewStore::write_chain_event`].
//! Consumer writes and cursor advances land in one materialized-view write
//! batch per dispatched page.
//!
//! Reader processes (`zinder-query` and `zinder-explorer`) open the same
//! materialized-view store path in secondary mode (per
//! [`zinder_materialized_views::MaterializedViewStore::open_secondary`]) and advance their view via
//! [`zinder_materialized_views::MaterializedViewStore::try_catch_up`].

use std::{
    collections::HashMap,
    num::NonZeroU32,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use parking_lot::{Mutex, MutexGuard, RwLock};
use prost::Message as _;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zinder_core::{BlockHeight, BlockHeightRange, ChainEpoch, NetworkUpgradeActivations};
use zinder_materialized_views::{
    BLOCK_PRODUCTION_TIME_CONSUMER_NAME, BLOCK_SUMMARY_COLUMN_FAMILY, BlockCommitContext,
    BlockKeyedConsumer, BlockProductionTimeConsumer, CONVENTIONAL_FEE_DISTRIBUTION_CONSUMER_NAME,
    ChainEventBlockJoin, ChainEventDispatchConsumers, ChainEventDispatchInputs,
    ConventionalFeeDistributionConsumer, MaterializedViewConsumer, MaterializedViewConsumerName,
    MaterializedViewDispatch, MaterializedViewPreset, MaterializedViewState, MaterializedViewStore,
    MaterializedViewStoreOptions, MaterializedViewWriteMeasurement,
    PAID_FEE_DISTRIBUTION_CONSUMER_NAME, TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME,
    TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME, TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY,
    TransactionComponentSummaryConsumer, VALUE_POOL_FLOW_HISTORY_CONSUMER_NAME,
    bundled_materialized_view_consumer_definitions,
};
use zinder_proto::v1::wallet::{MaterializedViewHealth, MaterializedViewStatus};
use zinder_runtime::{ComposedReadiness, IngestPhase, Readiness, ReadinessCause};
use zinder_store::{
    BlockHashLookup, CanonicalEventCursor, CanonicalEventHistoryRequest, CanonicalEventKind,
    CanonicalRetainedEvent, CanonicalStoreError, ChainEpochCommitted, ChainEvent,
    ChainRangeReverted, MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS, RocksDbCanonicalSecondary,
    RocksDbResourceBudget,
};

use crate::{
    CanonicalBlockContextReader, IngestError, MaterializedViewReplayConfig,
    MaterializedViewReplayPolicy,
    chain_ingest::{ingest_error_class, outcome_status},
    conventional_fee_distribution_backfill::seed_conventional_fee_distribution_visible_tail,
    memory_pressure::RuntimeMemorySnapshot,
    require_genesis_complete_history,
    runtime_config::{HistoricalWorkGate, nonzero_u32, sleep_or_cancel},
    transaction_component_backfill::seed_transaction_component_visible_tail,
};

const MATERIALIZED_VIEW_REPLAY_STAGE_READ_EVENTS: &str = "read_events";
const MATERIALIZED_VIEW_REPLAY_STAGE_HYDRATE_BLOCKS: &str = "hydrate_blocks";
const MATERIALIZED_VIEW_REPLAY_STAGE_DISPATCH_EVENT: &str = "dispatch_event";
const MATERIALIZED_VIEW_WRITE_SOURCE_CHAIN_EVENT: &str = "chain_event";
static MATERIALIZED_VIEW_WRITE_LOCK: Mutex<()> = parking_lot::const_mutex(());

pub(crate) fn materialized_view_write_guard() -> MutexGuard<'static, ()> {
    MATERIALIZED_VIEW_WRITE_LOCK.lock()
}

/// Default poll cadence for the materialized-view tailer when the canonical store is
/// ingesting faster than chain-event notifications arrive.
pub const DEFAULT_MATERIALIZED_VIEW_TAILER_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Retained canonical transitions read in one event page.
const MATERIALIZED_VIEW_REPLAY_EVENT_PAGE: NonZeroU32 = nonzero_u32(256);

/// Cadence for refreshing the persisted [`MaterializedViewStatus`] while one
/// catch-up pass stays inside a long rebuild.
const MATERIALIZED_VIEW_STATUS_PERSIST_INTERVAL: Duration = Duration::from_secs(1);

/// Canonical blocks one backfill tail-seed batch hydrates.
const BACKFILL_TAIL_SEED_BATCH_BLOCKS: NonZeroU32 = nonzero_u32(256);

// Variant order is throttle severity; `Ord` picks the stricter state.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum MaterializedViewReplayBudgetState {
    Normal,
    Degraded,
    Paused,
}

impl MaterializedViewReplayBudgetState {
    const fn as_label(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Degraded => "degraded",
            Self::Paused => "paused",
        }
    }

    const fn is_paused(self) -> bool {
        matches!(self, Self::Paused)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct EffectiveMaterializedViewReplayLimits {
    state: MaterializedViewReplayBudgetState,
    batch_blocks: u32,
    memory_budget_bytes: Option<u64>,
    memory_pressure_ratio: Option<f64>,
    phase_gate_engaged: bool,
}

/// Returns whether the current ingest phase gives the storage budget
/// exclusively to canonical work by pausing materialized-view replay.
///
/// Replay fails closed until the ingest loop has positively classified
/// [`IngestPhase::FollowingTip`]. This prevents startup and upstream-wait
/// windows from admitting materialized-view work before canonical ownership is known.
const fn phase_engages_replay_gate(phase: Option<IngestPhase>) -> bool {
    !matches!(phase, Some(IngestPhase::FollowingTip))
}

#[derive(Clone, Debug)]
struct MaterializedViewReplayBudget {
    config: MaterializedViewReplayConfig,
    memory_state: MaterializedViewReplayBudgetState,
    applied_state: MaterializedViewReplayBudgetState,
    /// Live source of the ingest loop phase for the canonical-phase gate; the
    /// phase-driven ingest loop stamps [`IngestPhase`] on this shared handle every
    /// iteration.
    phase_gate: Option<Readiness>,
    /// Process cancellation sampled at replay page boundaries.
    cancel: Option<CancellationToken>,
}

impl MaterializedViewReplayBudget {
    const fn new(config: MaterializedViewReplayConfig) -> Self {
        Self {
            config,
            memory_state: MaterializedViewReplayBudgetState::Normal,
            applied_state: MaterializedViewReplayBudgetState::Normal,
            phase_gate: None,
            cancel: None,
        }
    }

    const fn with_phase_gate(config: MaterializedViewReplayConfig, readiness: Readiness) -> Self {
        Self {
            config,
            memory_state: MaterializedViewReplayBudgetState::Normal,
            applied_state: MaterializedViewReplayBudgetState::Normal,
            phase_gate: Some(readiness),
            cancel: None,
        }
    }

    fn with_phase_gate_and_cancel(
        config: MaterializedViewReplayConfig,
        readiness: Readiness,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            config,
            memory_state: MaterializedViewReplayBudgetState::Normal,
            applied_state: MaterializedViewReplayBudgetState::Normal,
            phase_gate: Some(readiness),
            cancel: Some(cancel),
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancel
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
    }

    fn evaluate_current(&mut self) -> EffectiveMaterializedViewReplayLimits {
        let phase = self
            .phase_gate
            .as_ref()
            .map_or(Some(IngestPhase::FollowingTip), Readiness::phase);
        self.evaluate(RuntimeMemorySnapshot::sample(), phase)
    }

    fn evaluate(
        &mut self,
        memory_snapshot: RuntimeMemorySnapshot,
        phase: Option<IngestPhase>,
    ) -> EffectiveMaterializedViewReplayLimits {
        let memory_budget_bytes = self
            .config
            .memory_budget_bytes
            .map(std::num::NonZeroU64::get)
            .or(memory_snapshot.cgroup_high_bytes)
            .or(memory_snapshot.cgroup_max_bytes);
        let memory_pressure_bytes = memory_snapshot.non_reclaimable_bytes();
        let memory_pressure_ratio = memory_pressure_bytes.zip(memory_budget_bytes).and_then(
            |(current_bytes, budget_bytes)| {
                (budget_bytes > 0).then(|| u64_to_f64(current_bytes) / u64_to_f64(budget_bytes))
            },
        );
        self.memory_state =
            next_memory_budget_state(self.config, self.memory_state, memory_pressure_ratio);
        let phase_gate_engaged = phase_engages_replay_gate(phase);
        let state = compose_replay_state(self.config, self.memory_state, phase_gate_engaged);
        self.applied_state = state;
        EffectiveMaterializedViewReplayLimits {
            state,
            batch_blocks: effective_replay_batch_blocks(
                self.config,
                state,
                memory_pressure_ratio,
                phase_gate_engaged,
            ),
            memory_budget_bytes,
            memory_pressure_ratio,
            phase_gate_engaged,
        }
    }
}

/// Advances the memory-pressure hysteresis machine independent of policy and
/// phase, so a later gate engagement inherits a warm state instead of a cold
/// `Normal`.
fn next_memory_budget_state(
    config: MaterializedViewReplayConfig,
    current_state: MaterializedViewReplayBudgetState,
    memory_pressure_ratio: Option<f64>,
) -> MaterializedViewReplayBudgetState {
    let Some(pressure_ratio) = memory_pressure_ratio else {
        return MaterializedViewReplayBudgetState::Normal;
    };
    match current_state {
        MaterializedViewReplayBudgetState::Normal => {
            if pressure_ratio >= config.memory_pause_ratio {
                MaterializedViewReplayBudgetState::Paused
            } else if pressure_ratio >= config.memory_degrade_ratio {
                MaterializedViewReplayBudgetState::Degraded
            } else {
                MaterializedViewReplayBudgetState::Normal
            }
        }
        MaterializedViewReplayBudgetState::Degraded => {
            if pressure_ratio >= config.memory_pause_ratio {
                MaterializedViewReplayBudgetState::Paused
            } else if pressure_ratio < config.memory_resume_ratio {
                MaterializedViewReplayBudgetState::Normal
            } else {
                MaterializedViewReplayBudgetState::Degraded
            }
        }
        MaterializedViewReplayBudgetState::Paused => {
            if pressure_ratio >= config.memory_pause_ratio {
                MaterializedViewReplayBudgetState::Paused
            } else if pressure_ratio >= config.memory_resume_ratio {
                MaterializedViewReplayBudgetState::Degraded
            } else {
                MaterializedViewReplayBudgetState::Normal
            }
        }
    }
}

/// Composes the applied replay state from the memory hysteresis machine and the
/// canonical-phase gate, letting the stricter of the two win.
///
/// The gate pauses materialized-view replay during canonical bulk catch-up for every
/// policy, so `continuous` keeps its meaning only as an at-tip override.
/// Rebuildable materialized views resume once canonical ingest enters tip follow.
fn compose_replay_state(
    config: MaterializedViewReplayConfig,
    memory_state: MaterializedViewReplayBudgetState,
    phase_gate_engaged: bool,
) -> MaterializedViewReplayBudgetState {
    let memory_applies =
        config.replay_policy == MaterializedViewReplayPolicy::CanonicalFirst || phase_gate_engaged;
    let applied_memory_state = if memory_applies {
        memory_state
    } else {
        MaterializedViewReplayBudgetState::Normal
    };
    let gate_floor = if phase_gate_engaged {
        MaterializedViewReplayBudgetState::Paused
    } else {
        MaterializedViewReplayBudgetState::Normal
    };
    applied_memory_state.max(gate_floor)
}

fn effective_replay_batch_blocks(
    config: MaterializedViewReplayConfig,
    state: MaterializedViewReplayBudgetState,
    memory_pressure_ratio: Option<f64>,
    phase_gate_engaged: bool,
) -> u32 {
    let configured_blocks = config.replay_batch_blocks.get();
    let min_blocks = config.min_replay_batch_blocks.get();
    if state == MaterializedViewReplayBudgetState::Normal {
        return configured_blocks;
    }
    if state == MaterializedViewReplayBudgetState::Paused {
        return 0;
    }
    if phase_gate_engaged {
        return min_blocks;
    }
    let midpoint = config.memory_degrade_ratio
        + ((config.memory_pause_ratio - config.memory_degrade_ratio) / 2.0);
    if memory_pressure_ratio.is_some_and(|ratio| ratio >= midpoint) {
        min_blocks
    } else {
        configured_blocks.saturating_div(2).max(min_blocks)
    }
}

/// Opens the replay-host-owned materialized-view store nested under a canonical store path.
pub fn open_primary_materialized_view_store(
    canonical_path: &Path,
    materialized_view_preset: MaterializedViewPreset,
    rocksdb_resource_budget: RocksDbResourceBudget,
) -> Result<MaterializedViewStore, zinder_materialized_views::MaterializedViewStoreError> {
    MaterializedViewStore::open_with_materialized_view_preset(
        MaterializedViewStore::path_for_canonical(canonical_path),
        materialized_view_preset,
        MaterializedViewStoreOptions {
            sync_writes: false,
            rocksdb_resource_budget,
            ..MaterializedViewStoreOptions::default()
        },
    )
}

/// One block-keyed chain-event consumer selected by the materialized-view store.
#[derive(Clone, Copy)]
struct SelectedBlockConsumer {
    name: MaterializedViewConsumerName,
    join: ChainEventBlockJoin,
    construct: fn() -> Box<dyn BlockKeyedConsumer>,
}

impl SelectedBlockConsumer {
    /// Returns whether the consumer's cursor defines the shared replay boundary
    /// that backfill-owned consumers join at startup.
    const fn defines_replay_boundary(self) -> bool {
        match self.join {
            ChainEventBlockJoin::Replay => true,
            ChainEventBlockJoin::CanonicalBackfill
            | ChainEventBlockJoin::CanonicalSnapshot { .. } => false,
        }
    }

    const fn is_backfill_owned(self) -> bool {
        match self.join {
            ChainEventBlockJoin::CanonicalBackfill => true,
            ChainEventBlockJoin::Replay | ChainEventBlockJoin::CanonicalSnapshot { .. } => false,
        }
    }

    fn receives_chain_events(
        self,
        materialized_view_store: &MaterializedViewStore,
    ) -> Result<bool, IngestError> {
        match self.join {
            ChainEventBlockJoin::Replay | ChainEventBlockJoin::CanonicalBackfill => Ok(true),
            ChainEventBlockJoin::CanonicalSnapshot { is_active } => {
                Ok(is_active(materialized_view_store)?)
            }
        }
    }
}

fn selected_block_consumers(
    materialized_view_store: &MaterializedViewStore,
) -> impl Iterator<Item = SelectedBlockConsumer> + '_ {
    bundled_materialized_view_consumer_definitions()
        .iter()
        .filter(|definition| materialized_view_store.has_consumer(definition.schema.name))
        .filter_map(|definition| match definition.dispatch {
            MaterializedViewDispatch::ChainEventBlocks { join, construct } => {
                Some(SelectedBlockConsumer {
                    name: definition.schema.name,
                    join,
                    construct,
                })
            }
            MaterializedViewDispatch::ChainEventEnvelope { .. }
            | MaterializedViewDispatch::MempoolEvents { .. }
            | MaterializedViewDispatch::CanonicalBackfillOnly => None,
        })
}

fn selected_event_only_consumers(
    materialized_view_store: &MaterializedViewStore,
) -> Vec<Box<dyn MaterializedViewConsumer>> {
    bundled_materialized_view_consumer_definitions()
        .iter()
        .filter(|definition| materialized_view_store.has_consumer(definition.schema.name))
        .filter_map(|definition| match definition.dispatch {
            MaterializedViewDispatch::ChainEventEnvelope { construct } => Some(construct()),
            MaterializedViewDispatch::ChainEventBlocks { .. }
            | MaterializedViewDispatch::MempoolEvents { .. }
            | MaterializedViewDispatch::CanonicalBackfillOnly => None,
        })
        .collect()
}

/// Seeds missing event cursors for consumers with dedicated historical backfills.
///
/// A newly declared block consumer normally starts without a cursor, causing
/// every bundled block consumer to replay from retained history. Backfill-owned
/// consumers instead reconstruct settled history from canonical facts. When
/// every other bundled block consumer agrees on one cursor, each missing
/// backfill-owned consumer can join at that exact boundary. A fresh or partially
/// rebuilt materialized-view store is left untouched so the normal replay contract applies.
pub fn seed_backfill_owned_consumer_cursors(
    canonical: &RocksDbCanonicalSecondary,
    activations: &NetworkUpgradeActivations,
    materialized_view_store: &MaterializedViewStore,
) -> Result<(), IngestError> {
    let Some(cursor) = unanimous_existing_block_consumer_cursor(materialized_view_store)? else {
        return Ok(());
    };
    let missing_consumers = missing_backfill_consumer_cursors(materialized_view_store, &cursor)?;
    if missing_consumers.is_empty() {
        return Ok(());
    }
    let Some(authoritative_height) =
        materialized_view_store.last_materialized_height_ascending(BLOCK_SUMMARY_COLUMN_FAMILY)?
    else {
        return Ok(());
    };
    BackfillCursorSeed {
        canonical,
        activations,
        materialized_view_store,
        cursor,
        missing_consumers,
        authoritative_height,
    }
    .run()
}

/// One backfill-owned cursor seeding pass over an admitted canonical secondary.
struct BackfillCursorSeed<'canonical> {
    canonical: &'canonical RocksDbCanonicalSecondary,
    activations: &'canonical NetworkUpgradeActivations,
    materialized_view_store: &'canonical MaterializedViewStore,
    cursor: Vec<u8>,
    missing_consumers: Vec<MaterializedViewConsumerName>,
    authoritative_height: BlockHeight,
}

impl BackfillCursorSeed<'_> {
    fn run(&self) -> Result<(), IngestError> {
        self.seed_conventional_fee_distribution_cursor()?;
        self.seed_block_production_time_cursor()?;
        self.seed_transaction_component_cursor()?;
        for consumer_name in self.missing_consumers.iter().copied().filter(|name| {
            ![
                CONVENTIONAL_FEE_DISTRIBUTION_CONSUMER_NAME,
                BLOCK_PRODUCTION_TIME_CONSUMER_NAME,
                PAID_FEE_DISTRIBUTION_CONSUMER_NAME,
                TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME,
                VALUE_POOL_FLOW_HISTORY_CONSUMER_NAME,
            ]
            .contains(name)
        }) {
            self.materialized_view_store
                .put_chain_event_cursor(consumer_name, &self.cursor)?;
            tracing::info!(
                target: "zinder::ingest",
                event = "backfill_owned_consumer_cursor_seeded",
                consumer = consumer_name.as_str(),
                "materialized-view consumer joined the existing event boundary; historical coverage remains backfill-owned"
            );
        }
        Ok(())
    }

    fn seed_block_production_time_cursor(&self) -> Result<(), IngestError> {
        if self
            .missing_consumers
            .contains(&BLOCK_PRODUCTION_TIME_CONSUMER_NAME)
        {
            let boundary_height = self.authoritative_height.next().ok_or_else(|| {
                IngestError::MaterializedViewDispatch(
                    "block-production time tail boundary height overflow".to_owned(),
                )
            })?;
            BlockProductionTimeConsumer::initialize_tail_boundary(
                self.materialized_view_store,
                boundary_height,
            )
            .map_err(|error| IngestError::MaterializedViewDispatch(error.to_string()))?;
            self.materialized_view_store
                .put_chain_event_cursor(BLOCK_PRODUCTION_TIME_CONSUMER_NAME, &self.cursor)?;
            tracing::info!(
                target: "zinder::ingest",
                event = "block_production_time_tail_boundary_initialized",
                tail_boundary = boundary_height.value(),
                "block-production time consumer joined the existing materialized-view event boundary"
            );
        }
        if self
            .materialized_view_store
            .consumer_state(BLOCK_PRODUCTION_TIME_CONSUMER_NAME)?
            .is_some()
        {
            return Ok(());
        }
        let chain_epoch = self.canonical.chain_epoch()?;
        let tip_hash = self
            .canonical
            .block_header_at(self.authoritative_height)?
            .ok_or_else(|| {
                IngestError::MaterializedViewDispatch(format!(
                    "canonical block {} is missing while seeding block-production time state",
                    self.authoritative_height.value(),
                ))
            })?
            .block_hash;
        self.materialized_view_store.put_consumer_state(
            BLOCK_PRODUCTION_TIME_CONSUMER_NAME,
            MaterializedViewState {
                chain_epoch_id: chain_epoch.id,
                tip_height: self.authoritative_height,
                tip_hash,
                revision: 1,
                coverage: None,
            },
        )?;
        Ok(())
    }

    fn seed_conventional_fee_distribution_cursor(&self) -> Result<(), IngestError> {
        let cursor_is_missing = self
            .missing_consumers
            .contains(&CONVENTIONAL_FEE_DISTRIBUTION_CONSUMER_NAME);
        let desired_tail_boundary = backfill_consumer_tail_boundary(
            self.canonical.chain_epoch()?.settled_tip_height,
            self.authoritative_height,
            "conventional-fee distribution",
        )?;
        let tail_boundary_changed =
            ConventionalFeeDistributionConsumer::widen_tail_boundary_for_startup(
                self.materialized_view_store,
                desired_tail_boundary,
            )
            .map_err(|error| IngestError::MaterializedViewDispatch(error.to_string()))?;
        let tail_needs_seed =
            ConventionalFeeDistributionConsumer::tail_coverage(self.materialized_view_store)?
                .is_some_and(|tail| {
                    tail.complete_through_height
                        .is_none_or(|through| through < self.authoritative_height)
                });
        if !(cursor_is_missing || tail_boundary_changed || tail_needs_seed) {
            return Ok(());
        }
        seed_conventional_fee_distribution_visible_tail(
            self.canonical,
            self.activations,
            self.materialized_view_store,
            self.authoritative_height,
            BACKFILL_TAIL_SEED_BATCH_BLOCKS,
        )?;
        if cursor_is_missing {
            self.materialized_view_store.put_chain_event_cursor(
                CONVENTIONAL_FEE_DISTRIBUTION_CONSUMER_NAME,
                &self.cursor,
            )?;
        }
        let tail_boundary =
            ConventionalFeeDistributionConsumer::tail_coverage(self.materialized_view_store)?
                .ok_or_else(|| {
                    IngestError::MaterializedViewDispatch(
                        "conventional-fee distribution tail coverage disappeared during startup"
                            .to_owned(),
                    )
                })?
                .boundary_height;
        tracing::info!(
            target: "zinder::ingest",
            event = "conventional_fee_distribution_tail_boundary_initialized",
            cursor_seeded = cursor_is_missing,
            tail_boundary = tail_boundary.value(),
            "conventional-fee distribution consumer joined the existing materialized-view event boundary"
        );
        Ok(())
    }

    fn seed_transaction_component_cursor(&self) -> Result<(), IngestError> {
        let cursor_is_missing = self
            .missing_consumers
            .contains(&TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME);
        let desired_tail_boundary = backfill_consumer_tail_boundary(
            self.canonical.chain_epoch()?.settled_tip_height,
            self.authoritative_height,
            "transaction-component",
        )?;
        let tail_boundary_changed =
            TransactionComponentSummaryConsumer::widen_tail_boundary_for_startup(
                self.materialized_view_store,
                desired_tail_boundary,
            )
            .map_err(|error| IngestError::MaterializedViewDispatch(error.to_string()))?;
        let tail_needs_seed =
            TransactionComponentSummaryConsumer::tail_coverage(self.materialized_view_store)?
                .is_some_and(|tail| {
                    tail.complete_through_height
                        .is_none_or(|through| through < self.authoritative_height)
                });
        if !(cursor_is_missing || tail_boundary_changed || tail_needs_seed) {
            return Ok(());
        }
        seed_transaction_component_visible_tail(
            self.canonical,
            self.activations,
            self.materialized_view_store,
            self.authoritative_height,
            BACKFILL_TAIL_SEED_BATCH_BLOCKS,
        )?;
        if cursor_is_missing {
            self.materialized_view_store.put_chain_event_cursor(
                TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME,
                &self.cursor,
            )?;
        }
        let tail_boundary =
            TransactionComponentSummaryConsumer::tail_coverage(self.materialized_view_store)?
                .ok_or_else(|| {
                    IngestError::MaterializedViewDispatch(
                        "transaction-component tail coverage disappeared during startup".to_owned(),
                    )
                })?
                .boundary_height;
        tracing::info!(
            target: "zinder::ingest",
            event = "transaction_component_tail_boundary_initialized",
            cursor_seeded = cursor_is_missing,
            tail_boundary = tail_boundary.value(),
            "transaction-component consumer joined the existing materialized-view event boundary"
        );
        Ok(())
    }
}

pub(crate) fn unanimous_existing_block_consumer_cursor(
    materialized_view_store: &MaterializedViewStore,
) -> Result<Option<Vec<u8>>, IngestError> {
    let mut agreed_cursor: Option<Vec<u8>> = None;
    for consumer in selected_block_consumers(materialized_view_store)
        .filter(|consumer| consumer.defines_replay_boundary())
    {
        let Some(candidate) = materialized_view_store.get_chain_event_cursor(consumer.name)? else {
            return Ok(None);
        };
        if agreed_cursor
            .as_ref()
            .is_some_and(|existing| existing != &candidate)
        {
            return Err(IngestError::MaterializedViewDispatch(
                "existing block materialized-view consumer cursors disagree while seeding backfill-owned consumers"
                    .to_owned(),
            ));
        }
        agreed_cursor = Some(candidate);
    }
    Ok(agreed_cursor)
}

fn missing_backfill_consumer_cursors(
    materialized_view_store: &MaterializedViewStore,
    cursor: &[u8],
) -> Result<Vec<MaterializedViewConsumerName>, IngestError> {
    let mut missing_consumers = Vec::new();
    for consumer_name in selected_block_consumers(materialized_view_store)
        .filter(|consumer| consumer.is_backfill_owned())
        .map(|consumer| consumer.name)
    {
        match materialized_view_store.get_chain_event_cursor(consumer_name)? {
            Some(existing) if existing != cursor => {
                return Err(IngestError::MaterializedViewDispatch(
                    "backfill-owned materialized-view consumer cursor disagrees with the existing block consumer boundary"
                        .to_owned(),
                ));
            }
            Some(_) => {}
            None => missing_consumers.push(consumer_name),
        }
    }
    Ok(missing_consumers)
}

pub(crate) fn backfill_consumer_tail_boundary(
    settled_tip_height: BlockHeight,
    authoritative_height: BlockHeight,
    consumer: &str,
) -> Result<BlockHeight, IngestError> {
    BlockHeight::new(settled_tip_height.value().min(authoritative_height.value()))
        .next()
        .ok_or_else(|| {
            IngestError::MaterializedViewDispatch(format!(
                "{consumer} live-tail boundary height overflow"
            ))
        })
}

/// Everything the always-on materialized-view tailer owns.
///
/// The canonical secondary is shared behind a lock because advancing it to the
/// writer's newest fence needs exclusive access while every hydration and event
/// read needs only shared access.
pub struct MaterializedViewTailer {
    /// In-process canonical secondary the replay reads through.
    pub canonical: Arc<RwLock<RocksDbCanonicalSecondary>>,
    /// Materialized-view store opened as the single primary writer.
    pub materialized_view_store: MaterializedViewStore,
    /// Replay batch and memory-pressure limits.
    pub config: MaterializedViewReplayConfig,
    /// Network-upgrade activation identity used to derive commitment-tree roots.
    pub activations: Arc<NetworkUpgradeActivations>,
    /// Reorg window the canonical writer commits under.
    pub reorg_window_blocks: u32,
    /// Chain-event retention window the writer prunes under, or `None` when
    /// eviction is disabled and no consumer cursor can expire.
    pub chain_event_retention_window: Option<Duration>,
    /// Stall duration after which a consumer cursor that has not advanced is
    /// reported through [`ReadinessCause::CursorAtRisk`].
    pub cursor_at_risk_warning: Duration,
}

impl MaterializedViewTailer {
    /// Replays every retained canonical transition into the materialized-view store.
    pub fn catch_up(&self) -> Result<(), IngestError> {
        self.catch_up_with_pass(&mut ReplayPass::new(MaterializedViewReplayBudget::new(
            self.config,
        )))
    }

    fn catch_up_with_pass(&self, pass: &mut ReplayPass) -> Result<(), IngestError> {
        if !self.materialized_view_store.has_consumer_column_families() {
            return Ok(());
        }
        self.canonical.write().try_catch_up()?;
        let canonical = self.canonical.read();
        self.replay_event_only_consumers(&canonical)?;
        self.replay_block_consumers(&canonical, pass)
    }

    /// Replays retained transitions into the block-keyed consumers, then
    /// rebuilds any height range they have not materialized yet.
    ///
    /// The height rebuild is what builds a fresh view store: the canonical
    /// event log is time-pruned, so a store with no consumer cursor cannot be
    /// built from events alone. It also resumes an interrupted build, because a
    /// partial build leaves the consumer head below the fence's visible tip.
    fn replay_block_consumers(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        pass: &mut ReplayPass,
    ) -> Result<(), IngestError> {
        if let Some(cursor) = persisted_chain_event_cursor(&self.materialized_view_store)? {
            self.replay_retained_events(canonical, pass, cursor)?;
            return self.rebuild_unmaterialized_heights(canonical, pass);
        }
        require_genesis_complete_history(canonical)?;
        match self.materialized_height()? {
            Some(durable_height)
                if !self.materialized_tip_is_canonical(canonical, durable_height)? =>
            {
                let reverted = self.reorg_window_below(canonical, durable_height);
                tracing::warn!(
                    target: "zinder::ingest",
                    event = "materialized_view_replay_uncursored_head_replaced",
                    durable_height = durable_height.value(),
                    revert_from_height = reverted.start.value(),
                    "materialized-view rows have no unanimous consumer cursor and their newest block is no longer canonical; reverting the reorg window and rebuilding from canonical heights"
                );
                self.rebuild_over_reorg_window(canonical, pass, reverted)
            }
            Some(_) | None => self.rebuild_unmaterialized_heights(canonical, pass),
        }
    }

    /// Returns whether the newest block the view checkpointed is still canonical.
    ///
    /// Without a unanimous cursor, retained events cannot show whether a reorg
    /// replaced heights the view already holds, as when a rebuild stops between
    /// pages and the chain reorgs before it resumes. Every page checkpoints the
    /// hash of its last block, and blocks commit to their parents, so a
    /// canonical checkpoint proves every row at or below it is canonical too.
    fn materialized_tip_is_canonical(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        durable_height: BlockHeight,
    ) -> Result<bool, IngestError> {
        let Some(state) = self
            .materialized_view_store
            .consumer_state(TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME)?
        else {
            return Ok(false);
        };
        if durable_height > state.tip_height {
            return Ok(false);
        }
        Ok(matches!(
            canonical.block_hash_lookup(state.tip_hash)?,
            BlockHashLookup::Resolved(block_id) if block_id.height == state.tip_height
        ))
    }

    fn replay_retained_events(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        pass: &mut ReplayPass,
        from_cursor: CanonicalEventCursor,
    ) -> Result<(), IngestError> {
        let mut cursor = from_cursor;
        loop {
            if pass.yields() {
                return Ok(());
            }
            let read_started_at = Instant::now();
            let page_outcome = read_canonical_event_page(canonical, Some(cursor));
            record_materialized_view_replay_stage(
                MATERIALIZED_VIEW_REPLAY_STAGE_READ_EVENTS,
                read_started_at,
                &page_outcome,
            );
            let page = match page_outcome {
                Ok(page) => page,
                Err(IngestError::CanonicalStore(
                    CanonicalStoreError::CanonicalEventCursorExpired {
                        event_sequence,
                        oldest_retained_sequence,
                    },
                )) => {
                    return self.recover_expired_cursor(
                        canonical,
                        pass,
                        ExpiredCursor {
                            event_sequence,
                            oldest_retained_sequence,
                        },
                    );
                }
                Err(error) => return Err(error),
            };
            if page.is_empty() {
                return Ok(());
            }
            for retained in page {
                if pass.yields() {
                    return Ok(());
                }
                self.replay_retained_event(canonical, pass, retained)?;
                cursor = retained.cursor();
            }
        }
    }

    fn replay_retained_event(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        pass: &mut ReplayPass,
        retained: CanonicalRetainedEvent,
    ) -> Result<(), IngestError> {
        let resulting_epoch = canonical.chain_epoch_at(retained.resulting_epoch_id())?;
        let event = ChainEvent::from_canonical_retained(
            retained,
            resulting_epoch,
            reverted_epoch(canonical, retained)?,
        )?;
        self.dispatch_committed_range(
            canonical,
            pass,
            &DispatchedTransition {
                chain_epoch: resulting_epoch,
                cursor: retained.cursor(),
                committed_range: retained.committed_range(),
                reverted: reverted_range_of(&event),
            },
        )
    }

    /// Rebuilds the height range the block consumers have not materialized at
    /// the secondary's admitted fence.
    ///
    /// Every page is stamped with the fence cursor, so the consumers resume
    /// event tailing strictly after the transition the fence names. A reorg
    /// that lands during the rebuild is ordered after that fence and replays
    /// its revert over anything the rebuild read from the replaced chain.
    fn rebuild_unmaterialized_heights(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        pass: &mut ReplayPass,
    ) -> Result<(), IngestError> {
        let chain_epoch = canonical.chain_epoch()?;
        let first_available_height = canonical.history_bounds().first_available_height();
        let start = self
            .materialized_height()?
            .map_or(first_available_height, |head| {
                head.next().unwrap_or(head).max(first_available_height)
            });
        if start > chain_epoch.visible_tip_height {
            return Ok(());
        }
        self.dispatch_committed_range(
            canonical,
            pass,
            &DispatchedTransition {
                chain_epoch,
                cursor: fence_cursor(canonical)?,
                committed_range: BlockHeightRange::inclusive(start, chain_epoch.visible_tip_height),
                reverted: None,
            },
        )
    }

    /// Recovers a consumer cursor that names history the canonical store pruned.
    fn recover_expired_cursor(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        pass: &mut ReplayPass,
        expired: ExpiredCursor,
    ) -> Result<(), IngestError> {
        require_genesis_complete_history(canonical)?;
        let Some(durable_height) = self.materialized_height()? else {
            return Ok(());
        };
        let reverted = self.reorg_window_below(canonical, durable_height);
        tracing::warn!(
            target: "zinder::ingest",
            event = "materialized_view_replay_cursor_expired",
            persisted_event_sequence = expired.event_sequence,
            oldest_retained_event_sequence = expired.oldest_retained_sequence,
            durable_height = durable_height.value(),
            revert_from_height = reverted.start.value(),
            "materialized-view consumer cursor is older than retained canonical events; reverting the reorg window and rebuilding from canonical heights"
        );
        self.rebuild_over_reorg_window(canonical, pass, reverted)
    }

    /// Returns the heights at or below `durable_height` a canonical replacement
    /// could have reached since the view wrote them.
    ///
    /// When the view applied height D the canonical settled tip was at least D
    /// minus the reorg window, so no replacement can reach deeper than that.
    fn reorg_window_below(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        durable_height: BlockHeight,
    ) -> BlockHeightRange {
        let revert_from = BlockHeight::new(
            durable_height
                .value()
                .saturating_sub(self.reorg_window_blocks)
                .saturating_add(1)
                .max(canonical.history_bounds().first_available_height().value()),
        );
        BlockHeightRange::inclusive(revert_from, durable_height)
    }

    /// Reverts `reverted`, then rebuilds from its start through the visible tip.
    ///
    /// Reverting the reorg window below the durable height and rebuilding the
    /// remainder from canonical heights converges on the same rows an
    /// uninterrupted event replay would write.
    fn rebuild_over_reorg_window(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        pass: &mut ReplayPass,
        reverted: BlockHeightRange,
    ) -> Result<(), IngestError> {
        let chain_epoch = canonical.chain_epoch()?;
        self.dispatch_committed_range(
            canonical,
            pass,
            &DispatchedTransition {
                chain_epoch,
                cursor: fence_cursor(canonical)?,
                committed_range: BlockHeightRange::inclusive(
                    reverted.start,
                    chain_epoch.visible_tip_height,
                ),
                reverted: Some(ChainRangeReverted {
                    chain_epoch,
                    block_range: reverted,
                }),
            },
        )
    }

    /// Hydrates and dispatches one transition's committed range in bounded pages.
    ///
    /// Only the last page advances the consumer cursors, so an interrupted
    /// transition replays from its start and re-applies the already-written
    /// pages idempotently.
    fn dispatch_committed_range(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        pass: &mut ReplayPass,
        transition: &DispatchedTransition,
    ) -> Result<(), IngestError> {
        let committed = transition.committed_range;
        if committed.start > committed.end {
            return self.dispatch_page(transition, committed, &HashMap::new(), true);
        }
        let mut hydrator = CanonicalBlockContextReader::new(canonical, &self.activations);
        let mut next_height = committed.start;
        let mut first_page = true;
        while next_height <= committed.end {
            let effective_limits = pass.evaluate();
            if effective_limits.state.is_paused() || pass.budget.is_cancelled() {
                return Ok(());
            }
            let page = replay_page(next_height, committed.end, effective_limits.batch_blocks)?;
            let hydrate_started_at = Instant::now();
            let contexts_outcome = hydrator.read_block_commit_contexts(page);
            record_materialized_view_replay_stage(
                MATERIALIZED_VIEW_REPLAY_STAGE_HYDRATE_BLOCKS,
                hydrate_started_at,
                &contexts_outcome,
            );
            let contexts = contexts_outcome?;
            let final_page = page.end >= committed.end;
            self.dispatch_page(
                &transition.for_page(first_page),
                page,
                &contexts,
                final_page,
            )?;
            self.record_replay_progress(page.end, transition.chain_epoch.visible_tip_height);
            if pass.status_persist_is_due() {
                self.persist_status(canonical, pass.budget.applied_state);
            }
            next_height = page.end.next().ok_or_else(|| {
                IngestError::MaterializedViewDispatch(
                    "materialized-view replay height overflow".to_owned(),
                )
            })?;
            first_page = false;
        }
        Ok(())
    }

    fn dispatch_page(
        &self,
        transition: &DispatchedTransition,
        page: BlockHeightRange,
        contexts: &HashMap<BlockHeight, Arc<BlockCommitContext>>,
        advance_cursor: bool,
    ) -> Result<(), IngestError> {
        let event = transition.page_event(page);
        let cursor = transition.cursor.as_bytes();
        let inputs = ChainEventDispatchInputs {
            chain_epoch: transition.chain_epoch,
            chain_event: &event,
            chain_cursor: &cursor,
            event_sequence: transition.cursor.event_sequence(),
            settled_tip_height: transition.chain_epoch.settled_tip_height,
        };
        let dispatch_started_at = Instant::now();
        let dispatch_outcome = dispatch_chain_event(
            &self.materialized_view_store,
            inputs,
            contexts,
            advance_cursor,
        );
        record_materialized_view_replay_stage(
            MATERIALIZED_VIEW_REPLAY_STAGE_DISPATCH_EVENT,
            dispatch_started_at,
            &dispatch_outcome,
        );
        record_materialized_view_replay_event(contexts.len(), dispatch_outcome.as_ref().err());
        dispatch_outcome
    }

    /// Replays retained transitions into consumers that never read block contexts.
    ///
    /// An expired cursor resets the incident log to the retained floor: the log
    /// is honestly window-bounded, so a pruned prefix is unrecoverable and the
    /// only correct resume point is the oldest retained transition.
    fn replay_event_only_consumers(
        &self,
        canonical: &RocksDbCanonicalSecondary,
    ) -> Result<(), IngestError> {
        if self
            .materialized_view_store
            .event_only_chain_event_consumer_names()
            .next()
            .is_none()
        {
            return Ok(());
        }
        let retention_floor = canonical.canonical_event_retention_floor()?;
        let mut cursor = persisted_event_only_chain_event_cursor(&self.materialized_view_store)?
            .filter(|cursor| cursor.event_sequence().saturating_add(1) >= retention_floor);
        loop {
            let read_started_at = Instant::now();
            let page_outcome = read_canonical_event_page(canonical, cursor);
            record_materialized_view_replay_stage(
                MATERIALIZED_VIEW_REPLAY_STAGE_READ_EVENTS,
                read_started_at,
                &page_outcome,
            );
            let page = page_outcome?;
            if page.is_empty() {
                return Ok(());
            }
            for retained in page {
                self.dispatch_event_only_transition(canonical, retained)?;
                cursor = Some(retained.cursor());
            }
        }
    }

    fn dispatch_event_only_transition(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        retained: CanonicalRetainedEvent,
    ) -> Result<(), IngestError> {
        let resulting_epoch = canonical.chain_epoch_at(retained.resulting_epoch_id())?;
        let event = ChainEvent::from_canonical_retained(
            retained,
            resulting_epoch,
            reverted_epoch(canonical, retained)?,
        )?;
        let cursor = retained.cursor().as_bytes();
        let inputs = ChainEventDispatchInputs {
            chain_epoch: resulting_epoch,
            chain_event: &event,
            chain_cursor: &cursor,
            event_sequence: retained.cursor().event_sequence(),
            settled_tip_height: resulting_epoch.settled_tip_height,
        };
        let dispatch_started_at = Instant::now();
        let dispatch_outcome =
            dispatch_event_only_chain_event(&self.materialized_view_store, inputs);
        record_materialized_view_replay_stage(
            MATERIALIZED_VIEW_REPLAY_STAGE_DISPATCH_EVENT,
            dispatch_started_at,
            &dispatch_outcome,
        );
        dispatch_outcome?;
        record_materialized_view_consumer_replay_progress(
            self.materialized_view_store
                .event_only_chain_event_consumer_names(),
            resulting_epoch.visible_tip_height,
            resulting_epoch.visible_tip_height,
        );
        Ok(())
    }

    fn materialized_height(&self) -> Result<Option<BlockHeight>, IngestError> {
        Ok(self
            .materialized_view_store
            .last_materialized_height_ascending(TRANSPARENT_OUTPOINT_SPEND_INDEX_COLUMN_FAMILY)?)
    }

    fn record_replay_progress(&self, progress_height: BlockHeight, canonical_tip: BlockHeight) {
        record_materialized_view_replay_status_metrics(
            Some(progress_height.value()),
            Some(canonical_tip.value()),
        );
        record_materialized_view_consumer_replay_progress(
            self.materialized_view_store.chain_event_consumer_names(),
            progress_height,
            canonical_tip,
        );
    }

    /// Persists the materialized-view plane's status into the shared materialized-view store.
    ///
    /// The explorer plane surfaces it on `ServerInfo`. Written on the paused
    /// branch too, so a stalled materialized-view plane is observable on the
    /// wire instead of silent. Best-effort: a write failure is logged, never fatal.
    fn persist_status(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        budget_state: MaterializedViewReplayBudgetState,
    ) {
        let indexed_height = match self.materialized_height() {
            Ok(indexed_height) => indexed_height.map(BlockHeight::value),
            Err(error) => {
                tracing::warn!(
                    target: "zinder::ingest",
                    event = "materialized_view_status_consumer_head_read_failed",
                    error = %error,
                    "failed to read the shared wallet-correctness consumer head",
                );
                return;
            }
        };
        let canonical_tip = record_current_materialized_view_replay_tip(canonical);
        let lag_blocks = match (canonical_tip, indexed_height) {
            (Some(tip), Some(indexed)) => u64::from(tip.saturating_sub(indexed)),
            (Some(tip), None) => u64::from(tip),
            (None, _) => 0,
        };
        record_materialized_view_replay_status_metrics(indexed_height, canonical_tip);
        let health = if budget_state.is_paused() {
            MaterializedViewHealth::Paused
        } else if indexed_height.is_some() && lag_blocks == 0 {
            MaterializedViewHealth::Live
        } else {
            MaterializedViewHealth::CatchingUp
        };
        let status = MaterializedViewStatus {
            health: health as i32,
            indexed_height: indexed_height.unwrap_or(0),
            lag_blocks,
            observed_at_millis: now_unix_millis(),
        };
        let mut bytes = Vec::with_capacity(status.encoded_len());
        if let Err(error) = status.encode(&mut bytes) {
            tracing::warn!(
                target: "zinder::ingest",
                event = "materialized_view_status_encode_failed",
                error = %error,
                "failed to encode materialized-view status record",
            );
            return;
        }
        if let Err(error) = self
            .materialized_view_store
            .put_materialized_view_status(&bytes)
        {
            tracing::warn!(
                target: "zinder::ingest",
                event = "materialized_view_status_persist_failed",
                error = %error,
                "failed to persist materialized-view status record",
            );
        }
    }

    fn refresh_historical_work_gate(
        &self,
        canonical: &RocksDbCanonicalSecondary,
        historical_work_gate: &HistoricalWorkGate,
    ) {
        let caught_up = self.replay_caught_up(canonical).unwrap_or_else(|error| {
            tracing::warn!(
                target: "zinder::ingest",
                event = "materialized_view_replay_gate_refresh_failed",
                error = %error,
                "failed to compare materialized-view replay with the canonical tip; historical work remains deferred"
            );
            false
        });
        historical_work_gate.set_materialized_views_caught_up(caught_up);
    }

    fn replay_caught_up(&self, canonical: &RocksDbCanonicalSecondary) -> Result<bool, IngestError> {
        let canonical_tip = canonical.chain_epoch()?.visible_tip_height;
        Ok(self
            .materialized_height()?
            .is_some_and(|indexed| indexed >= canonical_tip))
    }
}

/// Persisted cursor position that retained canonical history no longer covers.
#[derive(Clone, Copy, Debug)]
struct ExpiredCursor {
    event_sequence: u64,
    oldest_retained_sequence: u64,
}

/// One canonical transition being dispatched into the block-keyed consumers.
struct DispatchedTransition {
    chain_epoch: ChainEpoch,
    cursor: CanonicalEventCursor,
    committed_range: BlockHeightRange,
    reverted: Option<ChainRangeReverted>,
}

impl DispatchedTransition {
    /// Returns the transition as it applies to one page of its committed range.
    ///
    /// A revert applies once, on the page that opens the transition.
    fn for_page(&self, first_page: bool) -> Self {
        Self {
            chain_epoch: self.chain_epoch,
            cursor: self.cursor,
            committed_range: self.committed_range,
            reverted: self.reverted.filter(|_| first_page),
        }
    }

    fn page_event(&self, page: BlockHeightRange) -> ChainEvent {
        let committed = ChainEpochCommitted {
            chain_epoch: self.chain_epoch,
            block_range: page,
        };
        self.reverted
            .map_or(ChainEvent::ChainCommitted { committed }, |reverted| {
                ChainEvent::ChainReorged {
                    reverted,
                    committed,
                }
            })
    }
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "a future ChainEvent variant must not silently replay as a reorg"
)]
const fn reverted_range_of(event: &ChainEvent) -> Option<ChainRangeReverted> {
    match event {
        ChainEvent::ChainReorged { reverted, .. } => Some(*reverted),
        _ => None,
    }
}

fn reverted_epoch(
    canonical: &RocksDbCanonicalSecondary,
    retained: CanonicalRetainedEvent,
) -> Result<Option<ChainEpoch>, IngestError> {
    match retained.kind() {
        CanonicalEventKind::Committed => Ok(None),
        CanonicalEventKind::Reorged => Ok(retained
            .previous_epoch_id()
            .map(|epoch_id| canonical.chain_epoch_at(epoch_id))
            .transpose()?),
    }
}

fn replay_page(
    start: BlockHeight,
    end: BlockHeight,
    batch_blocks: u32,
) -> Result<BlockHeightRange, IngestError> {
    let span = batch_blocks
        .clamp(1, MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS)
        .saturating_sub(1);
    let page_end = BlockHeight::new(start.value().saturating_add(span).min(end.value()));
    if page_end < start {
        return Err(IngestError::MaterializedViewDispatch(
            "materialized-view replay page starts above its end".to_owned(),
        ));
    }
    Ok(BlockHeightRange::inclusive(start, page_end))
}

fn fence_cursor(
    canonical: &RocksDbCanonicalSecondary,
) -> Result<CanonicalEventCursor, IngestError> {
    Ok(CanonicalEventCursor::at(
        canonical.event_fence().chain_event_sequence(),
    )?)
}

fn read_canonical_event_page(
    canonical: &RocksDbCanonicalSecondary,
    cursor: Option<CanonicalEventCursor>,
) -> Result<Vec<CanonicalRetainedEvent>, IngestError> {
    let encoded = cursor.map(CanonicalEventCursor::as_bytes);
    Ok(
        canonical.canonical_event_history(CanonicalEventHistoryRequest::new(
            encoded.as_ref().map(<[u8; 9]>::as_slice),
            MATERIALIZED_VIEW_REPLAY_EVENT_PAGE,
        ))?,
    )
}

/// Spawns the ingest-owned canonical tailer for materialized-view consumers.
///
/// The task is intentionally best-effort from the canonical ingest point of
/// view: canonical commits have already succeeded before the tailer sees a
/// transition, so a materialized-view failure is exposed through lag and error
/// metrics without blocking new chain facts from being indexed.
#[must_use = "drop the handle to detach the materialized-view tailer or await it for symmetric shutdown"]
pub fn spawn_materialized_view_tailer_task(
    tailer: MaterializedViewTailer,
    poll_interval: Duration,
    historical_work_gate: HistoricalWorkGate,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(run_materialized_view_tailer(
        Arc::new(tailer),
        poll_interval,
        historical_work_gate,
        cancel,
    ))
}

async fn run_materialized_view_tailer(
    tailer: Arc<MaterializedViewTailer>,
    poll_interval: Duration,
    historical_work_gate: HistoricalWorkGate,
    cancel: CancellationToken,
) {
    if !tailer
        .materialized_view_store
        .has_consumer_column_families()
    {
        historical_work_gate.set_materialized_views_caught_up(true);
        tracing::info!(
            target: "zinder::ingest",
            event = "materialized_view_tailer_disabled",
            "materialized-view tailer disabled because the materialized-view store has no chain-event consumers"
        );
        return;
    }
    tracing::info!(
        target: "zinder::ingest",
        event = "materialized_view_tailer_started",
        poll_interval_ms = u64::try_from(poll_interval.as_millis()).unwrap_or(u64::MAX),
        replay_batch_blocks = tailer.config.replay_batch_blocks.get(),
        min_replay_batch_blocks = tailer.config.min_replay_batch_blocks.get(),
        replay_policy = tailer.config.replay_policy.as_kebab_case(),
        "materialized-view canonical tailer started"
    );

    let mut budget = MaterializedViewReplayBudget::with_phase_gate_and_cancel(
        tailer.config,
        historical_work_gate.readiness().runtime_readiness(),
        cancel.clone(),
    );
    let mut cursor_risk = CursorRiskWatch::new(&tailer, historical_work_gate.readiness());
    loop {
        let effective_limits = budget.evaluate_current();
        record_materialized_view_replay_budget(
            tailer.config.replay_policy,
            effective_limits,
            poll_interval,
        );
        {
            let canonical = tailer.canonical.read();
            tailer.refresh_historical_work_gate(&canonical, &historical_work_gate);
            tailer.persist_status(&canonical, effective_limits.state);
        }
        if !effective_limits.state.is_paused() {
            budget = run_materialized_view_tailer_pass(&tailer, budget).await;
        }
        if let Some(cursor_risk) = cursor_risk.as_mut() {
            cursor_risk.observe(&tailer.materialized_view_store);
        }
        if sleep_or_cancel(poll_interval, &cancel).await {
            tracing::info!(
                target: "zinder::ingest",
                event = "materialized_view_tailer_cancelled",
                "materialized-view canonical tailer cancelled"
            );
            return;
        }
    }
}

async fn run_materialized_view_tailer_pass(
    tailer: &Arc<MaterializedViewTailer>,
    budget: MaterializedViewReplayBudget,
) -> MaterializedViewReplayBudget {
    let started_at = Instant::now();
    let pass_tailer = Arc::clone(tailer);
    let joined = tokio::task::spawn_blocking(move || {
        let mut pass = ReplayPass::new(budget);
        let outcome = pass_tailer.catch_up_with_pass(&mut pass);
        (pass.budget, outcome)
    })
    .await;
    let (budget, outcome) = match joined {
        Ok((budget, outcome)) => (Some(budget), outcome),
        Err(join_error) => (
            None,
            Err(IngestError::BlockingTaskFailed {
                reason: join_error.to_string(),
            }),
        ),
    };
    record_materialized_view_tailer_tick(started_at, &outcome);
    if let Err(error) = outcome {
        tracing::warn!(
            target: "zinder::ingest",
            event = "materialized_view_tailer_replay_failed",
            error = %error,
            "materialized-view tailer failed to replay canonical transitions; retrying"
        );
    }
    budget.unwrap_or_else(|| MaterializedViewReplayBudget::new(tailer.config))
}

const SECONDS_PER_HOUR: u64 = 3_600;

/// Mirrors a stalled consumer cursor into the ready-but-warning readiness cause.
///
/// The persisted cursor names a retained canonical transition. Once it stops
/// advancing, the writer's retention sweep walks toward it, so a long stall is
/// the operator's lead time before the plane needs a rebuild. The warning
/// lives on the writer's admission state, which the follow loop keeps under a
/// lag-derived `Ready`; a refusing input such as the upstream probe still
/// supplies the projected cause while it fails.
struct CursorRiskWatch {
    readiness: ComposedReadiness,
    retention_hours: u64,
    warning_after: Duration,
    last_event_sequence: Option<u64>,
    last_advance: Instant,
    warned: bool,
}

impl CursorRiskWatch {
    fn new(tailer: &MaterializedViewTailer, readiness: ComposedReadiness) -> Option<Self> {
        let retention = tailer.chain_event_retention_window?;
        Some(Self {
            readiness,
            retention_hours: whole_hours(retention),
            warning_after: tailer.cursor_at_risk_warning,
            last_event_sequence: None,
            last_advance: Instant::now(),
            warned: false,
        })
    }

    fn observe(&mut self, materialized_view_store: &MaterializedViewStore) {
        let Ok(cursor) = persisted_chain_event_cursor(materialized_view_store) else {
            return;
        };
        let event_sequence = cursor.map(CanonicalEventCursor::event_sequence);
        if event_sequence != self.last_event_sequence {
            self.last_event_sequence = event_sequence;
            self.last_advance = Instant::now();
            self.clear();
            return;
        }
        let stalled_for = self.last_advance.elapsed();
        if stalled_for >= self.warning_after {
            self.raise(whole_hours(stalled_for));
        }
    }

    fn raise(&mut self, stalled_hours: u64) {
        let retention_hours = self.retention_hours;
        self.readiness.update_admission(|state| {
            if matches!(state.cause, ReadinessCause::Ready) {
                state.cause = ReadinessCause::CursorAtRisk {
                    oldest_retained_age_hours: stalled_hours,
                    retention_hours,
                };
            }
        });
        if self.warned {
            return;
        }
        self.warned = true;
        tracing::warn!(
            target: "zinder::ingest",
            event = "materialized_view_replay_cursor_at_risk",
            stalled_hours,
            retention_hours,
            "materialized-view consumer cursor has not advanced; canonical event retention will expire it"
        );
    }

    fn clear(&mut self) {
        self.readiness.update_admission(|state| {
            if matches!(state.cause, ReadinessCause::CursorAtRisk { .. }) {
                state.cause = ReadinessCause::Ready;
            }
        });
        if !self.warned {
            return;
        }
        self.warned = false;
        tracing::info!(
            target: "zinder::ingest",
            event = "materialized_view_replay_cursor_advanced",
            "materialized-view consumer cursor advanced; the cursor-at-risk warning is cleared"
        );
    }
}

const fn whole_hours(duration: Duration) -> u64 {
    duration.as_secs() / SECONDS_PER_HOUR
}

/// Logs the canonical-phase gate engage/disengage transition once per flip.
///
/// A `continuous` policy is configured never to throttle, so its engagement is
/// a `WARN` that the configured behavior is overridden while canonical bulk
/// catch-up owns the storage budget; every other transition is an `INFO`.
fn log_phase_gate_transition(
    replay_policy: MaterializedViewReplayPolicy,
    last_engaged: &mut Option<bool>,
    engaged: bool,
) {
    if *last_engaged == Some(engaged) {
        return;
    }
    let had_prior_state = last_engaged.is_some();
    *last_engaged = Some(engaged);
    if engaged {
        if replay_policy == MaterializedViewReplayPolicy::Continuous {
            tracing::warn!(
                target: "zinder::ingest",
                event = "materialized_view_replay_phase_gate_engaged",
                replay_policy = replay_policy.as_kebab_case(),
                "continuous materialized-view replay paused while canonical bulk catch-up owns the storage budget"
            );
        } else {
            tracing::info!(
                target: "zinder::ingest",
                event = "materialized_view_replay_phase_gate_engaged",
                replay_policy = replay_policy.as_kebab_case(),
                "materialized-view replay paused while canonical bulk catch-up owns the storage budget"
            );
        }
    } else if had_prior_state {
        tracing::info!(
            target: "zinder::ingest",
            event = "materialized_view_replay_phase_gate_disengaged",
            replay_policy = replay_policy.as_kebab_case(),
            "materialized-view replay resumed full scheduling after canonical bulk catch-up completed"
        );
    }
}

/// Spawns the materialized-view replay budget metric sampler.
///
/// The materialized-view tailer can stay inside one replay pass for a whole bulk catch-up
/// (hours), so its outer scheduling boundary is the wrong place to observe
/// phase-gate transitions. This task samples the budget on `sample_interval`,
/// keeping the replay budget gauges tied to current memory pressure and
/// emitting the phase-gate engage/disengage log within one sample of the flip
/// regardless of the tailer's progress. Owning both the gauge and the
/// transition log here keeps them from disagreeing.
#[must_use = "drop the handle to detach the materialized-view replay budget sampler or await it for symmetric shutdown"]
pub fn spawn_materialized_view_replay_budget_metrics_task(
    materialized_view_config: MaterializedViewReplayConfig,
    poll_interval: Duration,
    sample_interval: Duration,
    readiness: Readiness,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut replay_budget =
            MaterializedViewReplayBudget::with_phase_gate(materialized_view_config, readiness);
        let mut last_phase_gate_engaged: Option<bool> = None;
        loop {
            let effective_limits = replay_budget.evaluate_current();
            record_materialized_view_replay_budget(
                materialized_view_config.replay_policy,
                effective_limits,
                poll_interval,
            );
            log_phase_gate_transition(
                materialized_view_config.replay_policy,
                &mut last_phase_gate_engaged,
                effective_limits.phase_gate_engaged,
            );
            if sleep_or_cancel(sample_interval, &cancel).await {
                return;
            }
        }
    })
}

/// Mutable state one catch-up pass carries across its dispatched pages.
struct ReplayPass {
    budget: MaterializedViewReplayBudget,
    last_status_persist: Option<Instant>,
}

impl ReplayPass {
    const fn new(budget: MaterializedViewReplayBudget) -> Self {
        Self {
            budget,
            last_status_persist: None,
        }
    }

    fn evaluate(&mut self) -> EffectiveMaterializedViewReplayLimits {
        let effective_limits = self.budget.evaluate_current();
        record_materialized_view_replay_budget(
            self.budget.config.replay_policy,
            effective_limits,
            DEFAULT_MATERIALIZED_VIEW_TAILER_POLL_INTERVAL,
        );
        effective_limits
    }

    fn yields(&mut self) -> bool {
        self.budget.is_cancelled() || self.evaluate().state.is_paused()
    }

    /// Throttles the in-pass status refresh so a from-genesis rebuild keeps the
    /// operator-facing head truthful without one synced write per page.
    fn status_persist_is_due(&mut self) -> bool {
        if self
            .last_status_persist
            .is_some_and(|at| at.elapsed() < MATERIALIZED_VIEW_STATUS_PERSIST_INTERVAL)
        {
            return false;
        }
        self.last_status_persist = Some(Instant::now());
        true
    }
}

/// Dispatches block-keyed chain-event consumers against parsed block contexts
/// and lets `MaterializedViewStore` own the write-batch boundary.
pub(crate) fn dispatch_chain_event(
    materialized_view_store: &MaterializedViewStore,
    inputs: ChainEventDispatchInputs<'_>,
    blocks: &HashMap<BlockHeight, Arc<BlockCommitContext>>,
    advance_cursor: bool,
) -> Result<(), IngestError> {
    let _write_guard = materialized_view_write_guard();
    let mut constructed = Vec::new();
    for selected in selected_block_consumers(materialized_view_store) {
        if selected.receives_chain_events(materialized_view_store)? {
            constructed.push((selected.construct)());
        }
    }
    let mut consumers: Vec<&mut dyn BlockKeyedConsumer> = Vec::with_capacity(constructed.len());
    for consumer in &mut constructed {
        consumers.push(consumer.as_mut());
    }
    let measurements = materialized_view_store
        .write_chain_event_chunk(consumers.as_mut_slice(), inputs, blocks, advance_cursor)
        .map_err(|error| IngestError::MaterializedViewDispatch(error.to_string()))?;
    record_materialized_view_write_measurements(
        &measurements,
        MATERIALIZED_VIEW_WRITE_SOURCE_CHAIN_EVENT,
        blocks.len(),
    );
    Ok(())
}

fn dispatch_event_only_chain_event(
    materialized_view_store: &MaterializedViewStore,
    inputs: ChainEventDispatchInputs<'_>,
) -> Result<(), IngestError> {
    let mut constructed = selected_event_only_consumers(materialized_view_store);
    if constructed.is_empty() {
        return Ok(());
    }
    let _write_guard = materialized_view_write_guard();
    let mut block_consumers: [&mut dyn BlockKeyedConsumer; 0] = [];
    let mut event_consumers: Vec<&mut dyn MaterializedViewConsumer> =
        Vec::with_capacity(constructed.len());
    for consumer in &mut constructed {
        event_consumers.push(consumer.as_mut());
    }
    let blocks = HashMap::<BlockHeight, Arc<BlockCommitContext>>::new();
    let measurements = materialized_view_store
        .write_chain_event_chunk_with_event_consumers(
            ChainEventDispatchConsumers {
                block_consumers: &mut block_consumers,
                event_consumers: &mut event_consumers,
            },
            inputs,
            &blocks,
            true,
        )
        .map_err(|error| IngestError::MaterializedViewDispatch(error.to_string()))?;
    record_materialized_view_write_measurements(
        &measurements,
        MATERIALIZED_VIEW_WRITE_SOURCE_CHAIN_EVENT,
        0,
    );
    Ok(())
}

fn persisted_event_only_chain_event_cursor(
    materialized_view_store: &MaterializedViewStore,
) -> Result<Option<CanonicalEventCursor>, IngestError> {
    let mut cursor: Option<Vec<u8>> = None;
    for consumer_name in materialized_view_store.event_only_chain_event_consumer_names() {
        let Some(candidate) = materialized_view_store.get_chain_event_cursor(consumer_name)? else {
            return Ok(None);
        };
        if cursor
            .as_ref()
            .is_some_and(|existing| existing != &candidate)
        {
            return Err(IngestError::MaterializedViewDispatch(
                "event-only materialized-view consumer cursors disagree".to_owned(),
            ));
        }
        cursor = Some(candidate);
    }
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    decode_persisted_cursor(materialized_view_store, &cursor).map(Some)
}

fn persisted_chain_event_cursor(
    materialized_view_store: &MaterializedViewStore,
) -> Result<Option<CanonicalEventCursor>, IngestError> {
    let mut cursor: Option<Vec<u8>> = None;
    for selected in selected_block_consumers(materialized_view_store) {
        if !selected.receives_chain_events(materialized_view_store)? {
            continue;
        }
        let consumer_name = selected.name;
        // A consumer without a cursor is fresh or was reset by a scoped schema
        // rebuild; the plane then rebuilds from canonical heights while the
        // others re-apply the same deterministic rows idempotently.
        let Some(candidate) = materialized_view_store.get_chain_event_cursor(consumer_name)? else {
            return Ok(None);
        };
        if cursor
            .as_ref()
            .is_some_and(|existing| existing != &candidate)
        {
            return Err(IngestError::MaterializedViewDispatch(
                "chain materialized-view consumer cursors disagree".to_owned(),
            ));
        }
        cursor = Some(candidate);
    }
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    decode_persisted_cursor(materialized_view_store, &cursor).map(Some)
}

fn decode_persisted_cursor(
    materialized_view_store: &MaterializedViewStore,
    cursor: &[u8],
) -> Result<CanonicalEventCursor, IngestError> {
    CanonicalEventCursor::from_persisted(cursor).map_err(|source| {
        IngestError::MaterializedViewCursorUnreadable {
            path: materialized_view_store.storage_path().to_path_buf(),
            source,
        }
    })
}

fn record_materialized_view_replay_stage<T>(
    stage: &'static str,
    started_at: Instant,
    outcome: &Result<T, IngestError>,
) {
    metrics::histogram!(
        "zinder_materialized_view_replay_stage_duration_seconds",
        "stage" => stage,
        "status" => outcome_status(outcome),
        "error_class" => ingest_error_class(outcome.as_ref().err())
    )
    .record(started_at.elapsed());
}

fn record_materialized_view_replay_event(block_count: usize, error: Option<&IngestError>) {
    let status = if error.is_some() { "error" } else { "ok" };
    let error_class = ingest_error_class(error);
    metrics::counter!(
        "zinder_materialized_view_replay_events_total",
        "status" => status,
        "error_class" => error_class
    )
    .increment(1);
    metrics::counter!(
        "zinder_materialized_view_replay_blocks_total",
        "status" => status,
        "error_class" => error_class
    )
    .increment(usize_to_u64_saturating(block_count));
}

fn record_materialized_view_write_measurements(
    measurements: &[MaterializedViewWriteMeasurement],
    source: &'static str,
    block_count: usize,
) {
    for measurement in measurements {
        let consumer = measurement.consumer.as_str();
        metrics::counter!(
            "zinder_materialized_view_write_operations_total",
            "consumer" => consumer,
            "source" => source
        )
        .increment(measurement.operations);
        metrics::counter!(
            "zinder_materialized_view_write_bytes_total",
            "consumer" => consumer,
            "source" => source
        )
        .increment(measurement.logical_bytes);
        metrics::histogram!(
            "zinder_materialized_view_dispatch_duration_seconds",
            "consumer" => consumer,
            "source" => source
        )
        .record(measurement.dispatch_duration);
        if source == MATERIALIZED_VIEW_WRITE_SOURCE_CHAIN_EVENT {
            metrics::counter!(
                "zinder_materialized_view_replay_dispatches_total",
                "consumer" => consumer
            )
            .increment(1);
            metrics::counter!(
                "zinder_materialized_view_replay_blocks_total",
                "consumer" => consumer
            )
            .increment(usize_to_u64_saturating(block_count));
        }
    }
}

fn record_materialized_view_consumer_replay_progress(
    consumers: impl IntoIterator<Item = MaterializedViewConsumerName>,
    progress_height: BlockHeight,
    canonical_tip_height: BlockHeight,
) {
    let replay_lag_blocks = canonical_tip_height
        .value()
        .saturating_sub(progress_height.value());
    for consumer in consumers {
        let consumer = consumer.as_str();
        metrics::gauge!(
            "zinder_materialized_view_replay_height",
            "consumer" => consumer
        )
        .set(f64::from(progress_height.value()));
        metrics::gauge!(
            "zinder_materialized_view_replay_lag_blocks",
            "consumer" => consumer
        )
        .set(f64::from(replay_lag_blocks));
    }
}

fn record_materialized_view_replay_status_metrics(
    indexed_height: Option<u32>,
    canonical_tip_height: Option<u32>,
) {
    let indexed_height = indexed_height.unwrap_or(0);
    let canonical_tip_height = canonical_tip_height.unwrap_or(0);
    metrics::gauge!("zinder_materialized_view_replay_height").set(f64::from(indexed_height));
    metrics::gauge!("zinder_materialized_view_replay_tip_height")
        .set(f64::from(canonical_tip_height));
    metrics::gauge!("zinder_materialized_view_replay_lag_blocks").set(f64::from(
        canonical_tip_height.saturating_sub(indexed_height),
    ));
}

fn record_current_materialized_view_replay_tip(
    canonical: &RocksDbCanonicalSecondary,
) -> Option<u32> {
    let tip_height = canonical
        .chain_epoch()
        .ok()
        .map(|chain_epoch| chain_epoch.visible_tip_height.value())?;
    metrics::gauge!("zinder_materialized_view_replay_tip_height").set(f64::from(tip_height));
    Some(tip_height)
}

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

fn record_materialized_view_replay_budget(
    replay_policy: MaterializedViewReplayPolicy,
    effective_limits: EffectiveMaterializedViewReplayLimits,
    poll_interval: Duration,
) {
    metrics::gauge!(
        "zinder_materialized_view_replay_policy",
        "policy" => replay_policy.as_kebab_case()
    )
    .set(1.0);
    for state in [
        MaterializedViewReplayBudgetState::Normal,
        MaterializedViewReplayBudgetState::Degraded,
        MaterializedViewReplayBudgetState::Paused,
    ] {
        metrics::gauge!(
            "zinder_materialized_view_replay_budget_state",
            "state" => state.as_label()
        )
        .set(if effective_limits.state == state {
            1.0
        } else {
            0.0
        });
    }
    metrics::gauge!("zinder_materialized_view_replay_effective_batch_blocks")
        .set(f64::from(effective_limits.batch_blocks));
    if let Some(memory_budget_bytes) = effective_limits.memory_budget_bytes {
        metrics::gauge!("zinder_materialized_view_replay_memory_budget_bytes")
            .set(u64_to_f64(memory_budget_bytes));
    }
    metrics::gauge!("zinder_materialized_view_replay_paused").set(
        if effective_limits.state.is_paused() {
            1.0
        } else {
            0.0
        },
    );
    metrics::gauge!("zinder_materialized_view_replay_phase_gate").set(
        if effective_limits.phase_gate_engaged {
            1.0
        } else {
            0.0
        },
    );
    metrics::gauge!("zinder_materialized_view_replay_budget_seconds").set(
        if effective_limits.state.is_paused() {
            0.0
        } else {
            poll_interval.as_secs_f64()
        },
    );
}

fn record_materialized_view_tailer_tick(started_at: Instant, outcome: &Result<(), IngestError>) {
    metrics::histogram!(
        "zinder_materialized_view_tailer_tick_duration_seconds",
        "status" => outcome_status(outcome),
        "error_class" => ingest_error_class(outcome.as_ref().err())
    )
    .record(started_at.elapsed());
    metrics::counter!(
        "zinder_materialized_view_tailer_ticks_total",
        "status" => outcome_status(outcome),
        "error_class" => ingest_error_class(outcome.as_ref().err())
    )
    .increment(1);
}

fn usize_to_u64_saturating(amount: usize) -> u64 {
    u64::try_from(amount).unwrap_or(u64::MAX)
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus gauges use f64 samples; memory byte counts are diagnostic magnitudes"
)]
fn u64_to_f64(sample: u64) -> f64 {
    sample as f64
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use zinder_core::{BlockHash, ChainEpochId, ChainTipMetadata, Network, UnixTimestampMillis};

    use super::*;

    fn replay_config() -> MaterializedViewReplayConfig {
        MaterializedViewReplayConfig {
            replay_batch_blocks: nonzero_u32(100),
            replay_policy: MaterializedViewReplayPolicy::CanonicalFirst,
            memory_budget_bytes: NonZeroU64::new(1_000),
            memory_degrade_ratio: 0.85,
            memory_pause_ratio: 0.95,
            memory_resume_ratio: 0.75,
            min_replay_batch_blocks: nonzero_u32(10),
        }
    }

    fn cursor_risk_watch(readiness: ComposedReadiness) -> CursorRiskWatch {
        CursorRiskWatch {
            readiness,
            retention_hours: 168,
            warning_after: Duration::from_hours(24),
            last_event_sequence: None,
            last_advance: Instant::now(),
            warned: false,
        }
    }

    #[test]
    fn a_stalled_cursor_warns_only_while_the_runtime_is_otherwise_ready() {
        let readiness = Readiness::default();
        let composed = ComposedReadiness::new(readiness.clone(), &[]);
        let mut watch = cursor_risk_watch(composed.clone());
        composed.publish_admission(zinder_runtime::ReadinessState::syncing(
            Some(10),
            Some(90),
            Some(100),
        ));

        watch.raise(25);

        assert!(matches!(
            readiness.report().cause,
            ReadinessCause::Syncing { .. }
        ));

        composed.publish_admission(zinder_runtime::ReadinessState::ready(Some(100)));
        watch.raise(25);

        assert!(matches!(
            readiness.report().cause,
            ReadinessCause::CursorAtRisk {
                oldest_retained_age_hours: 25,
                retention_hours: 168,
            }
        ));
    }

    #[test]
    fn an_advancing_cursor_clears_the_warning_without_touching_other_causes() {
        let readiness = Readiness::default();
        let composed = ComposedReadiness::new(readiness.clone(), &[]);
        let mut watch = cursor_risk_watch(composed.clone());
        composed.publish_admission(zinder_runtime::ReadinessState::cursor_at_risk(
            25,
            168,
            Some(100),
        ));

        watch.clear();

        assert!(matches!(readiness.report().cause, ReadinessCause::Ready));

        composed.publish_admission(zinder_runtime::ReadinessState::syncing(
            Some(10),
            Some(90),
            Some(100),
        ));
        watch.clear();

        assert!(matches!(
            readiness.report().cause,
            ReadinessCause::Syncing { .. }
        ));
    }

    fn memory_snapshot(current_bytes: u64) -> RuntimeMemorySnapshot {
        RuntimeMemorySnapshot {
            cgroup_current_bytes: Some(current_bytes),
            cgroup_anon_bytes: Some(current_bytes),
            cgroup_max_bytes: Some(1_000),
            ..RuntimeMemorySnapshot::default()
        }
    }

    fn chain_epoch() -> ChainEpoch {
        let tip_hash = BlockHash::from_bytes([0x42; 32]);
        ChainEpoch {
            id: ChainEpochId::new(1),
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(10),
            visible_tip_hash: tip_hash,
            settled_tip_height: BlockHeight::new(10),
            settled_tip_hash: tip_hash,
            artifact_schema_version: zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION,
            tip_metadata: ChainTipMetadata::empty(),
            created_at: UnixTimestampMillis::new(1),
        }
    }

    #[test]
    fn replay_budget_degrades_batch_before_pause() {
        let mut budget = MaterializedViewReplayBudget::new(replay_config());

        let normal = budget.evaluate(memory_snapshot(800), Some(IngestPhase::FollowingTip));
        assert_eq!(normal.state, MaterializedViewReplayBudgetState::Normal);
        assert_eq!(normal.batch_blocks, 100);

        let degraded = budget.evaluate(memory_snapshot(875), Some(IngestPhase::FollowingTip));
        assert_eq!(degraded.state, MaterializedViewReplayBudgetState::Degraded);
        assert_eq!(degraded.batch_blocks, 50);

        let minimum = budget.evaluate(memory_snapshot(925), Some(IngestPhase::FollowingTip));
        assert_eq!(minimum.state, MaterializedViewReplayBudgetState::Degraded);
        assert_eq!(minimum.batch_blocks, 10);

        let paused = budget.evaluate(memory_snapshot(950), Some(IngestPhase::FollowingTip));
        assert_eq!(paused.state, MaterializedViewReplayBudgetState::Paused);
        assert_eq!(paused.batch_blocks, 0);
    }

    #[test]
    fn replay_budget_resumes_paused_replay_as_degraded_work() {
        let mut budget = MaterializedViewReplayBudget::new(replay_config());

        assert_eq!(
            budget
                .evaluate(memory_snapshot(960), Some(IngestPhase::FollowingTip))
                .state,
            MaterializedViewReplayBudgetState::Paused
        );
        assert_eq!(
            budget
                .evaluate(memory_snapshot(900), Some(IngestPhase::FollowingTip))
                .state,
            MaterializedViewReplayBudgetState::Degraded
        );
        assert_eq!(
            budget
                .evaluate(memory_snapshot(800), Some(IngestPhase::FollowingTip))
                .state,
            MaterializedViewReplayBudgetState::Degraded
        );
        assert_eq!(
            budget
                .evaluate(memory_snapshot(700), Some(IngestPhase::FollowingTip))
                .state,
            MaterializedViewReplayBudgetState::Normal
        );
    }

    #[test]
    fn replay_budget_uses_anon_pressure_when_cgroup_stat_is_available() {
        let mut budget = MaterializedViewReplayBudget::new(replay_config());
        let snapshot = RuntimeMemorySnapshot {
            cgroup_current_bytes: Some(980),
            cgroup_max_bytes: Some(1_000),
            cgroup_anon_bytes: Some(200),
            cgroup_active_file_bytes: Some(480),
            cgroup_inactive_file_bytes: Some(300),
            ..RuntimeMemorySnapshot::default()
        };

        let limits = budget.evaluate(snapshot, Some(IngestPhase::FollowingTip));

        assert_eq!(limits.memory_pressure_ratio, Some(0.2));
        assert_eq!(limits.state, MaterializedViewReplayBudgetState::Normal);
        assert_eq!(limits.batch_blocks, 100);
    }

    #[test]
    fn replay_budget_uses_process_rss_anon_when_cgroup_anon_is_absent() {
        let mut budget = MaterializedViewReplayBudget::new(replay_config());
        let snapshot = RuntimeMemorySnapshot {
            cgroup_current_bytes: Some(980),
            cgroup_max_bytes: Some(1_000),
            cgroup_anon_bytes: None,
            cgroup_active_file_bytes: Some(480),
            cgroup_inactive_file_bytes: Some(300),
            process_rss_anon_bytes: Some(875),
            ..RuntimeMemorySnapshot::default()
        };

        let limits = budget.evaluate(snapshot, Some(IngestPhase::FollowingTip));

        assert_eq!(limits.memory_pressure_ratio, Some(0.875));
        assert_eq!(limits.state, MaterializedViewReplayBudgetState::Degraded);
        assert_eq!(limits.batch_blocks, 50);
    }

    fn continuous_config() -> MaterializedViewReplayConfig {
        MaterializedViewReplayConfig {
            replay_policy: MaterializedViewReplayPolicy::Continuous,
            ..replay_config()
        }
    }

    #[test]
    fn continuous_bulk_catchup_pauses_replay() {
        let mut budget = MaterializedViewReplayBudget::new(continuous_config());

        let limits = budget.evaluate(memory_snapshot(800), Some(IngestPhase::BulkCatchup));

        assert!(limits.phase_gate_engaged);
        assert_eq!(limits.state, MaterializedViewReplayBudgetState::Paused);
        assert_eq!(limits.batch_blocks, 0);
    }

    #[test]
    fn unclassified_startup_phase_pauses_replay() {
        let mut budget = MaterializedViewReplayBudget::new(continuous_config());

        let limits = budget.evaluate(memory_snapshot(800), None);

        assert!(limits.phase_gate_engaged);
        assert_eq!(limits.state, MaterializedViewReplayBudgetState::Paused);
        assert_eq!(limits.batch_blocks, 0);
    }

    #[test]
    fn replay_without_a_phase_gate_is_allowed() {
        let mut budget = MaterializedViewReplayBudget::new(continuous_config());

        let limits = budget.evaluate_current();

        assert!(!limits.phase_gate_engaged);
        assert_eq!(limits.state, MaterializedViewReplayBudgetState::Normal);
        assert_eq!(limits.batch_blocks, 100);
    }

    #[test]
    fn installed_but_unclassified_phase_gate_pauses_replay() {
        let readiness = Readiness::default();
        let mut budget =
            MaterializedViewReplayBudget::with_phase_gate(continuous_config(), readiness);

        let limits = budget.evaluate_current();

        assert!(limits.phase_gate_engaged);
        assert_eq!(limits.state, MaterializedViewReplayBudgetState::Paused);
        assert_eq!(limits.batch_blocks, 0);
    }

    #[test]
    fn awaiting_upstream_phase_pauses_replay() {
        let mut budget = MaterializedViewReplayBudget::new(continuous_config());

        let limits = budget.evaluate(memory_snapshot(800), Some(IngestPhase::AwaitingUpstream));

        assert!(limits.phase_gate_engaged);
        assert_eq!(limits.state, MaterializedViewReplayBudgetState::Paused);
        assert_eq!(limits.batch_blocks, 0);
    }

    #[test]
    fn continuous_following_tip_stays_unthrottled_under_memory_pressure() {
        let mut budget = MaterializedViewReplayBudget::new(continuous_config());

        let limits = budget.evaluate(memory_snapshot(950), Some(IngestPhase::FollowingTip));

        assert!(!limits.phase_gate_engaged);
        assert_eq!(limits.state, MaterializedViewReplayBudgetState::Normal);
        assert_eq!(limits.batch_blocks, 100);
    }

    #[test]
    fn canonical_first_composes_memory_pause_with_bulk_catchup_gate() {
        let mut budget = MaterializedViewReplayBudget::new(replay_config());

        let phase_paused = budget.evaluate(memory_snapshot(800), Some(IngestPhase::BulkCatchup));
        assert!(phase_paused.phase_gate_engaged);
        assert_eq!(
            phase_paused.state,
            MaterializedViewReplayBudgetState::Paused
        );
        assert_eq!(phase_paused.batch_blocks, 0);

        let paused = budget.evaluate(memory_snapshot(950), Some(IngestPhase::BulkCatchup));
        assert!(paused.phase_gate_engaged);
        assert_eq!(paused.state, MaterializedViewReplayBudgetState::Paused);
        assert_eq!(paused.batch_blocks, 0);
    }

    #[test]
    fn phase_gate_disengages_when_phase_leaves_bulk_catchup() {
        let mut budget = MaterializedViewReplayBudget::new(continuous_config());

        let engaged = budget.evaluate(memory_snapshot(800), Some(IngestPhase::BulkCatchup));
        assert!(engaged.phase_gate_engaged);
        assert_eq!(engaged.state, MaterializedViewReplayBudgetState::Paused);
        assert_eq!(engaged.batch_blocks, 0);

        let disengaged = budget.evaluate(memory_snapshot(800), Some(IngestPhase::FollowingTip));
        assert!(!disengaged.phase_gate_engaged);
        assert_eq!(disengaged.state, MaterializedViewReplayBudgetState::Normal);
        assert_eq!(disengaged.batch_blocks, 100);
    }

    #[test]
    fn phase_gate_survives_readiness_cause_replacement() {
        let readiness = Readiness::default();
        readiness.set_phase(IngestPhase::BulkCatchup);
        let mut budget =
            MaterializedViewReplayBudget::with_phase_gate(continuous_config(), readiness.clone());

        // Bulk catch-up replaces the readiness cause on every committed batch
        // and every upstream-outage backoff; the gate must keep reading
        // BulkCatchup rather than fail open to unthrottled replay.
        readiness.set(zinder_runtime::ReadinessState::syncing(
            Some(1_500_000),
            Some(100),
            Some(1_500_100),
        ));

        let limits = budget.evaluate_current();
        assert!(limits.phase_gate_engaged);
        assert_ne!(limits.state, MaterializedViewReplayBudgetState::Normal);
    }

    #[test]
    fn replay_budget_observes_process_cancellation() {
        let cancel = CancellationToken::new();
        let budget = MaterializedViewReplayBudget::with_phase_gate_and_cancel(
            replay_config(),
            Readiness::default(),
            cancel.clone(),
        );

        assert!(!budget.is_cancelled());
        cancel.cancel();
        assert!(budget.is_cancelled());
    }

    #[test]
    fn phase_gate_transition_logs_once_per_flip() {
        let mut last_engaged = None;

        log_phase_gate_transition(
            MaterializedViewReplayPolicy::Continuous,
            &mut last_engaged,
            false,
        );
        assert_eq!(last_engaged, Some(false));

        log_phase_gate_transition(
            MaterializedViewReplayPolicy::Continuous,
            &mut last_engaged,
            true,
        );
        assert_eq!(last_engaged, Some(true));

        log_phase_gate_transition(
            MaterializedViewReplayPolicy::Continuous,
            &mut last_engaged,
            true,
        );
        assert_eq!(last_engaged, Some(true));

        log_phase_gate_transition(
            MaterializedViewReplayPolicy::Continuous,
            &mut last_engaged,
            false,
        );
        assert_eq!(last_engaged, Some(false));
    }

    #[test]
    fn replay_pages_stay_within_the_batch_and_canonical_scan_bounds() -> Result<(), IngestError> {
        let page = replay_page(BlockHeight::new(10), BlockHeight::new(1_000), 100)?;
        assert_eq!(page.start, BlockHeight::new(10));
        assert_eq!(page.end, BlockHeight::new(109));

        let short = replay_page(BlockHeight::new(10), BlockHeight::new(12), 100)?;
        assert_eq!(short.end, BlockHeight::new(12));

        let clamped = replay_page(BlockHeight::new(1), BlockHeight::new(u32::MAX), u32::MAX)?;
        assert_eq!(
            clamped.end,
            BlockHeight::new(MAX_CANONICAL_INCREMENTAL_REPLAY_BLOCKS)
        );

        let paused = replay_page(BlockHeight::new(5), BlockHeight::new(9), 0)?;
        assert_eq!(paused.end, BlockHeight::new(5));
        Ok(())
    }

    #[test]
    fn only_the_opening_page_of_a_reorg_carries_its_revert() -> Result<(), IngestError> {
        let chain_epoch = chain_epoch();
        let transition = DispatchedTransition {
            chain_epoch,
            cursor: CanonicalEventCursor::at(7)?,
            committed_range: BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(4)),
            reverted: Some(ChainRangeReverted {
                chain_epoch,
                block_range: BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(3)),
            }),
        };
        let page = BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(2));

        assert!(matches!(
            transition.for_page(true).page_event(page),
            ChainEvent::ChainReorged { .. }
        ));
        assert!(matches!(
            transition.for_page(false).page_event(page),
            ChainEvent::ChainCommitted { .. }
        ));
        Ok(())
    }
}
