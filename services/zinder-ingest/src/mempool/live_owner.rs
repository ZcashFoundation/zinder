//! Live mempool ownership backed by the canonical event log.
//!
//! The mutable [`MempoolIndex`] remains process-local for low-latency wallet
//! overlays. Its resumable history, cursor head, and retention floor live in
//! the follower-owned canonical primary and are reached only through the
//! canonical control channel.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::FutureExt as _;
use parking_lot::{Mutex as ParkingMutex, RwLock};
use tokio::{
    sync::{Mutex, watch},
    time::MissedTickBehavior,
};
use tokio_stream::StreamExt as _;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use zinder_core::{
    BlockId, ChainEpoch, MempoolEntry, MempoolEvictionReason, TransactionId, UnixTimestampMillis,
};
use zinder_proto::{
    status_for_reason,
    v1::{ops::ErrorReason, wallet},
};
use zinder_source::{
    MempoolHydrationFailureReason, MempoolSource, MempoolSourceEvent, SourceError,
};
use zinder_store::{
    EventStreamStartPosition, MempoolEvent, MempoolEventPosition, MempoolEventRetentionConfig,
    MempoolEventRetentionStepBudget, MempoolEventRetentionStepOutcome,
    MempoolEventRetentionStepStop, StreamCursorTokenV1, mempool_event_envelope_message,
};

use crate::source_recovery::default_recovery_backoff;
use crate::writer::control::{CanonicalControlHandle, ParkedWriterHalt};

use super::{
    MempoolApplyOutcome, MempoolEntryBuildError, MempoolIndex, MempoolIndexPreflight,
    MempoolReadySignal, build_mempool_entry,
};

const MEMPOOL_EVENT_PAGE_SIZE: u32 = 64;
const MEMPOOL_RECONNECT_BACKOFF: Duration = Duration::from_secs(1);
const MEMPOOL_CANONICAL_TIP_POLL_INTERVAL: Duration = Duration::from_millis(25);
const MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS: usize = 256;
const MEMPOOL_LIFECYCLE_STATUS_LABELS: &[&str] =
    &["hydrating", "serving", "rebuild_required", "halted"];
/// Longest a live read waits for the owner to certify the canonical tip.
///
/// After a block, canonical ingest and the replacement source generation
/// reach the new tip independently. A read in between waits for the
/// replacement instead of refusing a wallet that asked at the wrong moment.
const MEMPOOL_TIP_TRANSITION_READ_WAIT: Duration = Duration::from_secs(5);
/// Default raw transaction payload budget for one durable reconciliation append.
pub const DEFAULT_RECONCILIATION_BATCH_TARGET_RAW_TRANSACTION_BYTES: u64 = 16_000_000;

/// One source generation buffered until its snapshot-complete marker.
///
/// Staging keeps a reconnect's partial observations out of the durable event
/// log. Until completion, a previously certified generation keeps answering
/// reads at its own tip. At completion, the owner reconciles the staged set
/// against the last durable/live set under the mutation gate, so no read
/// observes a partially reconciled index.
#[derive(Debug)]
struct StagedMempoolGeneration {
    index: MempoolIndex,
    terminal_events: HashMap<TransactionId, (MempoolEvent, UnixTimestampMillis)>,
    next_synthetic_sequence: u64,
    /// Blocks staged `Mined` transitions reported. Any block but the
    /// snapshot's tip means the source's chain moved away from that tip.
    mined_blocks: HashSet<BlockId>,
}

impl StagedMempoolGeneration {
    fn new() -> Self {
        Self {
            index: MempoolIndex::new(),
            terminal_events: HashMap::new(),
            next_synthetic_sequence: 0,
            mined_blocks: HashSet::new(),
        }
    }

    fn record_mined_block(&mut self, event: &MempoolEvent) {
        if let Some(mined_block) = mined_block(event) {
            self.mined_blocks.insert(mined_block);
        }
    }

    fn mined_block_off_tip(&self, source_tip: BlockId) -> Option<BlockId> {
        self.mined_blocks
            .iter()
            .copied()
            .find(|mined_block| *mined_block != source_tip)
    }

    fn next_position(
        &mut self,
        transaction_id: TransactionId,
    ) -> Result<MempoolEventPosition, Status> {
        self.next_synthetic_sequence =
            self.next_synthetic_sequence.checked_add(1).ok_or_else(|| {
                Status::resource_exhausted("staged mempool event sequence is exhausted")
            })?;
        Ok(MempoolEventPosition {
            event_sequence: self.next_synthetic_sequence,
            transaction_id,
        })
    }
}

/// A serving generation or staged snapshot that a `Mined` transition outran.
///
/// A generation opens a fresh change stream at its tip, so its only expected
/// `Mined` changes are for the tip block itself. A block above the tip, or at
/// its height on another branch, means the tip moved before its notification
/// arrived. A block below the tip means a reorg to a branch no higher than the
/// tip, whose transactions the tip's canonical chain may lack. Either way the
/// transaction could be missing from both views, so the generation ends
/// instead of applying the removal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GenerationOutran {
    /// Tip the ended generation was certified against.
    generation_source_tip: BlockId,
    /// Block the transition reported.
    observed_source_tip: BlockId,
}

/// Returns the block a `Mined` transition reports.
fn mined_block(event: &MempoolEvent) -> Option<BlockId> {
    if let MempoolEvent::Mined {
        mined_height,
        block_hash,
        ..
    } = event
    {
        return Some(BlockId::new(*mined_height, *block_hash));
    }
    None
}

/// Snapshot page read under the same gate as durable event append and index mutation.
#[derive(Clone, Debug)]
pub(crate) struct LiveMempoolSnapshotPage {
    pub(crate) source_tip: BlockId,
    pub(crate) entries: Vec<Arc<MempoolEntry>>,
    pub(crate) events_resume_cursor: Vec<u8>,
    pub(crate) snapshot_age_millis: u64,
    pub(crate) next_cursor: Vec<u8>,
}

/// Live mempool ownership state visible to the private control surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MempoolOwnerStatus {
    /// No certified generation exists yet, so the first source generation
    /// builds an in-memory snapshot that must not be exposed until its
    /// completion marker arrives.
    Hydrating,
    /// The durable log and process-local index have a verified transition
    /// path at `source_tip`. A replacement generation may be staging behind
    /// it after a normal tip change.
    Serving {
        source_tip: BlockId,
        snapshot_certified_at: UnixTimestampMillis,
    },
    /// A defensive post-append divergence was detected and the index must be reseeded.
    RebuildRequired,
    /// The canonical writer parked on a reorg deeper than its window. The
    /// owner no longer follows the node mempool, and every read and stream
    /// refuses with the writer's typed halt until the process restarts.
    Halted(ParkedWriterHalt),
}

impl MempoolOwnerStatus {
    /// Whether the owner still follows its source, so a staged generation may
    /// complete. A rebuild or a halt discards staging instead.
    const fn follows_source(self) -> bool {
        matches!(self, Self::Hydrating | Self::Serving { .. })
    }

    /// Returns the source tip and certification time of a serving index, or
    /// the status a request that needs one is refused with.
    fn serving_anchor(self) -> Result<(BlockId, UnixTimestampMillis), Status> {
        match self {
            Self::Serving {
                source_tip,
                snapshot_certified_at,
            } => Ok((source_tip, snapshot_certified_at)),
            Self::Hydrating => Err(Status::unavailable(
                "live mempool index is hydrating from an upstream snapshot",
            )),
            Self::RebuildRequired => Err(Status::unavailable(
                "live mempool index is rebuilding after a durable transition mismatch",
            )),
            Self::Halted(halt) => Err(halt.status()),
        }
    }
}

/// Process-local live index with a serialized durable-log handoff.
#[derive(Clone)]
pub struct LiveMempoolOwner {
    index: MempoolIndex,
    reconciliation_batch_target_raw_transaction_bytes: u64,
    /// Serializes preflight, durable append, index mutation, event-page reads,
    /// and snapshot anchoring. No public response can observe an event between
    /// durable append and its verified in-memory mutation.
    mutation_gate: Arc<Mutex<()>>,
    status: Arc<RwLock<MempoolOwnerStatus>>,
    /// Bumped on every status change so a waiting read can retry.
    status_changes: Arc<watch::Sender<u64>>,
    /// A generation hydrating behind the serving one, or the first one.
    staged_generation: Arc<ParkingMutex<Option<StagedMempoolGeneration>>>,
}

impl LiveMempoolOwner {
    /// Creates an empty live index backed by the configured canonical writer.
    #[must_use]
    pub fn new() -> Self {
        Self::with_batch_target(DEFAULT_RECONCILIATION_BATCH_TARGET_RAW_TRANSACTION_BYTES)
    }

    /// Creates an empty live index with a custom raw transaction payload budget
    /// for each durable reconciliation append.
    #[must_use]
    pub fn with_reconciliation_batch_target_raw_transaction_bytes(
        reconciliation_batch_target_raw_transaction_bytes: NonZeroU64,
    ) -> Self {
        Self::with_batch_target(reconciliation_batch_target_raw_transaction_bytes.get())
    }

    fn with_batch_target(reconciliation_batch_target_raw_transaction_bytes: u64) -> Self {
        let owner = Self {
            index: MempoolIndex::new(),
            reconciliation_batch_target_raw_transaction_bytes,
            mutation_gate: Arc::new(Mutex::new(())),
            status: Arc::new(RwLock::new(MempoolOwnerStatus::Hydrating)),
            status_changes: Arc::new(watch::Sender::new(0)),
            staged_generation: Arc::new(ParkingMutex::new(Some(StagedMempoolGeneration::new()))),
        };
        record_mempool_lifecycle_status(MempoolOwnerStatus::Hydrating);
        owner
    }

    /// Returns a durable/index consistency error while a source reseed is required.
    pub(crate) fn require_serving(&self) -> Result<(), Status> {
        let status = *self.status.read();
        status.serving_anchor().map(|_| ())
    }

    /// Refuses with the writer's typed halt once the owner is halted.
    fn require_not_halted(&self) -> Result<(), Status> {
        let status = *self.status.read();
        match status {
            MempoolOwnerStatus::Halted(halt) => Err(halt.status()),
            MempoolOwnerStatus::Hydrating
            | MempoolOwnerStatus::Serving { .. }
            | MempoolOwnerStatus::RebuildRequired => Ok(()),
        }
    }

    fn is_halted(&self) -> bool {
        matches!(*self.status.read(), MempoolOwnerStatus::Halted(_))
    }

    fn coherent_serving_state_for(
        &self,
        chain_epoch: ChainEpoch,
    ) -> Result<(BlockId, UnixTimestampMillis), Status> {
        let status = *self.status.read();
        let (source_tip, snapshot_certified_at) = status.serving_anchor()?;
        let visible_tip =
            BlockId::new(chain_epoch.visible_tip_height, chain_epoch.visible_tip_hash);
        if source_tip != visible_tip {
            return Err(status_for_reason(
                ErrorReason::ChainEpochPinUnavailable,
                "live mempool index is not coherent with the requested chain epoch",
            ));
        }
        Ok((source_tip, snapshot_certified_at))
    }

    /// Returns the live entry for a transaction at the canonical writer's
    /// current chain epoch, and that epoch.
    ///
    /// A read that arrives while canonical ingest and the live index are
    /// moving to a new tip waits up to [`MEMPOOL_TIP_TRANSITION_READ_WAIT`] for
    /// both to agree.
    pub(crate) async fn entry_at_canonical_tip(
        &self,
        canonical: &CanonicalControlHandle,
        transaction_id: TransactionId,
    ) -> Result<(ChainEpoch, Option<Arc<MempoolEntry>>), Status> {
        self.read_at_canonical_tip(canonical, |chain_epoch| {
            self.entry_for(chain_epoch, transaction_id)
        })
        .await
    }

    /// Reads one snapshot page at the canonical writer's current chain epoch,
    /// and returns that epoch.
    ///
    /// Like [`Self::entry_at_canonical_tip`], a read during a tip transition
    /// waits for canonical ingest and the live index to agree.
    pub(crate) async fn snapshot_page_at_canonical_tip(
        &self,
        canonical: &CanonicalControlHandle,
        max_entries: u32,
        from_cursor: Vec<u8>,
    ) -> Result<(ChainEpoch, LiveMempoolSnapshotPage), Status> {
        self.read_at_canonical_tip(canonical, |chain_epoch| {
            self.snapshot_page(canonical, chain_epoch, max_entries, from_cursor.clone())
        })
        .await
    }

    /// Runs `read` at the canonical writer's current chain epoch, retrying
    /// while a refusal can still turn into an answer.
    ///
    /// The status watch is subscribed before the epoch is captured, so a
    /// certification that lands after the capture always wakes the retry.
    /// The bound also covers the read itself, which waits for the mutation
    /// gate while a replacement generation reconciles.
    async fn read_at_canonical_tip<Answer, Read, ReadFuture>(
        &self,
        canonical: &CanonicalControlHandle,
        mut read: Read,
    ) -> Result<(ChainEpoch, Answer), Status>
    where
        Read: FnMut(ChainEpoch) -> ReadFuture,
        ReadFuture: Future<Output = Result<Answer, Status>>,
    {
        let deadline = tokio::time::Instant::now() + MEMPOOL_TIP_TRANSITION_READ_WAIT;
        loop {
            let mut status_changes = self.status_changes.subscribe();
            let chain_epoch = canonical.chain_epoch().await?.chain_epoch;
            let refusal = match tokio::time::timeout_at(deadline, read(chain_epoch)).await {
                Ok(Ok(answer)) => return Ok((chain_epoch, answer)),
                Ok(Err(refusal)) => refusal,
                Err(_elapsed) => return Err(tip_transition_read_expired()),
            };
            if tokio::time::Instant::now() >= deadline {
                return Err(refusal);
            }
            let visible_tip =
                BlockId::new(chain_epoch.visible_tip_height, chain_epoch.visible_tip_hash);
            // `set_status` publishes the watch bump before releasing the
            // status lock, so checking the watch after reading the status
            // catches a certification that landed after the refusal.
            let awaits_tip = self.awaits_tip(visible_tip);
            if status_changes.has_changed().unwrap_or(false) {
                continue;
            }
            if !awaits_tip {
                return Err(refusal);
            }
            match tokio::time::timeout_at(deadline, status_changes.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => return Err(refusal),
            }
        }
    }

    /// Whether a refused read at `visible_tip` can succeed once the owner
    /// certifies a generation, rather than failing for a lasting reason.
    fn awaits_tip(&self, visible_tip: BlockId) -> bool {
        let status = *self.status.read();
        match status {
            MempoolOwnerStatus::Hydrating => true,
            MempoolOwnerStatus::Serving { source_tip, .. } => source_tip != visible_tip,
            MempoolOwnerStatus::RebuildRequired | MempoolOwnerStatus::Halted(_) => false,
        }
    }

    /// Returns the live entry for a transaction when it is currently visible.
    pub(crate) async fn entry_for(
        &self,
        chain_epoch: ChainEpoch,
        transaction_id: TransactionId,
    ) -> Result<Option<Arc<MempoolEntry>>, Status> {
        self.require_serving()?;
        let _mutation_guard = self.mutation_gate.lock().await;
        self.coherent_serving_state_for(chain_epoch)?;
        Ok(self.index.entry_for(transaction_id))
    }

    /// Reads one in-memory snapshot page with a durable events-resume anchor.
    pub(crate) async fn snapshot_page(
        &self,
        canonical: &CanonicalControlHandle,
        chain_epoch: ChainEpoch,
        max_entries: u32,
        from_cursor: Vec<u8>,
    ) -> Result<LiveMempoolSnapshotPage, Status> {
        self.require_serving()?;
        let _mutation_guard = self.mutation_gate.lock().await;
        let (source_tip, snapshot_certified_at) = self.coherent_serving_state_for(chain_epoch)?;
        let start = canonical.begin_mempool_snapshot(from_cursor).await?;
        let snapshot = self
            .index
            .snapshot_page(max_entries, start.after_transaction_id());
        let next_cursor = match snapshot.next_after_transaction_id {
            Some(after_transaction_id) => canonical
                .encode_mempool_snapshot_next_cursor(
                    start.events_resume_anchor(),
                    after_transaction_id,
                )
                .await?
                .as_bytes()
                .to_vec(),
            None => Vec::new(),
        };
        let snapshot_age_millis = UnixTimestampMillis::now()
            .value()
            .saturating_sub(snapshot_certified_at.value());
        metrics::histogram!("zinder_mempool_snapshot_age_seconds")
            .record(Duration::from_millis(snapshot_age_millis));
        Ok(LiveMempoolSnapshotPage {
            source_tip,
            entries: snapshot.entries,
            events_resume_cursor: start
                .events_resume_cursor()
                .map_or_else(Vec::new, |cursor| cursor.as_bytes().to_vec()),
            snapshot_age_millis,
            next_cursor,
        })
    }

    /// Resolves a durable mempool stream start while excluding a concurrent index transition.
    pub(crate) async fn resolve_event_start(
        &self,
        canonical: &CanonicalControlHandle,
        start: EventStreamStartPosition,
    ) -> Result<Option<StreamCursorTokenV1>, Status> {
        let _mutation_guard = self.mutation_gate.lock().await;
        self.require_not_halted()?;
        canonical.resolve_mempool_event_start(start).await
    }

    /// Reads one durable mempool event page and encodes it for the public wire.
    pub(crate) async fn event_page(
        &self,
        canonical: &CanonicalControlHandle,
        after_cursor: Option<StreamCursorTokenV1>,
    ) -> Result<Vec<wallet::MempoolEventEnvelope>, Status> {
        let _mutation_guard = self.mutation_gate.lock().await;
        self.require_not_halted()?;
        canonical
            .mempool_event_page(
                after_cursor.map(|cursor| cursor.as_bytes().to_vec()),
                std::num::NonZeroU32::new(MEMPOOL_EVENT_PAGE_SIZE)
                    .ok_or_else(|| Status::internal("mempool event page size must be nonzero"))?,
            )
            .await?
            .iter()
            .map(|envelope| {
                mempool_event_envelope_message(envelope).map_err(|_| {
                    Status::failed_precondition("durable mempool event cannot be encoded")
                })
            })
            .collect()
    }

    /// Stages one source transition while a generation hydrates, or applies
    /// it durably to the serving generation otherwise.
    ///
    /// A generation never publishes partial snapshot observations to the
    /// durable log. Its marker first reconciles the staged state with the
    /// last durable/live state under the mutation gate.
    ///
    /// A serving generation answers "not in the mempool" for its source tip.
    /// Zebra's mempool changes and tip notifications have no shared order, so
    /// a `Mined` transition for a block other than the tip (the next block, or
    /// a block on a reorged branch) can arrive before the tip change. Removing
    /// the transaction then could leave it in neither the canonical chain at
    /// the tip nor the mempool, so such a transition starts a replacement
    /// generation instead, exactly as a tip change does. The serving
    /// generation keeps answering at its own tip until the replacement is
    /// certified. Only a `Mined` transition for the tip block itself removes
    /// the entry.
    pub(crate) async fn apply_event(
        &self,
        canonical: &CanonicalControlHandle,
        event: MempoolEvent,
        observed_at: UnixTimestampMillis,
    ) -> Result<Option<GenerationOutran>, Status> {
        let _mutation_guard = self.mutation_gate.lock().await;
        let state = *self.status.read();
        if state.follows_source() && self.staged_generation.lock().is_some() {
            self.stage_event_locked(event, observed_at)?;
            return Ok(None);
        }
        let (source_tip, _) = state.serving_anchor()?;
        if let Some(observed_source_tip) =
            mined_block(&event).filter(|mined_block| *mined_block != source_tip)
        {
            self.begin_next_generation_locked();
            return Ok(Some(GenerationOutran {
                generation_source_tip: source_tip,
                observed_source_tip,
            }));
        }
        self.append_and_apply_locked(canonical, event, observed_at)
            .await?;
        Ok(None)
    }

    async fn append_and_apply_locked(
        &self,
        canonical: &CanonicalControlHandle,
        event: MempoolEvent,
        observed_at: UnixTimestampMillis,
    ) -> Result<MempoolApplyOutcome, Status> {
        let preflight = self.index.preflight_event(&event).map_err(|error| {
            self.mark_rebuild_required();
            Status::failed_precondition(format!(
                "mempool index preflight rejected a source transition: {error}"
            ))
        })?;
        if preflight == MempoolIndexPreflight::NoChange {
            return Ok(MempoolApplyOutcome::NoChange);
        }

        let envelope = canonical
            .append_mempool_event(event.clone(), observed_at)
            .await?;
        let outcome = apply_to_index(&self.index, event, envelope.position())?;
        if outcome != MempoolApplyOutcome::Applied {
            self.mark_rebuild_required();
            return Err(Status::unavailable(
                "durable mempool event could not be applied to the live index",
            ));
        }
        record_mempool_size_gauge(&self.index);
        Ok(outcome)
    }

    async fn append_and_apply_reconciliation_transitions_locked(
        &self,
        canonical: &CanonicalControlHandle,
        transitions: Vec<(MempoolEvent, UnixTimestampMillis)>,
    ) -> Result<(), Status> {
        let mut batch = Vec::with_capacity(
            transitions
                .len()
                .min(MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS),
        );
        let mut batch_raw_transaction_bytes = 0_u64;
        for transition in transitions {
            let transition_raw_transaction_bytes = match &transition.0 {
                MempoolEvent::Added { entry } => {
                    u64::try_from(entry.raw_transaction_bytes().len()).unwrap_or(u64::MAX)
                }
                MempoolEvent::Invalidated { .. } | MempoolEvent::Mined { .. } | _ => 0,
            };
            let exceeds_event_limit = batch.len() >= MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS;
            let exceeds_raw_transaction_byte_limit = !batch.is_empty()
                && batch_raw_transaction_bytes.saturating_add(transition_raw_transaction_bytes)
                    > self.reconciliation_batch_target_raw_transaction_bytes;
            if exceeds_event_limit || exceeds_raw_transaction_byte_limit {
                self.append_and_apply_reconciliation_batch_locked(
                    canonical,
                    std::mem::take(&mut batch),
                )
                .await?;
                batch_raw_transaction_bytes = 0;
            }
            batch_raw_transaction_bytes =
                batch_raw_transaction_bytes.saturating_add(transition_raw_transaction_bytes);
            batch.push(transition);
        }
        if !batch.is_empty() {
            self.append_and_apply_reconciliation_batch_locked(canonical, batch)
                .await?;
        }
        Ok(())
    }

    async fn append_and_apply_reconciliation_batch_locked(
        &self,
        canonical: &CanonicalControlHandle,
        transitions: Vec<(MempoolEvent, UnixTimestampMillis)>,
    ) -> Result<(), Status> {
        let mut applicable_transitions = Vec::with_capacity(transitions.len());
        for (event, observed_at) in transitions {
            let preflight = self.index.preflight_event(&event).map_err(|error| {
                self.mark_rebuild_required();
                Status::failed_precondition(format!(
                    "mempool index preflight rejected a reconciliation transition: {error}"
                ))
            })?;
            if preflight == MempoolIndexPreflight::Apply {
                applicable_transitions.push((event, observed_at));
            }
        }
        if applicable_transitions.is_empty() {
            return Ok(());
        }

        let expected_envelope_count = applicable_transitions.len();
        let envelopes = canonical
            .append_mempool_events(applicable_transitions)
            .await?;
        if envelopes.len() != expected_envelope_count {
            self.mark_rebuild_required();
            return Err(Status::unavailable(
                "durable mempool reconciliation returned an incomplete transition batch",
            ));
        }
        for envelope in envelopes {
            let position = envelope.position();
            let outcome =
                apply_to_index(&self.index, envelope.event, position).inspect_err(|_status| {
                    self.mark_rebuild_required();
                })?;
            if outcome != MempoolApplyOutcome::Applied {
                self.mark_rebuild_required();
                return Err(Status::unavailable(
                    "durable mempool reconciliation could not be applied to the live index",
                ));
            }
        }
        record_mempool_size_gauge(&self.index);
        Ok(())
    }

    fn stage_event_locked(
        &self,
        event: MempoolEvent,
        observed_at: UnixTimestampMillis,
    ) -> Result<MempoolApplyOutcome, Status> {
        let transaction_id = event.transaction_id();
        let mut staged_generation_guard = self.staged_generation.lock();
        let staged_generation = staged_generation_guard.as_mut().ok_or_else(|| {
            Status::failed_precondition("mempool source emitted a transition outside hydration")
        })?;

        match &event {
            MempoolEvent::Added { .. } => {
                staged_generation.terminal_events.remove(&transaction_id);
            }
            MempoolEvent::Invalidated { .. } | MempoolEvent::Mined { .. } => {
                staged_generation.record_mined_block(&event);
                staged_generation
                    .terminal_events
                    .insert(transaction_id, (event.clone(), observed_at));
            }
            _ => {
                self.mark_rebuild_required();
                return Err(Status::failed_precondition(
                    "mempool source event variant is unsupported during hydration",
                ));
            }
        }

        let preflight = staged_generation
            .index
            .preflight_event(&event)
            .map_err(|error| {
                self.mark_rebuild_required();
                Status::failed_precondition(format!(
                    "staged mempool index preflight rejected a source transition: {error}"
                ))
            })?;
        if preflight == MempoolIndexPreflight::NoChange {
            return Ok(MempoolApplyOutcome::NoChange);
        }
        let position = staged_generation.next_position(transaction_id)?;
        let outcome = apply_to_index(&staged_generation.index, event, position)?;
        if outcome != MempoolApplyOutcome::Applied {
            self.mark_rebuild_required();
            return Err(Status::unavailable(
                "staged mempool transition could not be applied to the live-index model",
            ));
        }
        drop(staged_generation_guard);
        Ok(outcome)
    }

    /// Prunes durable mempool history without opening a second primary.
    pub(crate) async fn prune_events(
        &self,
        canonical: &CanonicalControlHandle,
        now: UnixTimestampMillis,
        retention: MempoolEventRetentionConfig,
        budget: MempoolEventRetentionStepBudget,
    ) -> Result<MempoolEventRetentionStepOutcome, Status> {
        let _mutation_guard = self.mutation_gate.lock().await;
        self.require_not_halted()?;
        canonical.prune_mempool_events(now, retention, budget).await
    }

    /// Withdraws the live index for the rest of the process because the
    /// canonical writer parked.
    pub(crate) async fn halt(&self, halt: ParkedWriterHalt) {
        let _mutation_guard = self.mutation_gate.lock().await;
        self.staged_generation.lock().take();
        self.set_status(MempoolOwnerStatus::Halted(halt));
        self.index.reset();
        record_mempool_size_gauge(&self.index);
    }

    fn mark_rebuild_required(&self) {
        self.set_status(MempoolOwnerStatus::RebuildRequired);
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the status-change bump must be visible before a reader can observe the new status"
    )]
    fn set_status(&self, status: MempoolOwnerStatus) {
        let mut current = self.status.write();
        // A halt is terminal for the process; nothing may resume serving.
        if matches!(*current, MempoolOwnerStatus::Halted(_)) {
            return;
        }
        *current = status;
        self.status_changes
            .send_modify(|changes| *changes = changes.wrapping_add(1));
        drop(current);
        record_mempool_lifecycle_status(status);
    }

    /// Starts staging a new source generation and returns whether a certified
    /// generation keeps serving behind it.
    async fn begin_next_generation(&self) -> bool {
        let _mutation_guard = self.mutation_gate.lock().await;
        self.begin_next_generation_locked()
    }

    /// Stages a new source generation behind the serving one, or hydrates
    /// from scratch when none is serving.
    ///
    /// A normal tip change keeps the serving generation: it still describes
    /// the mempool at its own tip, which is exactly what a reader whose
    /// canonical epoch has not moved yet needs.
    fn begin_next_generation_locked(&self) -> bool {
        *self.staged_generation.lock() = Some(StagedMempoolGeneration::new());
        if matches!(*self.status.read(), MempoolOwnerStatus::Serving { .. }) {
            return true;
        }
        self.set_status(MempoolOwnerStatus::Hydrating);
        false
    }

    /// Reconciles a complete source snapshot once its tip matches canonical
    /// ingest, or keeps the snapshot private while canonical ingest catches up.
    /// A snapshot a staged `Mined` transition outran ends without certifying,
    /// and a serving generation keeps answering throughout.
    pub(crate) async fn try_complete_hydration(
        &self,
        canonical: &CanonicalControlHandle,
        source_tip: BlockId,
    ) -> Result<SnapshotCertification, Status> {
        let _mutation_guard = self.mutation_gate.lock().await;
        if !self.status.read().follows_source() || self.staged_generation.lock().is_none() {
            return Err(Status::failed_precondition(
                "mempool source emitted an unexpected snapshot-complete marker",
            ));
        }
        // A staged `Mined` transition for any block but the snapshot's tip
        // means the snapshot no longer describes that tip; certifying it could
        // hide the transaction from both the canonical chain and the mempool.
        let mined_block_off_tip = self
            .staged_generation
            .lock()
            .as_ref()
            .and_then(|staged_generation| staged_generation.mined_block_off_tip(source_tip));
        if let Some(observed_source_tip) = mined_block_off_tip {
            self.begin_next_generation_locked();
            return Ok(SnapshotCertification::GenerationOutran(GenerationOutran {
                generation_source_tip: source_tip,
                observed_source_tip,
            }));
        }
        let chain_epoch = canonical.chain_epoch().await?.chain_epoch;
        let visible_tip =
            BlockId::new(chain_epoch.visible_tip_height, chain_epoch.visible_tip_hash);
        if source_tip != visible_tip {
            return Ok(SnapshotCertification::PendingCanonicalTip);
        }
        let staged_generation = self.staged_generation.lock().take().ok_or_else(|| {
            Status::failed_precondition("mempool source completion marker has no staged snapshot")
        })?;
        // Earlier batches may already have changed the index, which the
        // previous generation then no longer describes.
        self.reconcile_staged_generation_locked(canonical, staged_generation)
            .await
            .inspect_err(|_status| self.mark_rebuild_required())?;
        self.set_status(MempoolOwnerStatus::Serving {
            source_tip,
            snapshot_certified_at: UnixTimestampMillis::now(),
        });
        record_mempool_size_gauge(&self.index);
        Ok(SnapshotCertification::Certified)
    }

    async fn reconcile_staged_generation_locked(
        &self,
        canonical: &CanonicalControlHandle,
        mut staged_generation: StagedMempoolGeneration,
    ) -> Result<(), Status> {
        self.reconcile_removed_entries_locked(canonical, &mut staged_generation)
            .await?;
        self.reconcile_added_entries_locked(canonical, &staged_generation)
            .await?;

        Ok(())
    }

    async fn reconcile_removed_entries_locked(
        &self,
        canonical: &CanonicalControlHandle,
        staged_generation: &mut StagedMempoolGeneration,
    ) -> Result<(), Status> {
        let mut after_transaction_id = None;
        loop {
            let page = self.index.snapshot_page(
                u32::try_from(MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS).map_err(|_| {
                    Status::internal("mempool reconciliation page size exceeds u32")
                })?,
                after_transaction_id,
            );
            let Some(last_entry) = page.entries.last() else {
                return Ok(());
            };
            after_transaction_id = Some(last_entry.transaction_id());
            let mut removals = Vec::with_capacity(page.entries.len());
            for entry in page.entries {
                let transaction_id = entry.transaction_id();
                if staged_generation.index.is_in_mempool(transaction_id) {
                    continue;
                }
                let (terminal_event, observed_at) = staged_generation
                    .terminal_events
                    .remove(&transaction_id)
                    .unwrap_or_else(|| {
                        (
                            MempoolEvent::Invalidated {
                                transaction_id,
                                reason: MempoolEvictionReason::Unknown,
                            },
                            UnixTimestampMillis::now(),
                        )
                    });
                removals.push((terminal_event, observed_at));
            }
            self.append_and_apply_reconciliation_transitions_locked(canonical, removals)
                .await?;
            if page.next_after_transaction_id.is_none() {
                return Ok(());
            }
        }
    }

    async fn reconcile_added_entries_locked(
        &self,
        canonical: &CanonicalControlHandle,
        staged_generation: &StagedMempoolGeneration,
    ) -> Result<(), Status> {
        let mut after_transaction_id = None;
        loop {
            let page = staged_generation.index.snapshot_page(
                u32::try_from(MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS).map_err(|_| {
                    Status::internal("mempool reconciliation page size exceeds u32")
                })?,
                after_transaction_id,
            );
            let Some(last_entry) = page.entries.last() else {
                return Ok(());
            };
            after_transaction_id = Some(last_entry.transaction_id());
            let additions = page
                .entries
                .into_iter()
                .filter(|entry| !self.index.is_in_mempool(entry.transaction_id()))
                .map(|entry| {
                    (
                        MempoolEvent::Added {
                            entry: entry.as_ref().clone(),
                        },
                        entry.first_seen_unix_millis(),
                    )
                })
                .collect();
            self.append_and_apply_reconciliation_transitions_locked(canonical, additions)
                .await?;
            if page.next_after_transaction_id.is_none() {
                return Ok(());
            }
        }
    }

    async fn restore_durable_index(
        &self,
        canonical: &CanonicalControlHandle,
    ) -> Result<(), Status> {
        let _mutation_guard = self.mutation_gate.lock().await;
        self.index.reset();
        *self.staged_generation.lock() = Some(StagedMempoolGeneration::new());
        self.set_status(MempoolOwnerStatus::Hydrating);
        let mut after_cursor = None;
        loop {
            let page = canonical
                .mempool_event_page(
                    after_cursor.clone(),
                    std::num::NonZeroU32::new(MEMPOOL_EVENT_PAGE_SIZE).ok_or_else(|| {
                        Status::internal("mempool event page size must be nonzero")
                    })?,
                )
                .await?;
            if page.is_empty() {
                break;
            }
            for envelope in page {
                after_cursor = Some(envelope.cursor.as_bytes().to_vec());
                let event = envelope.event.clone();
                let preflight = self.index.preflight_event(&event).map_err(|error| {
                    self.mark_rebuild_required();
                    Status::failed_precondition(format!(
                        "durable mempool history cannot rebuild the live index: {error}"
                    ))
                })?;
                if preflight == MempoolIndexPreflight::NoChange {
                    continue;
                }
                let outcome = apply_to_index(&self.index, event, envelope.position())?;
                if outcome != MempoolApplyOutcome::Applied {
                    self.mark_rebuild_required();
                    return Err(Status::failed_precondition(
                        "durable mempool history could not rebuild the live index",
                    ));
                }
            }
        }
        record_mempool_size_gauge(&self.index);
        Ok(())
    }

    async fn withdraw_for_rebuild(&self) {
        let _mutation_guard = self.mutation_gate.lock().await;
        self.staged_generation.lock().take();
        self.mark_rebuild_required();
    }

    #[cfg(test)]
    fn is_serving(&self) -> bool {
        matches!(*self.status.read(), MempoolOwnerStatus::Serving { .. })
    }

    #[cfg(test)]
    fn staged_entry_count(&self) -> usize {
        self.staged_generation
            .lock()
            .as_ref()
            .map_or(0, |generation| generation.index.entry_count())
    }
}

impl Default for LiveMempoolOwner {
    fn default() -> Self {
        Self::new()
    }
}

/// Runs the one source-to-index owner until controlled shutdown.
///
/// A completed source generation is reconciled with the durable/live state
/// under the owner gate. Serving transitions append to the follower-owned
/// primary before applying the matching in-memory overlay; hydrating
/// transitions remain staged until their snapshot-complete marker. A normal
/// tip change keeps the serving generation answering at its own tip while the
/// replacement stages. If the impossible post-append mismatch is detected,
/// the public index is withdrawn and the source stream is reopened from a
/// fresh snapshot.
pub async fn run_live_mempool_owner(
    source: Arc<dyn MempoolSource>,
    canonical: CanonicalControlHandle,
    owner: LiveMempoolOwner,
    mempool_ready_signal: MempoolReadySignal,
    cancel: CancellationToken,
) {
    let halt = tokio::select! {
        () = follow_mempool_source(source, &canonical, &owner, &mempool_ready_signal, &cancel) => {
            return;
        }
        halt = mempool_ready_signal.writer_halted() => halt,
    };
    // Dropping the source loop releases the owner gate; a command it left
    // queued reaches the parked writer, which refuses every mempool command.
    owner.halt(halt).await;
    mempool_ready_signal.withdraw_certification();
    tracing::warn!(
        target: "zinder::ingest",
        event = "mempool_owner_halted",
        "canonical writer parked; live mempool owner stopped following the node mempool"
    );
    // The task stays alive so its supervisor does not mistake the halt for
    // an unexpected exit and replace the typed halt with a restart.
    cancel.cancelled().await;
}

async fn follow_mempool_source(
    source: Arc<dyn MempoolSource>,
    canonical: &CanonicalControlHandle,
    owner: &LiveMempoolOwner,
    mempool_ready_signal: &MempoolReadySignal,
    cancel: &CancellationToken,
) {
    if !restore_durable_mempool_index(owner, canonical, mempool_ready_signal, cancel).await {
        return;
    }
    loop {
        match run_source_generation(&source, canonical, owner, mempool_ready_signal, cancel).await {
            MempoolOwnerLoopOutcome::Shutdown => return,
            MempoolOwnerLoopOutcome::Rehydrate => {}
            MempoolOwnerLoopOutcome::Rebuild { reconnect_backoff } => {
                if !withdraw_and_wait_for_mempool_rebuild(
                    owner,
                    mempool_ready_signal,
                    cancel,
                    reconnect_backoff,
                )
                .await
                {
                    return;
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MempoolOwnerLoopOutcome {
    Shutdown,
    Rehydrate,
    Rebuild { reconnect_backoff: Duration },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Whether a complete source snapshot can become the serving generation.
pub(crate) enum SnapshotCertification {
    /// The source snapshot is complete but canonical ingest has not reached its tip.
    PendingCanonicalTip,
    /// The snapshot was reconciled and can serve against its exact source tip.
    Certified,
    /// A staged transition reported a block other than the snapshot's tip, so
    /// the generation ended without certifying.
    GenerationOutran(GenerationOutran),
}

async fn restore_durable_mempool_index(
    owner: &LiveMempoolOwner,
    canonical: &CanonicalControlHandle,
    mempool_ready_signal: &MempoolReadySignal,
    cancel: &CancellationToken,
) -> bool {
    loop {
        match owner.restore_durable_index(canonical).await {
            Ok(()) => return true,
            Err(status) => {
                mempool_ready_signal.withdraw_certification();
                tracing::warn!(
                    target: "zinder::ingest",
                    event = "mempool_durable_replay_failed",
                    code = ?status.code(),
                    "live mempool owner could not reconstruct its durable live state"
                );
                if wait_or_cancel(cancel, MEMPOOL_RECONNECT_BACKOFF).await {
                    return false;
                }
            }
        }
    }
}

async fn run_source_generation(
    source: &Arc<dyn MempoolSource>,
    canonical: &CanonicalControlHandle,
    owner: &LiveMempoolOwner,
    mempool_ready_signal: &MempoolReadySignal,
    cancel: &CancellationToken,
) -> MempoolOwnerLoopOutcome {
    if !owner.begin_next_generation().await {
        mempool_ready_signal.withdraw_certification();
    }
    if wait_for_canonical_epoch(canonical, cancel).await.is_none() {
        return MempoolOwnerLoopOutcome::Shutdown;
    }
    let event_stream = match source.events().await {
        Ok(event_stream) => {
            tracing::info!(
                target: "zinder::ingest",
                event = "mempool_source_opened",
                "live mempool source stream opened"
            );
            event_stream
        }
        Err(error) => {
            // A serving generation stops following the node mempool here, so
            // it is withdrawn rather than kept certified through the outage.
            record_source_open_failure(&error);
            return MempoolOwnerLoopOutcome::Rebuild {
                reconnect_backoff: MEMPOOL_RECONNECT_BACKOFF,
            };
        }
    };
    consume_source_events(event_stream, canonical, owner, mempool_ready_signal, cancel).await
}

async fn consume_source_events(
    mut event_stream: zinder_source::MempoolSourceEventStream,
    canonical: &CanonicalControlHandle,
    owner: &LiveMempoolOwner,
    mempool_ready_signal: &MempoolReadySignal,
    cancel: &CancellationToken,
) -> MempoolOwnerLoopOutcome {
    let context = SourceGenerationContext {
        canonical,
        owner,
        cancel,
    };
    let mut pending_source_tip = None;
    let mut canonical_tip_poll = tokio::time::interval(MEMPOOL_CANONICAL_TIP_POLL_INTERVAL);
    canonical_tip_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        let source_event = tokio::select! {
            () = cancel.cancelled() => return MempoolOwnerLoopOutcome::Shutdown,
            _ = canonical_tip_poll.tick(), if pending_source_tip.is_some() => {
                if let Some(ready_source_event) = event_stream.next().now_or_never() {
                    let Some(ready_source_event) = ready_source_event else {
                        return rebuild_after_source_closed();
                    };
                    if let Some(outcome) = handle_source_event(
                        ready_source_event,
                        &context,
                        &mut pending_source_tip,
                    ).await {
                        return outcome;
                    }
                    continue;
                }
                let Some(source_tip) = pending_source_tip else {
                    continue;
                };
                match certify_source_snapshot(canonical, owner, mempool_ready_signal, source_tip).await {
                    Ok(SnapshotCertification::PendingCanonicalTip) => continue,
                    Ok(SnapshotCertification::Certified) => {
                        pending_source_tip = None;
                        continue;
                    }
                    Ok(SnapshotCertification::GenerationOutran(outran)) => {
                        return rehydrate_after_outran(outran);
                    }
                    Err(status) => return rejected_snapshot_outcome(&status),
                }
            }
            source_event = event_stream.next() => source_event,
        };
        let Some(source_event) = source_event else {
            return rebuild_after_source_closed();
        };
        if let Some(outcome) =
            handle_source_event(source_event, &context, &mut pending_source_tip).await
        {
            return outcome;
        }
    }
}

struct SourceGenerationContext<'a> {
    canonical: &'a CanonicalControlHandle,
    owner: &'a LiveMempoolOwner,
    cancel: &'a CancellationToken,
}

async fn handle_source_event(
    source_event: Result<MempoolSourceEvent, SourceError>,
    context: &SourceGenerationContext<'_>,
    pending_source_tip: &mut Option<BlockId>,
) -> Option<MempoolOwnerLoopOutcome> {
    match source_event {
        Ok(MempoolSourceEvent::SourceTipChanged {
            generation_source_tip,
            observed_source_tip,
        }) => {
            record_source_tip_change(generation_source_tip, observed_source_tip);
            Some(MempoolOwnerLoopOutcome::Rehydrate)
        }
        Ok(MempoolSourceEvent::InitialSnapshotComplete { source_tip }) => {
            *pending_source_tip = Some(source_tip);
            None
        }
        Ok(source_event) => match apply_source_event(
            context.owner,
            context.canonical,
            context.cancel,
            source_event,
        )
        .await
        {
            Ok(None) => None,
            Ok(Some(outran)) => Some(rehydrate_after_outran(outran)),
            Err(status) => Some(rejected_source_event_outcome(&status)),
        },
        Err(error) => Some(source_item_error_outcome(&error)),
    }
}

async fn certify_source_snapshot(
    canonical: &CanonicalControlHandle,
    owner: &LiveMempoolOwner,
    mempool_ready_signal: &MempoolReadySignal,
    source_tip: BlockId,
) -> Result<SnapshotCertification, Status> {
    let certification = owner.try_complete_hydration(canonical, source_tip).await?;
    match certification {
        SnapshotCertification::PendingCanonicalTip => {
            tracing::debug!(
                target: "zinder::ingest",
                event = "mempool_snapshot_waiting_for_canonical_tip",
                source_tip = ?source_tip,
                "complete mempool snapshot remains private until canonical ingest catches up"
            );
        }
        SnapshotCertification::GenerationOutran(_) => {}
        SnapshotCertification::Certified => {
            mempool_ready_signal.certify_source_tip(source_tip);
            tracing::info!(
                target: "zinder::ingest",
                event = "mempool_snapshot_complete",
                "live mempool snapshot completed; live reads are available"
            );
        }
    }
    Ok(certification)
}

fn tip_transition_read_expired() -> Status {
    status_for_reason(
        ErrorReason::ChainEpochPinUnavailable,
        "live mempool index did not settle on the canonical chain epoch within the read bound",
    )
}

fn rejected_snapshot_outcome(status: &Status) -> MempoolOwnerLoopOutcome {
    tracing::warn!(
        target: "zinder::ingest",
        event = "mempool_snapshot_marker_rejected",
        code = ?status.code(),
        "live mempool source completion marker was rejected"
    );
    MempoolOwnerLoopOutcome::Rebuild {
        reconnect_backoff: MEMPOOL_RECONNECT_BACKOFF,
    }
}

fn rejected_source_event_outcome(status: &Status) -> MempoolOwnerLoopOutcome {
    tracing::warn!(
        target: "zinder::ingest",
        event = "mempool_event_rejected",
        code = ?status.code(),
        "live mempool source event was not published"
    );
    MempoolOwnerLoopOutcome::Rebuild {
        reconnect_backoff: MEMPOOL_RECONNECT_BACKOFF,
    }
}

fn source_item_error_outcome(error: &SourceError) -> MempoolOwnerLoopOutcome {
    metrics::counter!(
        "zinder_mempool_source_errors_total",
        "kind" => "stream_item",
        "failure_class" => error.upstream_classification().label()
    )
    .increment(1);
    tracing::warn!(
        target: "zinder::ingest",
        event = "mempool_source_item_error",
        error = %error,
        "live mempool source emitted an error item"
    );
    MempoolOwnerLoopOutcome::Rebuild {
        reconnect_backoff: mempool_source_reconnect_backoff(error),
    }
}

fn rebuild_after_source_closed() -> MempoolOwnerLoopOutcome {
    tracing::warn!(
        target: "zinder::ingest",
        event = "mempool_source_closed",
        "live mempool source stream closed; reconnecting"
    );
    MempoolOwnerLoopOutcome::Rebuild {
        reconnect_backoff: MEMPOOL_RECONNECT_BACKOFF,
    }
}

/// Records a normal tip boundary. The next source generation stages behind
/// the serving one, which keeps its certification until the replacement is
/// certified.
fn record_source_tip_change(
    generation_source_tip: zinder_core::BlockId,
    observed_source_tip: zinder_core::BlockId,
) {
    metrics::counter!("zinder_mempool_source_tip_changes_total").increment(1);
    tracing::debug!(
        target: "zinder::ingest",
        event = "mempool_source_tip_changed",
        generation_source_tip = ?generation_source_tip,
        observed_source_tip = ?observed_source_tip,
        "mempool source generation ended at a normal chain-tip boundary"
    );
}

fn rehydrate_after_outran(outran: GenerationOutran) -> MempoolOwnerLoopOutcome {
    record_source_tip_change(outran.generation_source_tip, outran.observed_source_tip);
    MempoolOwnerLoopOutcome::Rehydrate
}

async fn withdraw_and_wait_for_mempool_rebuild(
    owner: &LiveMempoolOwner,
    mempool_ready_signal: &MempoolReadySignal,
    cancel: &CancellationToken,
    reconnect_backoff: Duration,
) -> bool {
    mempool_ready_signal.withdraw_certification();
    owner.withdraw_for_rebuild().await;
    tracing::error!(
        target: "zinder::ingest",
        event = "mempool_index_rebuild_required",
        "withdrew live mempool reads until the source reseeds the index"
    );
    !wait_or_cancel(cancel, reconnect_backoff).await
}

fn mempool_source_reconnect_backoff(error: &SourceError) -> Duration {
    default_recovery_backoff().for_class(error.upstream_classification())
}

/// Scheduling and work limits for durable mempool-event retention.
#[derive(Clone, Copy, Debug)]
pub struct MempoolRetentionSettings {
    /// Per-event retention windows.
    pub retention: MempoolEventRetentionConfig,
    /// Event-count and encoded-byte budget for one maintenance step.
    pub budget: MempoolEventRetentionStepBudget,
    /// Delay between complete retention scans.
    pub check_interval: Duration,
}

/// Runs the durable mempool-event retention loop through the canonical writer.
pub async fn run_mempool_retention(
    canonical: CanonicalControlHandle,
    owner: LiveMempoolOwner,
    settings: MempoolRetentionSettings,
    cancel: CancellationToken,
) {
    let mut has_immediate_work = false;
    loop {
        if has_immediate_work {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                () = tokio::task::yield_now() => {}
            }
        } else {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(settings.check_interval) => {}
            }
        }
        if owner.is_halted() {
            // A parked writer refuses mempool pruning; retention resumes only
            // in a new process.
            cancel.cancelled().await;
            return;
        }
        let now = UnixTimestampMillis::now();
        let started_at = Instant::now();
        let retention_outcome = owner
            .prune_events(&canonical, now, settings.retention, settings.budget)
            .await;
        record_mempool_retention_pass(started_at, now, &retention_outcome);
        has_immediate_work = retention_outcome
            .as_ref()
            .is_ok_and(|outcome| outcome.has_immediate_work());
        if let Err(status) = retention_outcome {
            tracing::warn!(
                target: "zinder::ingest",
                event = "mempool_event_retention_failed",
                code = ?status.code(),
                "durable mempool-event retention step failed"
            );
        }
    }
}

async fn apply_source_event(
    owner: &LiveMempoolOwner,
    canonical: &CanonicalControlHandle,
    cancel: &CancellationToken,
    source_event: MempoolSourceEvent,
) -> Result<Option<GenerationOutran>, Status> {
    let observed_at = match &source_event {
        MempoolSourceEvent::Added(entry) => entry.observed_at_unix_millis,
        MempoolSourceEvent::Invalidated { .. } | MempoolSourceEvent::Mined { .. } => {
            UnixTimestampMillis::now()
        }
        MempoolSourceEvent::InitialSnapshotComplete { .. } | _ => UnixTimestampMillis::now(),
    };
    let event = match source_event {
        MempoolSourceEvent::Added(source_entry) => {
            let chain_epoch = wait_for_canonical_epoch(canonical, cancel)
                .await
                .ok_or_else(|| Status::cancelled("mempool owner cancelled"))?;
            build_mempool_entry(source_entry, chain_epoch)
                .map(|entry| MempoolEvent::Added { entry })
                .map_err(|error| {
                    record_hydration_failure(hydration_failure_reason(&error));
                    Status::failed_precondition("mempool transaction hydration failed")
                })?
        }
        MempoolSourceEvent::Invalidated {
            transaction_id,
            reason,
        } => MempoolEvent::Invalidated {
            transaction_id,
            reason,
        },
        MempoolSourceEvent::Mined {
            transaction_id,
            mined_height,
            block_hash,
        } => MempoolEvent::Mined {
            transaction_id,
            mined_height,
            block_hash,
        },
        MempoolSourceEvent::InitialSnapshotComplete { .. } | _ => {
            record_hydration_failure(MempoolHydrationFailureReason::UnknownSourceEventVariant);
            return Err(Status::failed_precondition(
                "mempool source event variant is unsupported",
            ));
        }
    };
    owner.apply_event(canonical, event, observed_at).await
}

async fn wait_for_canonical_epoch(
    canonical: &CanonicalControlHandle,
    cancel: &CancellationToken,
) -> Option<ChainEpoch> {
    loop {
        let snapshot = tokio::select! {
            () = cancel.cancelled() => return None,
            snapshot = canonical.chain_epoch() => snapshot,
        };
        match snapshot {
            Ok(snapshot) => return Some(snapshot.chain_epoch),
            Err(status) => {
                tracing::debug!(
                    target: "zinder::ingest",
                    event = "mempool_waiting_for_canonical_writer",
                    code = ?status.code(),
                    "mempool owner is waiting for the canonical writer command channel"
                );
                if wait_or_cancel(cancel, MEMPOOL_RECONNECT_BACKOFF).await {
                    return None;
                }
            }
        }
    }
}

async fn wait_or_cancel(cancel: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        () = cancel.cancelled() => true,
        () = tokio::time::sleep(duration) => false,
    }
}

#[allow(
    unreachable_patterns,
    reason = "MempoolEvent is non-exhaustive; the index must fail closed rather than inventing a live transition."
)]
fn apply_to_index(
    index: &MempoolIndex,
    event: MempoolEvent,
    position: zinder_store::MempoolEventPosition,
) -> Result<MempoolApplyOutcome, Status> {
    match event {
        MempoolEvent::Added { entry } => Ok(index.apply_added(entry, position)),
        MempoolEvent::Invalidated { transaction_id, .. } => {
            Ok(index.apply_invalidated(transaction_id, position))
        }
        MempoolEvent::Mined { transaction_id, .. } => {
            Ok(index.apply_mined(transaction_id, position))
        }
        _ => Err(Status::failed_precondition(
            "mempool event variant is unsupported",
        )),
    }
}

fn hydration_failure_reason(error: &MempoolEntryBuildError) -> MempoolHydrationFailureReason {
    match error {
        MempoolEntryBuildError::TransactionParseFailed { .. } => {
            MempoolHydrationFailureReason::TransactionParseFailed
        }
        MempoolEntryBuildError::TransactionIdMismatch { .. } => {
            MempoolHydrationFailureReason::TransactionIdMismatch
        }
        MempoolEntryBuildError::AuthDigestMismatch { .. } => {
            MempoolHydrationFailureReason::AuthDigestMismatch
        }
        MempoolEntryBuildError::CompactTransactionBuildFailed { .. } => {
            MempoolHydrationFailureReason::CompactTransactionBuildFailed
        }
        MempoolEntryBuildError::TransparentOutputIndexOverflow => {
            MempoolHydrationFailureReason::TransparentOutputIndexOverflow
        }
    }
}

fn record_hydration_failure(reason: MempoolHydrationFailureReason) {
    metrics::counter!(
        "zinder_mempool_hydration_failures_total",
        "reason" => reason.as_label()
    )
    .increment(1);
}

fn record_source_open_failure(error: &SourceError) {
    metrics::counter!(
        "zinder_mempool_source_errors_total",
        "kind" => "stream_open",
        "failure_class" => error.upstream_classification().label()
    )
    .increment(1);
    tracing::warn!(
        target: "zinder::ingest",
        event = "mempool_source_open_failed",
        error = %error,
        "live mempool source could not open; reconnecting"
    );
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus gauges accept f64 samples; live mempool size is diagnostic."
)]
fn record_mempool_size_gauge(index: &MempoolIndex) {
    metrics::gauge!("zinder_mempool_entries").set(index.entry_count() as f64);
}

fn record_mempool_lifecycle_status(status: MempoolOwnerStatus) {
    let active_status = match status {
        MempoolOwnerStatus::Hydrating => "hydrating",
        MempoolOwnerStatus::Serving { .. } => "serving",
        MempoolOwnerStatus::RebuildRequired => "rebuild_required",
        MempoolOwnerStatus::Halted(_) => "halted",
    };
    for status_label in MEMPOOL_LIFECYCLE_STATUS_LABELS {
        let sample = f64::from(u8::from(*status_label == active_status));
        metrics::gauge!(
            "zinder_mempool_lifecycle_state",
            "status" => *status_label
        )
        .set(sample);
    }
}

fn record_mempool_retention_pass(
    started_at: Instant,
    now: UnixTimestampMillis,
    retention_outcome: &Result<MempoolEventRetentionStepOutcome, Status>,
) {
    let status = retention_outcome
        .as_ref()
        .map_or("error", |outcome| retention_step_stop_label(outcome.stop));
    metrics::histogram!(
        "zinder_mempool_event_pruning_duration_seconds",
        "status" => status
    )
    .record(started_at.elapsed());

    let Ok(outcome) = retention_outcome else {
        return;
    };
    let report = &outcome.report;
    metrics::histogram!("zinder_mempool_event_retention_examined_events")
        .record(f64::from(outcome.examined_event_count));
    metrics::histogram!("zinder_mempool_event_retention_examined_encoded_bytes")
        .record(u64_to_f64(outcome.examined_encoded_bytes));
    for (kind, pruned_count) in [
        ("added", report.pruned_added_count),
        ("mined", report.pruned_mined_count),
        ("invalidated", report.pruned_invalidated_count),
    ] {
        metrics::counter!("zinder_mempool_events_pruned_total", "kind" => kind)
            .increment(pruned_count);
    }
    metrics::gauge!("zinder_mempool_events_retained").set(u64_to_f64(report.retained_event_count));
    metrics::gauge!("zinder_mempool_event_retention_oldest_sequence")
        .set(u64_to_f64(report.oldest_retained_sequence.unwrap_or(0)));
    let oldest_retained_age_seconds = report.oldest_retained_observed_at.map_or(0, |observed_at| {
        now.value().saturating_sub(observed_at.value()) / 1_000
    });
    metrics::gauge!("zinder_mempool_event_retention_oldest_age_seconds")
        .set(u64_to_f64(oldest_retained_age_seconds));
}

const fn retention_step_stop_label(stop: MempoolEventRetentionStepStop) -> &'static str {
    match stop {
        MempoolEventRetentionStepStop::BudgetExhausted => "budget_exhausted",
        MempoolEventRetentionStepStop::ReachedHead => "reached_head",
        MempoolEventRetentionStepStop::ReachedUnexpiredEvent => "reached_unexpired_event",
        MempoolEventRetentionStepStop::RetentionDisabled => "retention_disabled",
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus gauges accept f64 samples; retention counters are diagnostic."
)]
fn u64_to_f64(sample: u64) -> f64 {
    sample as f64
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        num::{NonZeroU32, NonZeroU64},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU32, Ordering},
        },
        time::{Duration, Instant},
    };

    use metrics_exporter_prometheus::PrometheusBuilder;
    use parking_lot::Mutex as ParkingMutex;
    use tokio_util::sync::CancellationToken;
    use tonic::Code;
    use tonic_types::StatusExt as _;
    use zebra_chain::{
        serialization::ZcashDeserializeInto as _, transaction::Transaction as ZebraTransaction,
    };
    use zinder_core::{
        AuthDigest, ChainEpoch, CompactTransactionData, MempoolEntry, MempoolEvictionReason,
        MempoolObservation, RawTransactionBytes, TransactionId, UnixTimestampMillis,
    };
    use zinder_proto::v1::ops::ErrorReason;
    use zinder_source::{MempoolSource as _, MempoolSourceEntry};
    use zinder_store::{
        MempoolEvent, MempoolEventRetentionConfig, MempoolEventRetentionStepBudget,
        RocksDbCanonicalStore,
    };
    use zinder_testkit::{MockMempoolSource, MockMempoolSourceControl};

    use crate::{
        MempoolReadyGate, MempoolReadySignal, mempool_ready_channel,
        writer::control::{
            CanonicalControlCommand, CanonicalControlHandle, CanonicalWriterSnapshot,
            ParkedWriterHalt, apply_canonical_control_command, canonical_control_channel,
            test_support::published_fixture_store,
        },
    };

    use super::{
        LiveMempoolOwner, MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS,
        MEMPOOL_TIP_TRANSITION_READ_WAIT, MempoolOwnerLoopOutcome, MempoolOwnerStatus,
        MempoolRetentionSettings, SnapshotCertification, consume_source_events,
        record_mempool_lifecycle_status, record_mempool_retention_pass, run_live_mempool_owner,
        run_mempool_retention,
    };

    #[test]
    fn lifecycle_and_retention_metrics_publish_the_documented_contract() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let now = UnixTimestampMillis::now();
        let retention_report = zinder_store::MempoolEventRetentionReport {
            current_event_sequence: 12,
            oldest_retained_sequence: Some(7),
            oldest_retained_observed_at: Some(UnixTimestampMillis::new(
                now.value().saturating_sub(5_000),
            )),
            retained_event_count: 6,
            pruned_added_count: 1,
            pruned_mined_count: 2,
            pruned_invalidated_count: 3,
        };

        metrics::with_local_recorder(&recorder, || {
            record_mempool_lifecycle_status(MempoolOwnerStatus::Serving {
                source_tip: fixture_source_tip(),
                snapshot_certified_at: now,
            });
            record_mempool_retention_pass(
                Instant::now(),
                now,
                &Ok(zinder_store::MempoolEventRetentionStepOutcome {
                    report: retention_report,
                    examined_event_count: 4,
                    examined_encoded_bytes: 4_096,
                    stop: zinder_store::MempoolEventRetentionStepStop::ReachedHead,
                }),
            );
        });

        let exposition = handle.render();
        for expected_sample in [
            "zinder_mempool_lifecycle_state{status=\"hydrating\"} 0",
            "zinder_mempool_lifecycle_state{status=\"serving\"} 1",
            "zinder_mempool_lifecycle_state{status=\"rebuild_required\"} 0",
            "zinder_mempool_lifecycle_state{status=\"halted\"} 0",
            "zinder_mempool_events_pruned_total{kind=\"added\"} 1",
            "zinder_mempool_events_pruned_total{kind=\"mined\"} 2",
            "zinder_mempool_events_pruned_total{kind=\"invalidated\"} 3",
            "zinder_mempool_events_retained 6",
            "zinder_mempool_event_retention_oldest_sequence 7",
            "zinder_mempool_event_retention_oldest_age_seconds 5",
            "zinder_mempool_event_pruning_duration_seconds_count{status=\"reached_head\"} 1",
            "zinder_mempool_event_retention_examined_events_count 1",
            "zinder_mempool_event_retention_examined_encoded_bytes_count 1",
        ] {
            assert!(
                exposition.contains(expected_sample),
                "missing `{expected_sample}` in metrics exposition:\n{exposition}"
            );
        }
    }

    #[tokio::test]
    async fn cancellation_stops_an_immediate_retention_backlog_between_steps()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        for transaction_tag in 1_u8..=3 {
            let _envelope = store.append_mempool_event(
                MempoolEvent::Invalidated {
                    transaction_id: TransactionId::from_bytes([transaction_tag; 32]),
                    reason: MempoolEvictionReason::Unknown,
                },
                UnixTimestampMillis::new(1_000),
            )?;
        }
        let (canonical, mut commands) = canonical_control_channel();
        let cancel = CancellationToken::new();
        let command_cancel = cancel.clone();
        let command_task = tokio::spawn(async move {
            let mut command_count = 0_u32;
            while let Some(command) = commands.recv().await {
                command_count = command_count.saturating_add(1);
                apply_canonical_control_command(&mut store, command);
                if command_count == 1 {
                    command_cancel.cancel();
                }
            }
            command_count
        });

        let retention_task = tokio::spawn(run_mempool_retention(
            canonical,
            LiveMempoolOwner::default(),
            MempoolRetentionSettings {
                retention: MempoolEventRetentionConfig::new(
                    Some(Duration::from_millis(1)),
                    Some(Duration::from_millis(1)),
                ),
                budget: MempoolEventRetentionStepBudget::new(
                    NonZeroU32::new(1).ok_or("retention event budget must be nonzero")?,
                    NonZeroU64::new(1_000_000).ok_or("retention byte budget must be nonzero")?,
                ),
                check_interval: Duration::ZERO,
            },
            cancel,
        ));

        tokio::time::timeout(Duration::from_secs(1), retention_task).await??;
        let command_count = tokio::time::timeout(Duration::from_secs(1), command_task).await??;
        assert_eq!(command_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_marker_waits_when_the_canonical_tip_differs() -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                apply_canonical_control_command(&mut store, command);
            }
        });
        let owner = LiveMempoolOwner::default();
        let mismatched_source_tip = zinder_core::BlockId::new(
            zinder_core::BlockHeight::new(1),
            zinder_core::BlockHash::from_bytes([2; 32]),
        );

        let outcome = owner
            .try_complete_hydration(&canonical, mismatched_source_tip)
            .await?;

        assert_eq!(outcome, SnapshotCertification::PendingCanonicalTip);
        assert!(matches!(
            *owner.status.read(),
            MempoolOwnerStatus::Hydrating
        ));
        assert!(!owner.is_serving());
        assert!(
            canonical
                .mempool_event_page(
                    None,
                    NonZeroU32::new(8).ok_or("mempool page size must be nonzero")?,
                )
                .await?
                .is_empty()
        );
        command_task.abort();
        let _ = command_task.await;
        Ok(())
    }

    #[tokio::test]
    async fn hydrating_point_read_fails_before_waiting_for_the_mutation_gate()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                apply_canonical_control_command(&mut store, command);
            }
        });
        let chain_epoch = canonical.chain_epoch().await?.chain_epoch;
        let owner = LiveMempoolOwner::default();
        let _stalled_reconciliation = owner.mutation_gate.lock().await;

        let outcome = tokio::time::timeout(
            Duration::from_millis(25),
            owner.entry_for(chain_epoch, TransactionId::from_bytes([0xAA; 32])),
        )
        .await?;

        assert_eq!(
            outcome.err().map(|status| status.code()),
            Some(Code::Unavailable)
        );
        command_task.abort();
        let _ = command_task.await;
        Ok(())
    }

    #[tokio::test]
    async fn point_read_reports_an_expired_chain_epoch_when_the_certified_tip_has_rotated()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                apply_canonical_control_command(&mut store, command);
            }
        });
        let owner = LiveMempoolOwner::default();
        assert_eq!(
            owner
                .try_complete_hydration(&canonical, fixture_source_tip())
                .await?,
            SnapshotCertification::Certified
        );
        let mut newer_chain_epoch = canonical.chain_epoch().await?.chain_epoch;
        newer_chain_epoch.visible_tip_hash = zinder_core::BlockHash::from_bytes([2; 32]);

        let outcome = owner
            .entry_for(newer_chain_epoch, TransactionId::from_bytes([0xA5; 32]))
            .await;
        let status = outcome.err().ok_or("point read unexpectedly succeeded")?;
        let error_info = status
            .get_error_details()
            .error_info()
            .cloned()
            .ok_or("chain epoch failure omitted ErrorInfo")?;

        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(error_info.domain, "zinder.dev");
        assert_eq!(
            error_info.reason,
            ErrorReason::ChainEpochPinUnavailable.as_str_name()
        );
        command_task.abort();
        let _ = command_task.await;
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_age_uses_source_generation_certification_even_when_empty()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                apply_canonical_control_command(&mut store, command);
            }
        });
        let chain_epoch = canonical.chain_epoch().await?.chain_epoch;
        let owner = LiveMempoolOwner::default();
        owner.set_status(MempoolOwnerStatus::Serving {
            source_tip: fixture_source_tip(),
            snapshot_certified_at: UnixTimestampMillis::new(
                UnixTimestampMillis::now().value().saturating_sub(5_000),
            ),
        });

        let snapshot = owner
            .snapshot_page(&canonical, chain_epoch, 8, Vec::new())
            .await?;

        assert!(snapshot.entries.is_empty());
        assert!(snapshot.snapshot_age_millis >= 5_000);
        command_task.abort();
        let _ = command_task.await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn source_snapshot_marker_hides_partial_hydration_and_reconnect_gap()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                apply_canonical_control_command(&mut store, command);
            }
        });
        let (source, source_control) = MockMempoolSource::streaming();
        let owner = LiveMempoolOwner::default();
        let (ready_signal, ready_gate) = mempool_ready_channel();
        let cancel = CancellationToken::new();
        let owner_task = tokio::spawn(run_live_mempool_owner(
            Arc::new(source),
            canonical.clone(),
            owner.clone(),
            ready_signal,
            cancel.clone(),
        ));

        assert_partial_snapshot_is_private(&source_control, &owner, &ready_gate, &canonical)
            .await?;
        let first_generation_cursor =
            complete_initial_generation(&source_control, &owner, &ready_gate, &canonical).await?;
        reconcile_empty_snapshot(
            &source_control,
            &owner,
            &ready_gate,
            &canonical,
            first_generation_cursor.as_bytes(),
        )
        .await?;
        assert_abandoned_generation_is_not_durable(
            &source_control,
            &owner,
            &ready_gate,
            &canonical,
        )
        .await?;

        cancel.cancel();
        owner_task.await?;
        command_task.abort();
        let _ = command_task.await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "the scenario follows one serving generation through consecutive tip changes"
    )]
    async fn source_tip_change_stages_a_replacement_behind_the_serving_generation()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let MutableEpochCanonicalControl {
            canonical,
            canonical_epoch,
            command_task,
        } = mutable_epoch_canonical_control(store)?;
        let (source, source_control) = MockMempoolSource::streaming();
        let owner = LiveMempoolOwner::default();
        let (ready_signal, ready_gate) = mempool_ready_channel();
        let cancel = CancellationToken::new();
        let owner_task = tokio::spawn(run_live_mempool_owner(
            Arc::new(source),
            canonical.clone(),
            owner.clone(),
            ready_signal,
            cancel.clone(),
        ));
        let generation_source_tip = fixture_source_tip();
        let observed_source_tip = fixture_tip(2);
        let next_source_tip = fixture_tip(3);
        let final_source_tip = fixture_tip(4);

        wait_for_source_open(&source_control, 1).await?;
        source_control.complete_initial_snapshot(generation_source_tip)?;
        wait_for_serving(&owner, true).await?;
        source_control.change_source_tip(generation_source_tip, observed_source_tip)?;
        tokio::time::timeout(
            Duration::from_millis(250),
            wait_for_source_open(&source_control, 2),
        )
        .await??;
        assert_eq!(
            ready_gate.certified_source_tip(),
            Some(generation_source_tip),
            "a normal tip change keeps the serving generation certified"
        );
        assert_serving_at(&owner, generation_source_tip);

        source_control.complete_initial_snapshot(observed_source_tip)?;
        tokio::time::sleep(Duration::from_millis(75)).await;
        assert_serving_at(&owner, generation_source_tip);
        assert_eq!(
            ready_gate.certified_source_tip(),
            Some(generation_source_tip)
        );
        assert_eq!(source_control.open_count(), 2);

        set_canonical_visible_tip(&canonical_epoch, observed_source_tip);
        tokio::time::timeout(
            Duration::from_millis(250),
            wait_until(|| ready_gate.certified_source_tip() == Some(observed_source_tip)),
        )
        .await??;
        assert_serving_at(&owner, observed_source_tip);
        assert_eq!(source_control.open_count(), 2);

        source_control.change_source_tip(observed_source_tip, next_source_tip)?;
        tokio::time::timeout(
            Duration::from_millis(250),
            wait_for_source_open(&source_control, 3),
        )
        .await??;
        source_control.push_added(source_entry(0xB2)?)?;
        wait_for_staged_entries(&owner, 1).await?;
        source_control.change_source_tip(next_source_tip, final_source_tip)?;
        tokio::time::timeout(
            Duration::from_millis(250),
            wait_for_source_open(&source_control, 4),
        )
        .await??;
        assert_eq!(owner.staged_entry_count(), 0);
        assert!(
            canonical
                .mempool_event_page(
                    None,
                    NonZeroU32::new(8).ok_or("mempool page size must be nonzero")?,
                )
                .await?
                .is_empty(),
            "tip-change control markers and abandoned staging must not become durable events"
        );

        cancel.cancel();
        owner_task.await?;
        command_task.abort();
        let _join_result = command_task.await;
        Ok(())
    }

    /// Block arrival during steady-state following never withdraws the live
    /// index.
    ///
    /// The serving generation answers at its own tip, and a read at the new
    /// canonical tip waits for the replacement generation instead of failing.
    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_read_during_a_tip_change_waits_for_the_replacement_generation()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let MutableEpochCanonicalControl {
            canonical,
            canonical_epoch,
            command_task,
        } = mutable_epoch_canonical_control(store)?;
        let (source, source_control) = MockMempoolSource::streaming();
        let owner = LiveMempoolOwner::default();
        let (ready_signal, ready_gate) = mempool_ready_channel();
        let cancel = CancellationToken::new();
        let owner_task = tokio::spawn(run_live_mempool_owner(
            Arc::new(source),
            canonical.clone(),
            owner.clone(),
            ready_signal,
            cancel.clone(),
        ));
        let serving_tip = fixture_tip(2);
        let next_tip = fixture_tip(3);
        set_canonical_visible_tip(&canonical_epoch, serving_tip);
        wait_for_source_open(&source_control, 1).await?;
        source_control.push_added(source_entry(0xD1)?)?;
        source_control.complete_initial_snapshot(serving_tip)?;
        wait_for_serving(&owner, true).await?;

        // The node reports the block first: the replacement generation stages
        // while canonical ingest still shows the serving tip.
        source_control.change_source_tip(serving_tip, next_tip)?;
        wait_for_source_open(&source_control, 2).await?;
        let (chain_epoch, page) = owner
            .snapshot_page_at_canonical_tip(&canonical, 8, Vec::new())
            .await?;
        assert_eq!(visible_tip(chain_epoch), serving_tip);
        assert_eq!(page.source_tip, serving_tip);
        assert_eq!(page.entries.len(), 1);

        // Canonical ingest commits the block before the replacement certifies.
        set_canonical_visible_tip(&canonical_epoch, next_tip);
        let read_owner = owner.clone();
        let read_canonical = canonical.clone();
        let read_at_next_tip = tokio::spawn(async move {
            read_owner
                .snapshot_page_at_canonical_tip(&read_canonical, 8, Vec::new())
                .await
        });
        tokio::time::sleep(Duration::from_millis(75)).await;
        assert!(
            !read_at_next_tip.is_finished(),
            "a read at the new tip waits for the replacement generation"
        );
        assert!(owner.require_serving().is_ok());
        assert_eq!(ready_gate.certified_source_tip(), Some(serving_tip));
        assert!(
            ready_gate.admits_canonical_tip(next_tip),
            "block arrival must not withdraw ingest readiness"
        );

        source_control.push_added(source_entry(0xD1)?)?;
        source_control.push_added(source_entry(0xD2)?)?;
        source_control.complete_initial_snapshot(next_tip)?;
        let (chain_epoch, page) =
            tokio::time::timeout(Duration::from_secs(2), read_at_next_tip).await???;
        assert_eq!(visible_tip(chain_epoch), next_tip);
        assert_eq!(page.source_tip, next_tip);
        assert_eq!(page.entries.len(), 2);
        assert_eq!(ready_gate.certified_source_tip(), Some(next_tip));
        assert_eq!(
            durable_event_count(&canonical).await?,
            2,
            "only the replacement's new entry becomes durable"
        );

        cancel.cancel();
        owner_task.await?;
        command_task.abort();
        let _join_result = command_task.await;
        Ok(())
    }

    /// A read refused for a lasting reason fails at once instead of waiting
    /// out the tip-transition bound.
    #[tokio::test]
    async fn read_during_a_rebuild_fails_without_waiting() -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                apply_canonical_control_command(&mut store, command);
            }
        });
        let owner = LiveMempoolOwner::default();
        owner.withdraw_for_rebuild().await;

        let outcome = tokio::time::timeout(
            Duration::from_millis(500),
            owner.entry_at_canonical_tip(&canonical, TransactionId::from_bytes([0xD3; 32])),
        )
        .await?;

        assert_eq!(
            outcome.err().map(|status| status.code()),
            Some(Code::Unavailable)
        );
        command_task.abort();
        let _join_result = command_task.await;
        Ok(())
    }

    /// The tip-transition bound also covers the wait for the mutation gate,
    /// so a read queued behind a long reconciliation still answers in time.
    #[tokio::test]
    async fn read_behind_a_held_mutation_gate_expires_at_the_transition_bound()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                apply_canonical_control_command(&mut store, command);
            }
        });
        let owner = LiveMempoolOwner::default();
        assert_eq!(
            owner
                .try_complete_hydration(&canonical, fixture_source_tip())
                .await?,
            SnapshotCertification::Certified
        );
        let _stalled_reconciliation = owner.mutation_gate.lock().await;

        let outcome = tokio::time::timeout(
            MEMPOOL_TIP_TRANSITION_READ_WAIT + Duration::from_secs(2),
            owner.entry_at_canonical_tip(&canonical, TransactionId::from_bytes([0xE2; 32])),
        )
        .await?;
        let status = outcome
            .err()
            .ok_or("read behind a held mutation gate unexpectedly succeeded")?;
        let error_info = status
            .get_error_details()
            .error_info()
            .cloned()
            .ok_or("tip-transition expiry omitted ErrorInfo")?;

        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(
            error_info.reason,
            ErrorReason::ChainEpochPinUnavailable.as_str_name()
        );
        command_task.abort();
        let _join_result = command_task.await;
        Ok(())
    }

    /// A certification that lands after a read was refused retries the read
    /// instead of returning the stale refusal.
    #[tokio::test]
    async fn read_retries_when_certification_lands_after_its_refusal() -> Result<(), Box<dyn Error>>
    {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                apply_canonical_control_command(&mut store, command);
            }
        });
        let owner = LiveMempoolOwner::default();
        let attempts = AtomicU32::new(0);
        let owner_ref = &owner;
        let attempts_ref = &attempts;

        let (chain_epoch, answered_tip) = tokio::time::timeout(
            Duration::from_secs(2),
            owner.read_at_canonical_tip(&canonical, move |chain_epoch| {
                let attempt = attempts_ref.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        owner_ref.set_status(MempoolOwnerStatus::Serving {
                            source_tip: visible_tip(chain_epoch),
                            snapshot_certified_at: UnixTimestampMillis::now(),
                        });
                        return Err(tonic::Status::unavailable("refused before certification"));
                    }
                    owner_ref
                        .coherent_serving_state_for(chain_epoch)
                        .map(|(source_tip, _snapshot_certified_at)| source_tip)
                }
            }),
        )
        .await??;

        assert_eq!(answered_tip, visible_tip(chain_epoch));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        command_task.abort();
        let _join_result = command_task.await;
        Ok(())
    }

    /// A replacement reconciliation that fails after earlier batches changed
    /// the index withdraws it before a read queued at the old epoch runs, so
    /// that read cannot observe a partially reconciled index.
    #[tokio::test(flavor = "multi_thread")]
    async fn failed_replacement_reconciliation_withdraws_before_a_queued_read()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let transaction_count = MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS * 2;
        // Two addition batches certify the first generation; the replacement's
        // second removal batch fails.
        let StalledAppendCanonicalControl {
            canonical,
            canonical_epoch,
            append_reached,
            release_append,
            command_task,
        } = stalled_append_canonical_control(store, 4)?;
        let (source, source_control) = MockMempoolSource::streaming();
        let owner = LiveMempoolOwner::default();
        let (ready_signal, _ready_gate) = mempool_ready_channel();
        let cancel = CancellationToken::new();
        let serving_tip = fixture_tip(2);
        let next_tip = fixture_tip(3);
        set_canonical_visible_tip(&canonical_epoch, serving_tip);
        let serving_epoch = canonical.chain_epoch().await?.chain_epoch;
        let owner_task = tokio::spawn(run_live_mempool_owner(
            Arc::new(source),
            canonical.clone(),
            owner.clone(),
            ready_signal,
            cancel.clone(),
        ));
        wait_for_source_open(&source_control, 1).await?;
        for transaction_nonce in 0..transaction_count {
            source_control
                .push_added(source_entry_with_nonce(u32::try_from(transaction_nonce)?)?)?;
        }
        wait_for_staged_entries(&owner, transaction_count).await?;
        source_control.complete_initial_snapshot(serving_tip)?;
        wait_for_serving(&owner, true).await?;

        source_control.change_source_tip(serving_tip, next_tip)?;
        wait_for_source_open(&source_control, 2).await?;
        source_control.complete_initial_snapshot(next_tip)?;
        set_canonical_visible_tip(&canonical_epoch, next_tip);
        tokio::time::timeout(Duration::from_secs(3), append_reached).await??;
        let read_owner = owner.clone();
        let queued_read = tokio::spawn(async move {
            read_owner
                .entry_for(serving_epoch, TransactionId::from_bytes([0xE1; 32]))
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !queued_read.is_finished(),
            "the read queues behind the reconciliation"
        );
        release_append
            .send(())
            .map_err(|()| "the stalled append was already released")?;

        let outcome = tokio::time::timeout(Duration::from_secs(2), queued_read).await??;
        assert_eq!(
            outcome.err().map(|status| status.code()),
            Some(Code::Unavailable)
        );

        cancel.cancel();
        owner_task.await?;
        command_task.abort();
        let _join_result = command_task.await;
        Ok(())
    }

    /// A source that cannot be reopened after a tip change withdraws the
    /// serving generation, which would otherwise stay certified while it no
    /// longer follows the node mempool.
    #[tokio::test(flavor = "multi_thread")]
    async fn failed_source_reopen_after_a_tip_change_withdraws_the_serving_generation()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let MutableEpochCanonicalControl {
            canonical,
            canonical_epoch,
            command_task,
        } = mutable_epoch_canonical_control(store)?;
        let (mock_source, source_control) = MockMempoolSource::streaming();
        let fail_opens = Arc::new(AtomicBool::new(false));
        let source = ReopenFailingMempoolSource {
            inner: mock_source,
            fail_opens: Arc::clone(&fail_opens),
        };
        let owner = LiveMempoolOwner::default();
        let (ready_signal, ready_gate) = mempool_ready_channel();
        let cancel = CancellationToken::new();
        let serving_tip = fixture_tip(2);
        set_canonical_visible_tip(&canonical_epoch, serving_tip);
        let owner_task = tokio::spawn(run_live_mempool_owner(
            Arc::new(source),
            canonical.clone(),
            owner.clone(),
            ready_signal,
            cancel.clone(),
        ));
        wait_for_source_open(&source_control, 1).await?;
        source_control.complete_initial_snapshot(serving_tip)?;
        wait_for_hydration(&ready_gate, true).await?;

        fail_opens.store(true, Ordering::SeqCst);
        source_control.change_source_tip(serving_tip, fixture_tip(3))?;
        wait_for_serving(&owner, false).await?;
        wait_for_hydration(&ready_gate, false).await?;
        assert!(owner.require_serving().is_err());

        cancel.cancel();
        owner_task.await?;
        command_task.abort();
        let _join_result = command_task.await;
        Ok(())
    }

    /// A park halts a serving owner for the rest of the process: it stops
    /// following the node mempool, refuses reads with the typed halt, records
    /// no further events, and keeps its task alive until shutdown.
    #[tokio::test(flavor = "multi_thread")]
    async fn writer_park_halts_the_owner_until_shutdown() -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let MutableEpochCanonicalControl {
            canonical,
            canonical_epoch: _,
            command_task,
        } = mutable_epoch_canonical_control(store)?;
        let (source, source_control) = MockMempoolSource::streaming();
        let owner = LiveMempoolOwner::default();
        let (ready_signal, ready_gate) = mempool_ready_channel();
        let cancel = CancellationToken::new();
        let owner_task = tokio::spawn(run_live_mempool_owner(
            Arc::new(source),
            canonical.clone(),
            owner.clone(),
            ready_signal,
            cancel.clone(),
        ));
        wait_for_source_open(&source_control, 1).await?;
        source_control.complete_initial_snapshot(fixture_source_tip())?;
        wait_for_serving(&owner, true).await?;

        let halt = ParkedWriterHalt::new(zinder_proto::v1::ops::ReorgWindowExceededDetail {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        });
        ready_gate.halt_owner(halt);
        wait_until(|| owner.is_halted()).await?;
        assert_eq!(ready_gate.certified_source_tip(), None);
        let durable_events = durable_event_count(&canonical).await?;

        let Err(status) = owner.require_serving() else {
            return Err("a halted owner must refuse reads".into());
        };
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(
            status
                .get_error_details()
                .error_info()
                .map(|error_info| error_info.reason.clone()),
            Some(ErrorReason::ReorgWindowExceeded.as_str_name().to_owned())
        );
        // The source is no longer consumed, so a later observation neither
        // reopens it nor becomes durable.
        let _ignored_after_halt = source_control.push_added(source_entry(0xB7)?);
        tokio::time::sleep(Duration::from_millis(75)).await;
        assert_eq!(source_control.open_count(), 1);
        assert_eq!(durable_event_count(&canonical).await?, durable_events);
        assert!(owner.is_halted());
        assert!(
            !owner_task.is_finished(),
            "a halted owner must stay alive so its supervisor keeps the typed halt"
        );

        cancel.cancel();
        owner_task.await?;
        command_task.abort();
        let _join_result = command_task.await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pending_snapshot_certifies_during_sustained_source_traffic()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let MutableEpochCanonicalControl {
            canonical,
            canonical_epoch,
            command_task,
        } = mutable_epoch_canonical_control(store)?;
        let (source, source_control) = MockMempoolSource::streaming();
        let event_stream = source.events().await?;
        let owner = LiveMempoolOwner::default();
        let owner_for_task = owner.clone();
        let (ready_signal, ready_gate) = mempool_ready_channel();
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let canonical_for_task = canonical.clone();
        let owner_task = tokio::spawn(async move {
            consume_source_events(
                event_stream,
                &canonical_for_task,
                &owner_for_task,
                &ready_signal,
                &task_cancel,
            )
            .await
        });
        let source_tip = fixture_tip(2);
        let repeated_entry = source_entry(0xB5)?;

        source_control.complete_initial_snapshot(source_tip)?;
        source_control.push_added(repeated_entry.clone())?;
        wait_for_staged_entries(&owner, 1).await?;
        set_canonical_visible_tip(&canonical_epoch, source_tip);
        let traffic_control = source_control.clone();
        let traffic_task = tokio::spawn(async move {
            for _ in 0..100 {
                traffic_control.push_added(repeated_entry.clone())?;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Ok::<(), zinder_testkit::MockMempoolSourceClosed>(())
        });

        tokio::time::timeout(Duration::from_millis(150), wait_for_serving(&owner, true)).await??;
        assert_eq!(ready_gate.certified_source_tip(), Some(source_tip));
        traffic_task.await??;
        cancel.cancel();
        assert_eq!(owner_task.await?, MempoolOwnerLoopOutcome::Shutdown);
        command_task.abort();
        let _join_result = command_task.await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn queued_tip_change_precedes_pending_snapshot_certification()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let MutableEpochCanonicalControl {
            canonical,
            canonical_epoch,
            command_task,
        } = mutable_epoch_canonical_control(store)?;
        let (source, source_control) = MockMempoolSource::streaming();
        let event_stream = source.events().await?;
        let owner = LiveMempoolOwner::default();
        let (ready_signal, ready_gate) = mempool_ready_channel();
        let cancel = CancellationToken::new();
        let source_tip = fixture_tip(2);
        let next_tip = fixture_tip(3);
        set_canonical_visible_tip(&canonical_epoch, source_tip);
        source_control.complete_initial_snapshot(source_tip)?;
        source_control.change_source_tip(source_tip, next_tip)?;

        let outcome = tokio::time::timeout(
            Duration::from_millis(150),
            consume_source_events(event_stream, &canonical, &owner, &ready_signal, &cancel),
        )
        .await?;

        assert_eq!(outcome, MempoolOwnerLoopOutcome::Rehydrate);
        assert!(!owner.is_serving());
        assert_eq!(ready_gate.certified_source_tip(), None);
        command_task.abort();
        let _join_result = command_task.await;
        Ok(())
    }

    /// A `Mined` change for any block but the serving tip ends the source
    /// generation without applying the removal.
    ///
    /// Zebra's mempool changes and tip notifications have no shared order, so
    /// a `Mined` change for the next block can reach a serving generation
    /// before its tip change. A competing block at the tip height, or a block
    /// below the tip after a reorg to a branch no higher than the tip, can
    /// likewise be missing from the tip's canonical chain. Removing the
    /// transaction then would leave it in neither the canonical chain at the
    /// generation tip nor the mempool.
    #[tokio::test(flavor = "multi_thread")]
    async fn mined_transition_off_the_serving_tip_ends_the_generation() -> Result<(), Box<dyn Error>>
    {
        let source_tip = fixture_tip(2);
        for off_tip in off_tip_blocks() {
            let mut harness = ServingOwnerHarness::start(source_tip).await?;
            let transaction_id = harness.serve_one_entry(0xC7).await?;

            harness
                .source_control
                .push_mined(transaction_id, off_tip.height, off_tip.hash)?;
            let outcome = harness.finish().await?;

            assert_eq!(outcome, MempoolOwnerLoopOutcome::Rehydrate);
            assert_serving_at(&harness.owner, source_tip);
            assert_eq!(harness.ready_gate.certified_source_tip(), Some(source_tip));
            let chain_epoch = harness.chain_epoch().await?;
            assert!(
                harness
                    .owner
                    .entry_for(chain_epoch, transaction_id)
                    .await?
                    .is_some(),
                "a generation outrun by {off_tip:?} keeps the entry at its own tip"
            );
            assert_eq!(
                durable_event_count(&harness.canonical).await?,
                1,
                "the outrunning Mined change must not become durable history"
            );
            harness.shutdown().await;
        }
        Ok(())
    }

    /// A staged `Mined` change for any block but the snapshot tip ends the
    /// generation instead of certifying it.
    #[tokio::test(flavor = "multi_thread")]
    async fn mined_transition_off_the_snapshot_tip_blocks_certification()
    -> Result<(), Box<dyn Error>> {
        let source_tip = fixture_tip(2);
        for off_tip in off_tip_blocks() {
            let temporary = tempfile::TempDir::new()?;
            let store = published_fixture_store(&temporary.path().join("canonical"))?;
            let MutableEpochCanonicalControl {
                canonical,
                canonical_epoch,
                command_task,
            } = mutable_epoch_canonical_control(store)?;
            let (source, source_control) = MockMempoolSource::streaming();
            let event_stream = source.events().await?;
            let owner = LiveMempoolOwner::default();
            let (ready_signal, ready_gate) = mempool_ready_channel();
            let cancel = CancellationToken::new();
            set_canonical_visible_tip(&canonical_epoch, source_tip);
            let entry = source_entry(0xC9)?;
            let transaction_id = entry.transaction_id;
            source_control.push_added(entry)?;
            source_control.push_mined(transaction_id, off_tip.height, off_tip.hash)?;
            source_control.complete_initial_snapshot(source_tip)?;

            let outcome = tokio::time::timeout(
                Duration::from_millis(500),
                consume_source_events(event_stream, &canonical, &owner, &ready_signal, &cancel),
            )
            .await?;

            assert_eq!(outcome, MempoolOwnerLoopOutcome::Rehydrate, "{off_tip:?}");
            assert!(!owner.is_serving(), "{off_tip:?}");
            assert_eq!(ready_gate.certified_source_tip(), None, "{off_tip:?}");
            assert_eq!(durable_event_count(&canonical).await?, 0, "{off_tip:?}");
            command_task.abort();
            let _join_result = command_task.await;
        }
        Ok(())
    }

    /// Blocks a `Mined` change can report that are not `fixture_tip(2)`: the
    /// next block, a competing block at the tip height, and a block below it.
    fn off_tip_blocks() -> [zinder_core::BlockId; 3] {
        let other_hash = zinder_core::BlockHash::from_bytes([0xEE; 32]);
        [3, 2, 1].map(|height| {
            zinder_core::BlockId::new(zinder_core::BlockHeight::new(height), other_hash)
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mined_transition_at_the_serving_tip_removes_the_entry() -> Result<(), Box<dyn Error>> {
        let source_tip = fixture_tip(2);
        let mut harness = ServingOwnerHarness::start(source_tip).await?;
        let transaction_id = harness.serve_one_entry(0xC8).await?;

        harness
            .source_control
            .push_mined(transaction_id, source_tip.height, source_tip.hash)?;
        let owner = harness.owner.clone();
        wait_until(|| !owner.index.is_in_mempool(transaction_id)).await?;

        assert!(harness.owner.is_serving());
        let chain_epoch = harness.chain_epoch().await?;
        assert!(
            harness
                .owner
                .entry_for(chain_epoch, transaction_id)
                .await?
                .is_none()
        );
        assert_eq!(
            harness.finish_by_cancel().await?,
            MempoolOwnerLoopOutcome::Shutdown
        );
        harness.shutdown().await;
        Ok(())
    }

    /// Owner loop serving one certified generation over a mock source.
    struct ServingOwnerHarness {
        _temporary: tempfile::TempDir,
        canonical: CanonicalControlHandle,
        command_task: tokio::task::JoinHandle<()>,
        source_control: MockMempoolSourceControl,
        owner: LiveMempoolOwner,
        ready_gate: MempoolReadyGate,
        /// Keeps the gate open after the owner loop returns, so a test can
        /// read the certification the loop left behind.
        _ready_signal: MempoolReadySignal,
        cancel: CancellationToken,
        owner_task: Option<tokio::task::JoinHandle<MempoolOwnerLoopOutcome>>,
    }

    impl ServingOwnerHarness {
        async fn start(source_tip: zinder_core::BlockId) -> Result<Self, Box<dyn Error>> {
            let temporary = tempfile::TempDir::new()?;
            let store = published_fixture_store(&temporary.path().join("canonical"))?;
            let MutableEpochCanonicalControl {
                canonical,
                canonical_epoch,
                command_task,
            } = mutable_epoch_canonical_control(store)?;
            set_canonical_visible_tip(&canonical_epoch, source_tip);
            let (source, source_control) = MockMempoolSource::streaming();
            let event_stream = source.events().await?;
            let owner = LiveMempoolOwner::default();
            let (ready_signal, ready_gate) = mempool_ready_channel();
            let retained_ready_signal = ready_signal.clone();
            let cancel = CancellationToken::new();
            let task_canonical = canonical.clone();
            let task_owner = owner.clone();
            let task_cancel = cancel.clone();
            let owner_task = tokio::spawn(async move {
                consume_source_events(
                    event_stream,
                    &task_canonical,
                    &task_owner,
                    &ready_signal,
                    &task_cancel,
                )
                .await
            });
            source_control.complete_initial_snapshot(source_tip)?;
            wait_for_serving(&owner, true).await?;
            Ok(Self {
                _temporary: temporary,
                canonical,
                command_task,
                source_control,
                owner,
                ready_gate,
                _ready_signal: retained_ready_signal,
                cancel,
                owner_task: Some(owner_task),
            })
        }

        async fn serve_one_entry(
            &self,
            transaction_tag: u8,
        ) -> Result<TransactionId, Box<dyn Error>> {
            let entry = source_entry(transaction_tag)?;
            let transaction_id = entry.transaction_id;
            self.source_control.push_added(entry)?;
            let owner = self.owner.clone();
            wait_until(|| owner.index.is_in_mempool(transaction_id)).await?;
            let chain_epoch = self.chain_epoch().await?;
            assert!(
                self.owner
                    .entry_for(chain_epoch, transaction_id)
                    .await?
                    .is_some()
            );
            Ok(transaction_id)
        }

        async fn chain_epoch(&self) -> Result<ChainEpoch, Box<dyn Error>> {
            Ok(self.canonical.chain_epoch().await?.chain_epoch)
        }

        async fn finish(&mut self) -> Result<MempoolOwnerLoopOutcome, Box<dyn Error>> {
            let owner_task = self
                .owner_task
                .take()
                .ok_or("owner loop already finished")?;
            Ok(tokio::time::timeout(Duration::from_millis(500), owner_task).await??)
        }

        async fn finish_by_cancel(&mut self) -> Result<MempoolOwnerLoopOutcome, Box<dyn Error>> {
            self.cancel.cancel();
            self.finish().await
        }

        async fn shutdown(self) {
            self.cancel.cancel();
            if let Some(owner_task) = self.owner_task {
                let _join_result = owner_task.await;
            }
            self.command_task.abort();
            let _join_result = self.command_task.await;
        }
    }

    async fn assert_partial_snapshot_is_private(
        source_control: &MockMempoolSourceControl,
        owner: &LiveMempoolOwner,
        ready_gate: &MempoolReadyGate,
        canonical: &CanonicalControlHandle,
    ) -> Result<(), Box<dyn Error>> {
        wait_for_source_open(source_control, 1).await?;
        assert_eq!(
            owner.require_serving().err().map(|status| status.code()),
            Some(Code::Unavailable)
        );
        assert_eq!(ready_gate.certified_source_tip(), None);

        source_control.push_added(source_entry(0xA1)?)?;
        wait_for_staged_entries(owner, 1).await?;
        assert_eq!(owner.index.entry_count(), 0);
        assert_eq!(
            owner.require_serving().err().map(|status| status.code()),
            Some(Code::Unavailable)
        );
        assert_eq!(ready_gate.certified_source_tip(), None);
        assert!(
            canonical
                .mempool_event_page(
                    None,
                    NonZeroU32::new(8).ok_or("mempool page size must be nonzero")?,
                )
                .await?
                .is_empty(),
            "partial source snapshots must not reach durable history"
        );

        source_control.push_added(source_entry(0xA2)?)?;
        wait_for_staged_entries(owner, 2).await?;
        assert_eq!(
            owner.require_serving().err().map(|status| status.code()),
            Some(Code::Unavailable)
        );
        Ok(())
    }

    async fn complete_initial_generation(
        source_control: &MockMempoolSourceControl,
        owner: &LiveMempoolOwner,
        ready_gate: &MempoolReadyGate,
        canonical: &CanonicalControlHandle,
    ) -> Result<zinder_store::StreamCursorTokenV1, Box<dyn Error>> {
        source_control.complete_initial_snapshot(fixture_source_tip())?;
        wait_for_serving(owner, true).await?;
        wait_for_hydration(ready_gate, true).await?;
        assert_eq!(owner.index.entry_count(), 2);
        let history = canonical
            .mempool_event_page(
                None,
                NonZeroU32::new(8).ok_or("mempool page size must be nonzero")?,
            )
            .await?;
        assert_eq!(
            history.len(),
            2,
            "the source completion marker is not durable history"
        );
        assert!(
            history
                .iter()
                .all(|envelope| matches!(envelope.event, MempoolEvent::Added { .. }))
        );
        Ok(history
            .last()
            .ok_or("first completed source generation omitted its durable cursor")?
            .cursor
            .clone())
    }

    async fn reconcile_empty_snapshot(
        source_control: &MockMempoolSourceControl,
        owner: &LiveMempoolOwner,
        ready_gate: &MempoolReadyGate,
        canonical: &CanonicalControlHandle,
        first_generation_cursor: &[u8],
    ) -> Result<(), Box<dyn Error>> {
        source_control.close_stream();
        wait_for_serving(owner, false).await?;
        wait_for_hydration(ready_gate, false).await?;
        wait_for_source_open(source_control, 2).await?;

        // The old index stays private until an empty replacement snapshot's
        // marker makes its terminal events durable.
        assert_eq!(owner.index.entry_count(), 2);
        source_control.complete_initial_snapshot(fixture_source_tip())?;
        wait_for_serving(owner, true).await?;
        wait_for_hydration(ready_gate, true).await?;
        assert_eq!(owner.index.entry_count(), 0);
        let terminal_history = canonical
            .mempool_event_page(
                Some(first_generation_cursor.to_vec()),
                NonZeroU32::new(8).ok_or("mempool page size must be nonzero")?,
            )
            .await?;
        assert_eq!(terminal_history.len(), 2);
        assert!(
            terminal_history
                .iter()
                .all(|envelope| matches!(envelope.event, MempoolEvent::Invalidated { .. }))
        );
        Ok(())
    }

    async fn assert_abandoned_generation_is_not_durable(
        source_control: &MockMempoolSourceControl,
        owner: &LiveMempoolOwner,
        ready_gate: &MempoolReadyGate,
        canonical: &CanonicalControlHandle,
    ) -> Result<(), Box<dyn Error>> {
        source_control.close_stream();
        wait_for_serving(owner, false).await?;
        wait_for_hydration(ready_gate, false).await?;
        wait_for_source_open(source_control, 3).await?;
        let abandoned_entry = source_entry(0xB1)?;
        let abandoned_transaction_id = abandoned_entry.transaction_id;
        source_control.push_added(abandoned_entry)?;
        wait_for_staged_entries(owner, 1).await?;
        let before_abandoned_generation = canonical
            .mempool_event_page(
                None,
                NonZeroU32::new(8).ok_or("mempool page size must be nonzero")?,
            )
            .await?;
        assert_eq!(before_abandoned_generation.len(), 4);

        source_control.close_stream();
        wait_for_source_open(source_control, 4).await?;
        source_control.complete_initial_snapshot(fixture_source_tip())?;
        wait_for_serving(owner, true).await?;
        wait_for_hydration(ready_gate, true).await?;
        assert_eq!(owner.index.entry_count(), 0);
        let after_abandoned_generation = canonical
            .mempool_event_page(
                None,
                NonZeroU32::new(8).ok_or("mempool page size must be nonzero")?,
            )
            .await?;
        assert_eq!(after_abandoned_generation.len(), 4);
        assert!(
            after_abandoned_generation
                .iter()
                .all(|envelope| envelope.transaction_id() != abandoned_transaction_id)
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn owner_restart_reconciles_retained_live_history_against_empty_snapshot()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                apply_canonical_control_command(&mut store, command);
            }
        });

        let (first_source, first_source_control) = MockMempoolSource::streaming();
        let first_owner = LiveMempoolOwner::default();
        let (first_ready_signal, first_ready_gate) = mempool_ready_channel();
        let first_cancel = CancellationToken::new();
        let first_owner_task = tokio::spawn(run_live_mempool_owner(
            Arc::new(first_source),
            canonical.clone(),
            first_owner.clone(),
            first_ready_signal,
            first_cancel.clone(),
        ));
        wait_for_source_open(&first_source_control, 1).await?;
        first_source_control.push_added(source_entry(0xC1)?)?;
        wait_for_staged_entries(&first_owner, 1).await?;
        first_source_control.complete_initial_snapshot(fixture_source_tip())?;
        wait_for_serving(&first_owner, true).await?;
        wait_for_hydration(&first_ready_gate, true).await?;
        let first_history = canonical
            .mempool_event_page(
                None,
                NonZeroU32::new(8).ok_or("mempool page size must be nonzero")?,
            )
            .await?;
        assert_eq!(first_history.len(), 1);
        let restart_cursor = first_history[0].cursor.clone();

        first_cancel.cancel();
        first_owner_task.await?;
        drop(first_owner);

        let (second_source, second_source_control) = MockMempoolSource::streaming();
        let second_owner = LiveMempoolOwner::default();
        let (second_ready_signal, second_ready_gate) = mempool_ready_channel();
        let second_cancel = CancellationToken::new();
        let second_owner_task = tokio::spawn(run_live_mempool_owner(
            Arc::new(second_source),
            canonical.clone(),
            second_owner.clone(),
            second_ready_signal,
            second_cancel.clone(),
        ));
        wait_for_source_open(&second_source_control, 1).await?;
        assert_eq!(second_owner.index.entry_count(), 1);
        assert_eq!(
            second_owner
                .require_serving()
                .err()
                .map(|status| status.code()),
            Some(Code::Unavailable),
            "durable replay must stay private until the replacement snapshot completes"
        );
        second_source_control.complete_initial_snapshot(fixture_source_tip())?;
        wait_for_serving(&second_owner, true).await?;
        wait_for_hydration(&second_ready_gate, true).await?;
        assert_eq!(second_owner.index.entry_count(), 0);
        let terminal_history = canonical
            .mempool_event_page(
                Some(restart_cursor.as_bytes().to_vec()),
                NonZeroU32::new(8).ok_or("mempool page size must be nonzero")?,
            )
            .await?;
        assert_eq!(terminal_history.len(), 1);
        assert!(matches!(
            terminal_history[0].event,
            MempoolEvent::Invalidated { .. }
        ));

        second_cancel.cancel();
        second_owner_task.await?;
        command_task.abort();
        let _ = command_task.await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn activation_scale_empty_replacement_reconciles_in_bounded_batches()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let (batch_size_sender, mut batch_size_receiver) = tokio::sync::mpsc::unbounded_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                if let CanonicalControlCommand::AppendMempoolEvents { events, .. } = &command {
                    let _send_outcome = batch_size_sender.send(events.len());
                }
                apply_canonical_control_command(&mut store, command);
            }
        });
        let (source, source_control) = MockMempoolSource::streaming();
        let owner = LiveMempoolOwner::default();
        let (ready_signal, ready_gate) = mempool_ready_channel();
        let cancel = CancellationToken::new();
        let owner_task = tokio::spawn(run_live_mempool_owner(
            Arc::new(source),
            canonical.clone(),
            owner.clone(),
            ready_signal,
            cancel.clone(),
        ));
        let transaction_count = MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS * 4;
        wait_for_source_open(&source_control, 1).await?;
        for transaction_nonce in 0..transaction_count {
            source_control
                .push_added(source_entry_with_nonce(u32::try_from(transaction_nonce)?)?)?;
        }
        wait_for_staged_entries(&owner, transaction_count).await?;
        source_control.complete_initial_snapshot(fixture_source_tip())?;
        wait_for_hydration(&ready_gate, true).await?;
        assert_eq!(owner.index.entry_count(), transaction_count);

        source_control.close_stream();
        wait_for_source_open(&source_control, 2).await?;
        source_control.complete_initial_snapshot(fixture_source_tip())?;
        wait_for_hydration(&ready_gate, true).await?;

        assert_eq!(owner.index.entry_count(), 0);
        let expected_event_count = transaction_count.saturating_mul(2);
        assert_eq!(durable_event_count(&canonical).await?, expected_event_count);
        let reconciliation_batch_sizes =
            std::iter::from_fn(|| batch_size_receiver.try_recv().ok()).collect::<Vec<_>>();
        assert!(reconciliation_batch_sizes.len() > 2);
        assert!(
            reconciliation_batch_sizes
                .iter()
                .all(|batch_size| *batch_size <= MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS)
        );
        assert_eq!(
            reconciliation_batch_sizes.iter().sum::<usize>(),
            expected_event_count
        );
        cancel.cancel();
        owner_task.await?;
        command_task.abort();
        let _ = command_task.await;
        Ok(())
    }

    #[tokio::test]
    async fn reconciliation_transitions_close_at_the_raw_transaction_byte_target()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let (batch_sender, mut batch_receiver) = tokio::sync::mpsc::unbounded_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                if let CanonicalControlCommand::AppendMempoolEvents { events, .. } = &command {
                    let raw_transaction_bytes = events
                        .iter()
                        .map(|(event, _observed_at)| match event {
                            MempoolEvent::Added { entry } => entry.raw_transaction_bytes().len(),
                            MempoolEvent::Invalidated { .. } | MempoolEvent::Mined { .. } | _ => 0,
                        })
                        .sum::<usize>();
                    let _send_outcome = batch_sender.send((events.len(), raw_transaction_bytes));
                }
                apply_canonical_control_command(&mut store, command);
            }
        });
        let chain_epoch = canonical.chain_epoch().await?.chain_epoch;
        let raw_transaction_byte_limit =
            NonZeroU64::new(10).ok_or("raw transaction byte limit must be nonzero")?;
        let owner = LiveMempoolOwner::with_reconciliation_batch_target_raw_transaction_bytes(
            raw_transaction_byte_limit,
        );
        let transitions = [4_usize, 6, 7, 3]
            .into_iter()
            .enumerate()
            .map(|(transaction_index, raw_transaction_bytes)| {
                added_transition(
                    u8::try_from(transaction_index)?,
                    raw_transaction_bytes,
                    chain_epoch,
                )
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;

        owner
            .append_and_apply_reconciliation_transitions_locked(&canonical, transitions)
            .await?;
        let terminal_transitions = (0_u8..4)
            .map(|transaction_tag| {
                (
                    MempoolEvent::Invalidated {
                        transaction_id: TransactionId::from_bytes([transaction_tag; 32]),
                        reason: MempoolEvictionReason::Unknown,
                    },
                    UnixTimestampMillis::new(1_750_000_000_001),
                )
            })
            .collect();
        owner
            .append_and_apply_reconciliation_transitions_locked(&canonical, terminal_transitions)
            .await?;

        assert_eq!(
            std::iter::from_fn(|| batch_receiver.try_recv().ok()).collect::<Vec<_>>(),
            vec![(2, 10), (2, 10), (4, 0)]
        );
        command_task.abort();
        let _ = command_task.await;
        Ok(())
    }

    #[tokio::test]
    async fn reconciliation_allows_an_oversized_addition_only_as_a_singleton_batch()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let (batch_sender, mut batch_receiver) = tokio::sync::mpsc::unbounded_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                if let CanonicalControlCommand::AppendMempoolEvents { events, .. } = &command {
                    let raw_transaction_bytes = events
                        .iter()
                        .map(|(event, _observed_at)| match event {
                            MempoolEvent::Added { entry } => entry.raw_transaction_bytes().len(),
                            MempoolEvent::Invalidated { .. } | MempoolEvent::Mined { .. } | _ => 0,
                        })
                        .sum::<usize>();
                    let _send_outcome = batch_sender.send((events.len(), raw_transaction_bytes));
                }
                apply_canonical_control_command(&mut store, command);
            }
        });
        let chain_epoch = canonical.chain_epoch().await?.chain_epoch;
        let raw_transaction_byte_limit =
            NonZeroU64::new(5).ok_or("raw transaction byte limit must be nonzero")?;
        let owner = LiveMempoolOwner::with_reconciliation_batch_target_raw_transaction_bytes(
            raw_transaction_byte_limit,
        );
        let transitions = vec![
            added_transition(0, 8, chain_epoch)?,
            added_transition(1, 2, chain_epoch)?,
        ];

        owner
            .append_and_apply_reconciliation_transitions_locked(&canonical, transitions)
            .await?;

        assert_eq!(
            std::iter::from_fn(|| batch_receiver.try_recv().ok()).collect::<Vec<_>>(),
            vec![(1, 8), (1, 2)]
        );
        command_task.abort();
        let _ = command_task.await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multi_page_mixed_replacement_applies_only_the_symmetric_difference()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                apply_canonical_control_command(&mut store, command);
            }
        });
        let (source, source_control) = MockMempoolSource::streaming();
        let owner = LiveMempoolOwner::default();
        let (ready_signal, ready_gate) = mempool_ready_channel();
        let cancel = CancellationToken::new();
        let owner_task = tokio::spawn(run_live_mempool_owner(
            Arc::new(source),
            canonical.clone(),
            owner.clone(),
            ready_signal,
            cancel.clone(),
        ));
        let initial_entry_count = MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS * 2;
        wait_for_source_open(&source_control, 1).await?;
        for transaction_nonce in 0..initial_entry_count {
            source_control
                .push_added(source_entry_with_nonce(u32::try_from(transaction_nonce)?)?)?;
        }
        wait_for_staged_entries(&owner, initial_entry_count).await?;
        source_control.complete_initial_snapshot(fixture_source_tip())?;
        wait_for_hydration(&ready_gate, true).await?;

        let shared_start = MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS / 2;
        let shared_end = shared_start + MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS;
        let added_end = initial_entry_count + MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS;
        source_control.close_stream();
        wait_for_source_open(&source_control, 2).await?;
        for transaction_nonce in shared_start..shared_end {
            source_control
                .push_added(source_entry_with_nonce(u32::try_from(transaction_nonce)?)?)?;
        }
        for transaction_nonce in initial_entry_count..added_end {
            source_control
                .push_added(source_entry_with_nonce(u32::try_from(transaction_nonce)?)?)?;
        }
        wait_for_staged_entries(&owner, initial_entry_count).await?;
        source_control.complete_initial_snapshot(fixture_source_tip())?;
        wait_for_hydration(&ready_gate, true).await?;

        assert_eq!(owner.index.entry_count(), initial_entry_count);
        for transaction_nonce in 0..added_end {
            let transaction_id =
                source_entry_with_nonce(u32::try_from(transaction_nonce)?)?.transaction_id;
            let should_remain = (shared_start..shared_end).contains(&transaction_nonce)
                || transaction_nonce >= initial_entry_count;
            assert_eq!(owner.index.is_in_mempool(transaction_id), should_remain);
        }
        assert_eq!(
            durable_event_count(&canonical).await?,
            initial_entry_count + MAX_MEMPOOL_RECONCILIATION_BATCH_EVENTS * 2
        );
        cancel.cancel();
        owner_task.await?;
        command_task.abort();
        let _ = command_task.await;
        Ok(())
    }

    fn source_entry(transaction_tag: u8) -> Result<MempoolSourceEntry, Box<dyn Error>> {
        source_entry_with_nonce(u32::from(transaction_tag))
    }

    fn source_entry_with_nonce(
        transaction_nonce: u32,
    ) -> Result<MempoolSourceEntry, Box<dyn Error>> {
        let raw_transaction_bytes = synthetic_v4_tx_bytes(transaction_nonce);
        let transaction: ZebraTransaction =
            raw_transaction_bytes.as_slice().zcash_deserialize_into()?;
        Ok(MempoolSourceEntry {
            transaction_id: TransactionId::from_bytes(transaction.hash().0),
            auth_digest: transaction
                .auth_digest()
                .map(|digest| AuthDigest::from_bytes(digest.0)),
            raw_transaction_bytes: RawTransactionBytes::new(raw_transaction_bytes),
            observed_at_unix_millis: UnixTimestampMillis::new(1_750_000_000_000),
        })
    }

    fn added_transition(
        transaction_tag: u8,
        raw_transaction_bytes: usize,
        chain_epoch: ChainEpoch,
    ) -> Result<(MempoolEvent, UnixTimestampMillis), Box<dyn Error>> {
        let transaction_id = TransactionId::from_bytes([transaction_tag; 32]);
        let observed_at = UnixTimestampMillis::new(1_750_000_000_000);
        let entry = MempoolEntry::new(
            transaction_id,
            None,
            RawTransactionBytes::new(vec![transaction_tag; raw_transaction_bytes]),
            CompactTransactionData::default(),
            MempoolObservation {
                first_seen_unix_millis: observed_at,
                first_seen_chain_epoch: chain_epoch,
            },
        )?;
        Ok((MempoolEvent::Added { entry }, observed_at))
    }

    fn fixture_source_tip() -> zinder_core::BlockId {
        fixture_tip(1)
    }

    fn fixture_tip(height: u8) -> zinder_core::BlockId {
        zinder_core::BlockId::new(
            zinder_core::BlockHeight::new(u32::from(height)),
            zinder_core::BlockHash::from_bytes([height; 32]),
        )
    }

    struct MutableEpochCanonicalControl {
        canonical: CanonicalControlHandle,
        canonical_epoch: Arc<ParkingMutex<ChainEpoch>>,
        command_task: tokio::task::JoinHandle<()>,
    }

    fn set_canonical_visible_tip(
        canonical_epoch: &ParkingMutex<ChainEpoch>,
        visible_tip: zinder_core::BlockId,
    ) {
        let mut epoch = canonical_epoch.lock();
        epoch.visible_tip_height = visible_tip.height;
        epoch.visible_tip_hash = visible_tip.hash;
    }

    fn mutable_epoch_canonical_control(
        mut store: RocksDbCanonicalStore,
    ) -> Result<MutableEpochCanonicalControl, Box<dyn Error>> {
        let canonical_epoch = Arc::new(ParkingMutex::new(store.chain_epoch()?));
        let command_epoch = canonical_epoch.clone();
        let canonical_fence = store.event_fence();
        let (canonical, mut commands) = canonical_control_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                #[allow(
                    clippy::wildcard_enum_match_arm,
                    reason = "the test overrides only epoch reads and delegates the full control contract"
                )]
                match command {
                    CanonicalControlCommand::ChainEpoch { reply } => {
                        let _send_outcome = reply.send(Ok(CanonicalWriterSnapshot {
                            chain_epoch: *command_epoch.lock(),
                            fence: canonical_fence,
                        }));
                    }
                    command => {
                        apply_canonical_control_command(&mut store, command);
                    }
                }
            }
        });
        Ok(MutableEpochCanonicalControl {
            canonical,
            canonical_epoch,
            command_task,
        })
    }

    /// Delegates to a mock source until `fail_opens` is set, then refuses
    /// every reopen.
    #[derive(Debug)]
    struct ReopenFailingMempoolSource {
        inner: MockMempoolSource,
        fail_opens: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl zinder_source::MempoolSource for ReopenFailingMempoolSource {
        fn capabilities(&self) -> zinder_source::MempoolSourceCapabilities {
            self.inner.capabilities()
        }

        async fn events(
            &self,
        ) -> Result<zinder_source::MempoolSourceEventStream, zinder_source::SourceError> {
            if self.fail_opens.load(Ordering::SeqCst) {
                return Err(zinder_source::SourceError::NodeUnavailable {
                    reason: "injected mempool source reopen failure".to_owned(),
                });
            }
            self.inner.events().await
        }
    }

    struct StalledAppendCanonicalControl {
        canonical: CanonicalControlHandle,
        canonical_epoch: Arc<ParkingMutex<ChainEpoch>>,
        append_reached: tokio::sync::oneshot::Receiver<()>,
        release_append: tokio::sync::oneshot::Sender<()>,
        command_task: tokio::task::JoinHandle<()>,
    }

    /// A mutable-epoch canonical control whose `failing_append`-th mempool
    /// append reports that it was reached, waits for release, and then fails.
    fn stalled_append_canonical_control(
        mut store: RocksDbCanonicalStore,
        failing_append: usize,
    ) -> Result<StalledAppendCanonicalControl, Box<dyn Error>> {
        let canonical_epoch = Arc::new(ParkingMutex::new(store.chain_epoch()?));
        let command_epoch = canonical_epoch.clone();
        let canonical_fence = store.event_fence();
        let (canonical, mut commands) = canonical_control_channel();
        let (reached_sender, append_reached) = tokio::sync::oneshot::channel();
        let (release_append, release_receiver) = tokio::sync::oneshot::channel::<()>();
        let command_task = tokio::spawn(async move {
            let mut reached_sender = Some(reached_sender);
            let mut release_receiver = Some(release_receiver);
            let mut append_count = 0_usize;
            while let Some(command) = commands.recv().await {
                #[allow(
                    clippy::wildcard_enum_match_arm,
                    reason = "the test overrides epoch reads and mempool appends and delegates the full control contract"
                )]
                match command {
                    CanonicalControlCommand::ChainEpoch { reply } => {
                        let _send_outcome = reply.send(Ok(CanonicalWriterSnapshot {
                            chain_epoch: *command_epoch.lock(),
                            fence: canonical_fence,
                        }));
                    }
                    CanonicalControlCommand::AppendMempoolEvents { events, reply } => {
                        append_count = append_count.saturating_add(1);
                        if append_count != failing_append {
                            apply_canonical_control_command(
                                &mut store,
                                CanonicalControlCommand::AppendMempoolEvents { events, reply },
                            );
                            continue;
                        }
                        if let Some(sender) = reached_sender.take() {
                            let _send_outcome = sender.send(());
                        }
                        if let Some(receiver) = release_receiver.take() {
                            let _release_outcome = receiver.await;
                        }
                        let _send_outcome = reply.send(Err(tonic::Status::unavailable(
                            "injected durable mempool append failure",
                        )));
                    }
                    command => {
                        apply_canonical_control_command(&mut store, command);
                    }
                }
            }
        });
        Ok(StalledAppendCanonicalControl {
            canonical,
            canonical_epoch,
            append_reached,
            release_append,
            command_task,
        })
    }

    fn synthetic_v4_tx_bytes(transaction_nonce: u32) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0x8000_0004_u32.to_le_bytes());
        bytes.extend_from_slice(&0x892F_2085_u32.to_le_bytes());
        bytes.push(0);
        bytes.push(0);
        bytes.extend_from_slice(&transaction_nonce.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&0_i64.to_le_bytes());
        bytes.push(0);
        bytes.push(0);
        bytes.push(0);
        bytes
    }

    async fn durable_event_count(
        canonical: &CanonicalControlHandle,
    ) -> Result<usize, Box<dyn Error>> {
        let page_size = NonZeroU32::new(64).ok_or("mempool page size must be nonzero")?;
        let mut after_cursor = None;
        let mut event_count = 0_usize;
        loop {
            let page = canonical
                .mempool_event_page(after_cursor, page_size)
                .await?;
            let Some(last) = page.last() else {
                return Ok(event_count);
            };
            event_count = event_count.saturating_add(page.len());
            after_cursor = Some(last.cursor.as_bytes().to_vec());
        }
    }

    async fn wait_for_source_open(
        control: &MockMempoolSourceControl,
        expected_open_count: u32,
    ) -> Result<(), Box<dyn Error>> {
        wait_until(|| control.open_count() >= expected_open_count).await
    }

    async fn wait_for_staged_entries(
        owner: &LiveMempoolOwner,
        expected_entry_count: usize,
    ) -> Result<(), Box<dyn Error>> {
        wait_until(|| owner.staged_entry_count() == expected_entry_count).await
    }

    async fn wait_for_serving(
        owner: &LiveMempoolOwner,
        expected: bool,
    ) -> Result<(), Box<dyn Error>> {
        wait_until(|| owner.is_serving() == expected).await
    }

    fn visible_tip(chain_epoch: ChainEpoch) -> zinder_core::BlockId {
        zinder_core::BlockId::new(chain_epoch.visible_tip_height, chain_epoch.visible_tip_hash)
    }

    fn assert_serving_at(owner: &LiveMempoolOwner, expected_source_tip: zinder_core::BlockId) {
        let status = *owner.status.read();
        assert!(
            matches!(
                status,
                MempoolOwnerStatus::Serving { source_tip, .. } if source_tip == expected_source_tip
            ),
            "expected the owner to serve at {expected_source_tip:?}, found {status:?}"
        );
    }

    async fn wait_for_hydration(
        ready_gate: &super::super::MempoolReadyGate,
        expected: bool,
    ) -> Result<(), Box<dyn Error>> {
        wait_until(|| ready_gate.certified_source_tip().is_some() == expected).await
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) -> Result<(), Box<dyn Error>> {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !condition() {
            if std::time::Instant::now() > deadline {
                return Err("timed out waiting for live mempool owner state".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}
