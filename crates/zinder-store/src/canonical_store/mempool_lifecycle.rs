//! Durable live mempool event lifecycle.
//!
//! The canonical primary retains the resumable event log but never stores the
//! mutable mempool index. The ingest owner appends an event here before it
//! publishes the matching in-memory index transition, so a writer restart can
//! always recover the event cursor history without reopening the canonical primary.

use std::{
    collections::{BTreeSet, HashMap},
    sync::atomic::{AtomicU64, Ordering},
};

use rust_rocksdb::{DB, Direction, IteratorMode, WriteBatch, WriteOptions};
use tokio::sync::watch;
use zinder_core::{Network, TransactionId, UnixTimestampMillis};

use crate::{
    EventStreamStartPosition, MempoolEvent, MempoolEventEnvelope, MempoolEventHistoryRequest,
    MempoolEventPosition, MempoolEventRetentionConfig, MempoolEventRetentionReport,
    MempoolEventRetentionStepBudget, MempoolEventRetentionStepOutcome,
    MempoolEventRetentionStepStop, SnapshotPageCursorAnchor, StreamCursorTokenV1,
    format::{StoreKey, decode_mempool_event_envelope, encode_mempool_event_envelope},
};

use super::{
    CanonicalStoreError, RocksDbCanonicalStore, publication::column_family,
    rocksdb::MEMPOOL_EVENT_COLUMN_FAMILY,
};

/// Durable mempool-event sequence pointer in the canonical control family.
pub(super) const MEMPOOL_EVENT_SEQUENCE_KEY: &[u8] = b"mempool_event_sequence_v1";
/// Durable inclusive retention floor for the canonical mempool-event log.
pub(super) const MEMPOOL_EVENT_RETENTION_FLOOR_KEY: &[u8] = b"mempool_event_retention_floor_v1";

/// Tracks the latest unmatched `Added` sequence per transaction while
/// maintaining the earliest active addition sequence in `O(log N)`.
#[derive(Debug, Default)]
pub(super) struct ActiveAddSequences {
    by_transaction_id: HashMap<TransactionId, u64>,
    by_sequence: BTreeSet<(u64, TransactionId)>,
}

impl ActiveAddSequences {
    pub(super) fn record_added(&mut self, transaction_id: TransactionId, event_sequence: u64) {
        if let Some(previous_sequence) = self
            .by_transaction_id
            .insert(transaction_id, event_sequence)
        {
            self.by_sequence
                .remove(&(previous_sequence, transaction_id));
        }
        self.by_sequence.insert((event_sequence, transaction_id));
    }

    pub(super) fn record_removed(&mut self, transaction_id: TransactionId) {
        if let Some(previous_sequence) = self.by_transaction_id.remove(&transaction_id) {
            self.by_sequence
                .remove(&(previous_sequence, transaction_id));
        }
    }

    pub(super) fn earliest_sequence(&self) -> Option<u64> {
        self.by_sequence.first().map(|&(sequence, _)| sequence)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.by_transaction_id.len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.by_transaction_id.is_empty()
    }
}

#[derive(Debug, Default)]
pub(super) struct MempoolEventReadCounters {
    event_row_reads: AtomicU64,
    envelope_decodes: AtomicU64,
}

impl MempoolEventReadCounters {
    fn record_event_row_read(&self) {
        self.event_row_reads.fetch_add(1, Ordering::Relaxed);
    }

    fn record_envelope_decode(&self) {
        self.envelope_decodes.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> MempoolEventReadCounts {
        MempoolEventReadCounts {
            event_row_reads: self.event_row_reads.load(Ordering::Relaxed),
            envelope_decodes: self.envelope_decodes.load(Ordering::Relaxed),
        }
    }
}

/// Cumulative durable mempool-event row reads and envelope decodes observed
/// through a [`RocksDbCanonicalStore`] handle.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MempoolEventReadCounts {
    /// Number of `mempool_event` column-family rows read.
    pub event_row_reads: u64,
    /// Number of durable mempool-event envelopes decoded.
    pub envelope_decodes: u64,
}

/// Live subscription to monotonic advances of the durable mempool-event head
/// sequence.
#[derive(Clone, Debug)]
pub struct MempoolEventHeadSubscription {
    head_sequence: watch::Receiver<u64>,
}

impl MempoolEventHeadSubscription {
    /// Returns the latest durable mempool-event head sequence observed by this
    /// subscription, marking it seen.
    pub fn current_head_sequence(&mut self) -> u64 {
        *self.head_sequence.borrow_and_update()
    }

    /// Waits until the durable mempool-event head sequence strictly exceeds
    /// `sequence`, or returns `None` if the owning store was dropped.
    pub async fn wait_for_advance_past(&mut self, sequence: u64) -> Option<u64> {
        self.head_sequence
            .wait_for(|head| *head > sequence)
            .await
            .ok()
            .map(|head| *head)
    }
}

/// Resolved start of a durable mempool-event stream paired with a live
/// head-advance subscription captured under the same lifecycle lock.
#[derive(Debug)]
pub struct CanonicalMempoolEventStreamStart {
    after_cursor: Option<StreamCursorTokenV1>,
    after_sequence: u64,
    head_sequence: u64,
    head_subscription: MempoolEventHeadSubscription,
}

impl CanonicalMempoolEventStreamStart {
    /// Returns the cursor that page reads must resume strictly after.
    #[must_use]
    pub const fn after_cursor(&self) -> Option<&StreamCursorTokenV1> {
        self.after_cursor.as_ref()
    }

    /// Returns the event sequence matching [`Self::after_cursor`], or `0` when
    /// resuming from the earliest retained event.
    #[must_use]
    pub const fn after_sequence(&self) -> u64 {
        self.after_sequence
    }

    /// Returns the durable head sequence observed when the stream opened.
    #[must_use]
    pub const fn head_sequence(&self) -> u64 {
        self.head_sequence
    }

    /// Unpacks the resume cursor, starting sequence, initial durable head
    /// sequence, and live head-advance subscription.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Option<StreamCursorTokenV1>,
        u64,
        u64,
        MempoolEventHeadSubscription,
    ) {
        (
            self.after_cursor,
            self.after_sequence,
            self.head_sequence,
            self.head_subscription,
        )
    }
}

/// Process-local progress for one resumable bounded retention scan.
///
/// Losing this state on restart is safe: the durable floor remains the source
/// of truth and the next step reconstructs progress from that floor.
#[derive(Debug)]
pub(super) struct MempoolEventRetentionProgress {
    expected_floor: u64,
    captured_head: u64,
    next_sequence: u64,
    active_add_sequences: ActiveAddSequences,
    observed_at: UnixTimestampMillis,
    retention: MempoolEventRetentionConfig,
    prunable_through: u64,
    terminal_stop: Option<MempoolEventRetentionStepStop>,
}

/// Reads the persisted mempool-event head sequence when opening a READY store.
pub(super) fn read_initial_mempool_event_sequence(db: &DB) -> Result<u64, CanonicalStoreError> {
    match read_admission_control_sequence(db, MEMPOOL_EVENT_SEQUENCE_KEY, "head")? {
        None => Ok(0),
        Some(0) => Err(CanonicalStoreError::MempoolEventLogInvalid {
            reason: "mempool event sequence pointer must be absent or nonzero".to_owned(),
        }),
        Some(sequence) => Ok(sequence),
    }
}

/// Validates the bounded durable mempool-log admission invariant.
///
/// The primary only needs to authenticate the floor and head rows while it
/// opens: the paged history read detects any interior gap before it could be
/// skipped. This keeps READY admission bounded even for a long retention
/// window while refusing metadata/column-family disagreement.
pub(super) fn validate_mempool_lifecycle_admission(
    db: &DB,
    network: Network,
    cursor_auth_key: [u8; 32],
) -> Result<(), CanonicalStoreError> {
    let head = read_admission_control_sequence(db, MEMPOOL_EVENT_SEQUENCE_KEY, "head")?;
    let floor = read_admission_control_sequence(db, MEMPOOL_EVENT_RETENTION_FLOOR_KEY, "floor")?;
    let event_family = column_family(db, MEMPOOL_EVENT_COLUMN_FAMILY)?;
    let first_row = db
        .iterator_cf(&event_family, IteratorMode::Start)
        .next()
        .transpose()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "mempool event admission first-row read",
            source,
        })?;
    let last_row = db
        .iterator_cf(&event_family, IteratorMode::End)
        .next()
        .transpose()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "mempool event admission last-row read",
            source,
        })?;

    match (head, floor, first_row, last_row) {
        (None, None, None, None) => Ok(()),
        (Some(head), Some(floor), Some(first), Some(last)) => {
            if head == 0 || floor == 0 || floor > head {
                return Err(CanonicalStoreError::MempoolEventLogInvalid {
                    reason: "mempool event floor and head are outside retained history".to_owned(),
                });
            }
            validate_admission_event_row(network, cursor_auth_key, floor, &first)?;
            validate_admission_event_row(network, cursor_auth_key, head, &last)?;
            Ok(())
        }
        (None, None, Some(_), Some(_)) => Err(CanonicalStoreError::MempoolEventLogInvalid {
            reason: "mempool event column family has orphan rows without lifecycle metadata"
                .to_owned(),
        }),
        _ => Err(CanonicalStoreError::MempoolEventLogInvalid {
            reason: "mempool event lifecycle metadata and column family disagree".to_owned(),
        }),
    }
}

/// Decoded start of a paginated in-memory mempool snapshot.
///
/// The cursor is authenticated by the canonical primary and carries the
/// durable event position from which a consumer may subsequently resume.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalMempoolSnapshotStart {
    after_transaction_id: Option<TransactionId>,
    events_resume_anchor: Option<MempoolEventPosition>,
    events_resume_cursor: Option<StreamCursorTokenV1>,
}

impl CanonicalMempoolSnapshotStart {
    /// Returns the transaction-id paging position for the in-memory index.
    #[must_use]
    pub const fn after_transaction_id(&self) -> Option<TransactionId> {
        self.after_transaction_id
    }

    /// Returns the durable event position captured by this snapshot walk.
    #[must_use]
    pub const fn events_resume_anchor(&self) -> Option<MempoolEventPosition> {
        self.events_resume_anchor
    }

    /// Returns the signed event cursor matching the snapshot anchor.
    #[must_use]
    pub fn events_resume_cursor(&self) -> Option<&StreamCursorTokenV1> {
        self.events_resume_cursor.as_ref()
    }
}

impl RocksDbCanonicalStore {
    /// Appends one durable mempool transition before the ingest owner mutates
    /// its process-local live index.
    ///
    /// The event row, monotonic head pointer, and first retention floor are
    /// written in one synced batch. Callers must preflight the matching index
    /// mutation before invoking this method, then apply it with the returned
    /// position under their mutation gate.
    pub fn append_mempool_event(
        &self,
        event: MempoolEvent,
        source_observed_at: UnixTimestampMillis,
    ) -> Result<MempoolEventEnvelope, CanonicalStoreError> {
        self.append_mempool_events(vec![(event, source_observed_at)])?
            .into_iter()
            .next()
            .ok_or_else(|| CanonicalStoreError::MempoolEventLogInvalid {
                reason: "single mempool event append produced no durable envelope".to_owned(),
            })
    }

    /// Appends an ordered batch of durable mempool transitions before the
    /// ingest owner mutates its process-local live index.
    ///
    /// Every event row, the final monotonic head pointer, and the first
    /// retention floor are committed in one synced `RocksDB` write. Callers
    /// preflight the complete transition set before invoking this method,
    /// then apply the returned positions in order under their mutation gate.
    pub fn append_mempool_events(
        &self,
        events: Vec<(MempoolEvent, UnixTimestampMillis)>,
    ) -> Result<Vec<MempoolEventEnvelope>, CanonicalStoreError> {
        for (event, _) in &events {
            validate_append_event(event)?;
        }
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let _lifecycle_guard = self.lifecycle_lock.lock();
        let current_event_sequence = current_mempool_event_sequence(self)?;
        if current_event_sequence == 0 {
            validate_mempool_lifecycle_admission(
                &self.bounded_open.db,
                self.network(),
                self.cursor_auth_key,
            )?;
        }
        let event_family = column_family(&self.bounded_open.db, MEMPOOL_EVENT_COLUMN_FAMILY)?;
        let mut batch = WriteBatch::default();
        let mut event_sequence = current_event_sequence;
        let mut envelopes = Vec::with_capacity(events.len());
        for (event, source_observed_at) in events {
            event_sequence = event_sequence
                .checked_add(1)
                .ok_or(CanonicalStoreError::MempoolEventSequenceOverflow)?;
            let cursor = mempool_event_cursor(self, event_sequence, event.transaction_id())?;
            let envelope = MempoolEventEnvelope {
                cursor,
                event_sequence,
                source_observed_unix_millis: source_observed_at.value(),
                event,
            };
            let encoded_envelope = encode_mempool_event_envelope(&envelope).map_err(|error| {
                CanonicalStoreError::MempoolEventLogInvalid {
                    reason: error.to_string(),
                }
            })?;
            batch.put_cf(
                &event_family,
                event_sequence.to_be_bytes(),
                encoded_envelope,
            );
            envelopes.push(envelope);
        }
        batch.put(MEMPOOL_EVENT_SEQUENCE_KEY, event_sequence.to_be_bytes());
        if current_event_sequence == 0 {
            batch.put(MEMPOOL_EVENT_RETENTION_FLOOR_KEY, 1_u64.to_be_bytes());
        }
        write_mempool_lifecycle_batch(self, &batch, "mempool event batch append")?;
        self.mempool_event_head.send_replace(event_sequence);
        Ok(envelopes)
    }

    /// Returns cumulative durable mempool-event row reads and envelope decodes
    /// observed through this canonical store handle.
    #[must_use]
    pub fn mempool_event_read_counts(&self) -> MempoolEventReadCounts {
        self.mempool_event_read_counters.snapshot()
    }

    /// Reads a bounded page of durable mempool events strictly after its
    /// authenticated cursor, or from the retained floor when absent.
    pub fn mempool_event_history(
        &self,
        request: MempoolEventHistoryRequest<'_>,
    ) -> Result<Vec<MempoolEventEnvelope>, CanonicalStoreError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        let current_event_sequence = current_mempool_event_sequence(self)?;
        if current_event_sequence == 0 {
            if request.from_cursor.is_some() {
                return Err(CanonicalStoreError::MempoolEventCursorInvalid {
                    reason: "cursor sequence is ahead of retained history",
                });
            }
            return Ok(Vec::new());
        }
        let oldest_retained_sequence = mempool_event_retention_floor(self, current_event_sequence)?;
        let start_sequence = match request.from_cursor {
            None => oldest_retained_sequence,
            Some(cursor) => validate_mempool_resume_cursor(
                self,
                cursor,
                current_event_sequence,
                oldest_retained_sequence,
            )?,
        };
        if start_sequence > current_event_sequence {
            return Ok(Vec::new());
        }

        let max_events = usize::try_from(request.max_events.get()).map_err(|_| {
            CanonicalStoreError::MempoolEventLogInvalid {
                reason: "mempool event page length cannot fit this platform".to_owned(),
            }
        })?;
        let event_family = column_family(&self.bounded_open.db, MEMPOOL_EVENT_COLUMN_FAMILY)?;
        let mut events = Vec::with_capacity(max_events);
        let iterator = self.bounded_open.db.iterator_cf(
            &event_family,
            IteratorMode::From(&start_sequence.to_be_bytes(), Direction::Forward),
        );
        for row in iterator {
            let (key, encoded_event) =
                row.map_err(|source| CanonicalStoreError::RocksDbOperation {
                    operation: "mempool event history scan",
                    source,
                })?;
            self.mempool_event_read_counters.record_event_row_read();
            let event_sequence = event_sequence_from_key(&key)?;
            if event_sequence > current_event_sequence || events.len() >= max_events {
                break;
            }
            if event_sequence < start_sequence {
                continue;
            }
            let emitted_event_count = u64::try_from(events.len()).map_err(|_| {
                CanonicalStoreError::MempoolEventLogInvalid {
                    reason: "mempool event page length cannot fit u64".to_owned(),
                }
            })?;
            let expected_sequence = start_sequence
                .checked_add(emitted_event_count)
                .ok_or(CanonicalStoreError::MempoolEventSequenceOverflow)?;
            if event_sequence != expected_sequence {
                return Err(CanonicalStoreError::MempoolEventCursorExpired {
                    event_sequence: event_sequence.saturating_sub(1),
                    oldest_retained_sequence: event_sequence,
                });
            }
            events.push(decode_mempool_event(self, event_sequence, &encoded_event)?);
        }
        if events.is_empty() && start_sequence <= current_event_sequence {
            return Err(CanonicalStoreError::MempoolEventLogInvalid {
                reason: "mempool event head points to an absent retained row".to_owned(),
            });
        }
        Ok(events)
    }

    /// Resolves a public mempool-event subscription start to the cursor that
    /// page reads must resume strictly after.
    pub fn resolve_mempool_event_stream_start(
        &self,
        start: &EventStreamStartPosition,
    ) -> Result<Option<StreamCursorTokenV1>, CanonicalStoreError> {
        Ok(self.open_mempool_event_stream(start)?.after_cursor)
    }

    /// Resolves a public mempool-event subscription start and captures a
    /// head-advance subscription under the same lifecycle lock.
    pub fn open_mempool_event_stream(
        &self,
        start: &EventStreamStartPosition,
    ) -> Result<CanonicalMempoolEventStreamStart, CanonicalStoreError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        let current_event_sequence = current_mempool_event_sequence(self)?;
        if *self.mempool_event_head.borrow() < current_event_sequence {
            self.mempool_event_head.send_replace(current_event_sequence);
        }
        let head_subscription = MempoolEventHeadSubscription {
            head_sequence: self.mempool_event_head.subscribe(),
        };
        let (after_cursor, after_sequence) = match start {
            EventStreamStartPosition::EarliestRetained => (None, 0),
            EventStreamStartPosition::AfterCursor(cursor) => {
                if current_event_sequence == 0 {
                    return Err(CanonicalStoreError::MempoolEventCursorInvalid {
                        reason: "cursor sequence is ahead of retained history",
                    });
                }
                let oldest_retained_sequence =
                    mempool_event_retention_floor(self, current_event_sequence)?;
                let next_sequence = validate_mempool_resume_cursor(
                    self,
                    cursor,
                    current_event_sequence,
                    oldest_retained_sequence,
                )?;
                (Some(cursor.clone()), next_sequence.saturating_sub(1))
            }
            EventStreamStartPosition::LiveTail => {
                if current_event_sequence == 0 {
                    (None, 0)
                } else {
                    let envelope = read_mempool_event(self, current_event_sequence)?;
                    (Some(envelope.cursor), current_event_sequence)
                }
            }
        };
        Ok(CanonicalMempoolEventStreamStart {
            after_cursor,
            after_sequence,
            head_sequence: current_event_sequence,
            head_subscription,
        })
    }

    /// Decodes a snapshot paging cursor or captures the current durable
    /// mempool-event head for a new snapshot walk.
    pub fn begin_mempool_snapshot(
        &self,
        cursor_bytes: &[u8],
    ) -> Result<CanonicalMempoolSnapshotStart, CanonicalStoreError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        let current_event_sequence = current_mempool_event_sequence(self)?;
        let current_anchor = if current_event_sequence == 0 {
            None
        } else {
            Some(read_mempool_event(self, current_event_sequence)?.position())
        };

        let (after_transaction_id, events_resume_anchor) = if cursor_bytes.is_empty() {
            (None, current_anchor)
        } else {
            let cursor = StreamCursorTokenV1::from_bytes(cursor_bytes.to_vec());
            let payload = cursor
                .decode_snapshot_page(self.network(), self.cursor_auth_key)
                .map_err(|_| CanonicalStoreError::MempoolSnapshotCursorInvalid {
                    reason: "cursor token failed validation",
                })?;
            if payload
                .events_resume_anchor
                .is_some_and(|anchor| anchor.event_sequence > current_event_sequence)
            {
                return Err(CanonicalStoreError::MempoolSnapshotCursorExpired {
                    anchor_event_sequence: payload
                        .events_resume_anchor
                        .map_or(0, |anchor| anchor.event_sequence),
                    current_event_sequence,
                });
            }
            (
                Some(payload.after_transaction_id),
                payload.events_resume_anchor,
            )
        };
        let events_resume_cursor = events_resume_anchor
            .map(|anchor| mempool_event_cursor(self, anchor.event_sequence, anchor.transaction_id))
            .transpose()?;
        Ok(CanonicalMempoolSnapshotStart {
            after_transaction_id,
            events_resume_anchor,
            events_resume_cursor,
        })
    }

    /// Mints the authenticated next-page cursor for one existing snapshot
    /// walk. The caller supplies the same durable anchor returned by
    /// [`Self::begin_mempool_snapshot`].
    pub fn encode_mempool_snapshot_next_cursor(
        &self,
        events_resume_anchor: Option<MempoolEventPosition>,
        after_transaction_id: TransactionId,
    ) -> Result<StreamCursorTokenV1, CanonicalStoreError> {
        StreamCursorTokenV1::snapshot_page(
            SnapshotPageCursorAnchor {
                network: self.network(),
                events_resume_anchor,
                after_transaction_id,
            },
            self.cursor_auth_key,
        )
        .map_err(|_| CanonicalStoreError::MempoolSnapshotCursorInvalid {
            reason: "cursor authentication key could not initialize the MAC",
        })
    }

    /// Reads the durable mempool-event retention floor and per-variant counts.
    pub fn mempool_event_retention_report(
        &self,
    ) -> Result<MempoolEventRetentionReport, CanonicalStoreError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        mempool_event_retention_report_locked(self)
    }

    /// Advances one bounded step of durable mempool-event retention.
    ///
    /// Process-local progress resumes the forward scan across calls. Each
    /// step bounds decoded event bytes and deletes; inspecting a candidate
    /// must fetch its encoded value before its size is known. The durable
    /// floor and current head remain retained as crash-safe authorities.
    pub fn advance_mempool_event_retention(
        &self,
        now: UnixTimestampMillis,
        retention: MempoolEventRetentionConfig,
        budget: MempoolEventRetentionStepBudget,
    ) -> Result<MempoolEventRetentionStepOutcome, CanonicalStoreError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        let current_event_sequence = current_mempool_event_sequence(self)?;
        if current_event_sequence == 0 {
            self.mempool_retention_progress.lock().take();
            return Ok(MempoolEventRetentionStepOutcome {
                report: MempoolEventRetentionReport::default(),
                examined_event_count: 0,
                examined_encoded_bytes: 0,
                stop: MempoolEventRetentionStepStop::ReachedHead,
            });
        }
        if retention.is_unbounded() {
            self.mempool_retention_progress.lock().take();
            return Ok(MempoolEventRetentionStepOutcome {
                report: mempool_event_retention_report_locked(self)?,
                examined_event_count: 0,
                examined_encoded_bytes: 0,
                stop: MempoolEventRetentionStepStop::RetentionDisabled,
            });
        }
        let oldest_retained_sequence = mempool_event_retention_floor(self, current_event_sequence)?;
        let mut progress_guard = self.mempool_retention_progress.lock();
        let progress_is_stale = progress_guard.as_ref().is_some_and(|progress| {
            progress.expected_floor != oldest_retained_sequence || progress.retention != retention
        });
        if progress_is_stale {
            progress_guard.take();
        }
        let progress = progress_guard.get_or_insert_with(|| MempoolEventRetentionProgress {
            expected_floor: oldest_retained_sequence,
            captured_head: current_event_sequence,
            next_sequence: oldest_retained_sequence,
            active_add_sequences: ActiveAddSequences::default(),
            observed_at: now,
            retention,
            prunable_through: oldest_retained_sequence.saturating_sub(1),
            terminal_stop: None,
        });

        let mut work =
            perform_mempool_retention_step(self, progress, budget, oldest_retained_sequence)?;

        if work.new_floor != oldest_retained_sequence {
            work.batch.put(
                MEMPOOL_EVENT_RETENTION_FLOOR_KEY,
                work.new_floor.to_be_bytes(),
            );
            write_mempool_lifecycle_batch(self, &work.batch, "mempool event prune")?;
            progress.expected_floor = work.new_floor;
        }
        let pending_deletes = work.new_floor <= progress.prunable_through;
        let stop = if pending_deletes || progress.terminal_stop.is_none() {
            MempoolEventRetentionStepStop::BudgetExhausted
        } else {
            progress
                .terminal_stop
                .unwrap_or(MempoolEventRetentionStepStop::ReachedHead)
        };
        if stop != MempoolEventRetentionStepStop::BudgetExhausted {
            progress_guard.take();
        }
        drop(progress_guard);
        let mut report = mempool_event_retention_report_locked(self)?;
        report.pruned_added_count = work.pruned_added_count;
        report.pruned_mined_count = work.pruned_mined_count;
        report.pruned_invalidated_count = work.pruned_invalidated_count;
        Ok(MempoolEventRetentionStepOutcome {
            report,
            examined_event_count: work.examined_event_count,
            examined_encoded_bytes: work.examined_encoded_bytes,
            stop,
        })
    }
}

fn perform_mempool_retention_step(
    store: &RocksDbCanonicalStore,
    progress: &mut MempoolEventRetentionProgress,
    budget: MempoolEventRetentionStepBudget,
    oldest_retained_sequence: u64,
) -> Result<MempoolRetentionStepWork, CanonicalStoreError> {
    let mut work = MempoolRetentionStepWork::new(oldest_retained_sequence);
    loop {
        let examined_event_count_before = work.examined_event_count;
        let new_floor_before = work.new_floor;
        let next_sequence_before = progress.next_sequence;
        delete_prunable_mempool_events(store, progress, budget, &mut work)?;
        scan_expired_mempool_events(store, progress, budget, &mut work)?;
        let made_progress = work.examined_event_count != examined_event_count_before
            || work.new_floor != new_floor_before
            || progress.next_sequence != next_sequence_before;
        if !made_progress {
            return Ok(work);
        }
    }
}

struct MempoolRetentionStepWork {
    batch: WriteBatch,
    new_floor: u64,
    examined_event_count: u32,
    examined_encoded_bytes: u64,
    pruned_added_count: u64,
    pruned_mined_count: u64,
    pruned_invalidated_count: u64,
}

impl MempoolRetentionStepWork {
    fn new(oldest_retained_sequence: u64) -> Self {
        Self {
            batch: WriteBatch::default(),
            new_floor: oldest_retained_sequence,
            examined_event_count: 0,
            examined_encoded_bytes: 0,
            pruned_added_count: 0,
            pruned_mined_count: 0,
            pruned_invalidated_count: 0,
        }
    }

    fn can_read_another(&self, budget: MempoolEventRetentionStepBudget) -> bool {
        self.examined_event_count < budget.max_events().get()
            && (self.examined_event_count == 0
                || self.examined_encoded_bytes < budget.max_encoded_bytes().get())
    }

    fn can_examine(&self, encoded_bytes: u64, budget: MempoolEventRetentionStepBudget) -> bool {
        self.examined_event_count < budget.max_events().get()
            && (self.examined_event_count == 0
                || self.examined_encoded_bytes.saturating_add(encoded_bytes)
                    <= budget.max_encoded_bytes().get())
    }

    fn record_examined(&mut self, encoded_bytes: u64) {
        self.examined_event_count = self.examined_event_count.saturating_add(1);
        self.examined_encoded_bytes = self.examined_encoded_bytes.saturating_add(encoded_bytes);
    }
}

fn delete_prunable_mempool_events(
    store: &RocksDbCanonicalStore,
    progress: &MempoolEventRetentionProgress,
    budget: MempoolEventRetentionStepBudget,
    work: &mut MempoolRetentionStepWork,
) -> Result<(), CanonicalStoreError> {
    let event_family = column_family(&store.bounded_open.db, MEMPOOL_EVENT_COLUMN_FAMILY)?;
    while work.new_floor <= progress.prunable_through && work.can_read_another(budget) {
        let event_sequence = work.new_floor;
        let Some(encoded_event) = read_encoded_mempool_event(store, event_sequence)? else {
            return Err(CanonicalStoreError::MempoolEventLogInvalid {
                reason: format!("mempool event {event_sequence} is absent"),
            });
        };
        let encoded_bytes = u64::try_from(encoded_event.len()).unwrap_or(u64::MAX);
        if !work.can_examine(encoded_bytes, budget) {
            break;
        }
        let envelope = decode_mempool_event(store, event_sequence, &encoded_event)?;
        increment_pruned_count(
            &envelope.event,
            &mut work.pruned_added_count,
            &mut work.pruned_mined_count,
            &mut work.pruned_invalidated_count,
        )?;
        work.batch
            .delete_cf(&event_family, event_sequence.to_be_bytes());
        work.record_examined(encoded_bytes);
        work.new_floor = event_sequence.saturating_add(1);
    }
    Ok(())
}

fn scan_expired_mempool_events(
    store: &RocksDbCanonicalStore,
    progress: &mut MempoolEventRetentionProgress,
    budget: MempoolEventRetentionStepBudget,
    work: &mut MempoolRetentionStepWork,
) -> Result<(), CanonicalStoreError> {
    while work.new_floor > progress.prunable_through
        && progress.terminal_stop.is_none()
        && progress.next_sequence <= progress.captured_head
        && work.can_read_another(budget)
    {
        let event_sequence = progress.next_sequence;
        let Some(encoded_event) = read_encoded_mempool_event(store, event_sequence)? else {
            return Err(CanonicalStoreError::MempoolEventLogInvalid {
                reason: format!("mempool event {event_sequence} is absent"),
            });
        };
        let encoded_bytes = u64::try_from(encoded_event.len()).unwrap_or(u64::MAX);
        if !work.can_examine(encoded_bytes, budget) {
            break;
        }
        let envelope = decode_mempool_event(store, event_sequence, &encoded_event)?;
        work.record_examined(encoded_bytes);
        let Some(window) = retention_window_for(&envelope.event, progress.retention)? else {
            progress.terminal_stop = Some(MempoolEventRetentionStepStop::ReachedUnexpiredEvent);
            break;
        };
        if !age_exceeds_window(
            progress.observed_at,
            envelope.source_observed_unix_millis,
            window,
        ) {
            progress.terminal_stop = Some(MempoolEventRetentionStepStop::ReachedUnexpiredEvent);
            break;
        }
        update_active_add_sequences(
            &mut progress.active_add_sequences,
            event_sequence,
            &envelope.event,
        )?;
        if event_sequence == progress.captured_head {
            progress.terminal_stop = Some(MempoolEventRetentionStepStop::ReachedHead);
        } else {
            progress.next_sequence = event_sequence.saturating_add(1);
        }
        progress.prunable_through = prunable_through(
            event_sequence,
            progress.captured_head,
            &progress.active_add_sequences,
        );
    }
    Ok(())
}

#[allow(
    unreachable_patterns,
    reason = "MempoolEvent is non-exhaustive; unrecognized variants must not silently change replay retention."
)]
fn update_active_add_sequences(
    active_add_sequences: &mut ActiveAddSequences,
    event_sequence: u64,
    event: &MempoolEvent,
) -> Result<(), CanonicalStoreError> {
    match event {
        MempoolEvent::Added { entry } => {
            active_add_sequences.record_added(entry.transaction_id(), event_sequence);
        }
        MempoolEvent::Invalidated { transaction_id, .. }
        | MempoolEvent::Mined { transaction_id, .. } => {
            active_add_sequences.record_removed(*transaction_id);
        }
        _ => {
            return Err(CanonicalStoreError::MempoolEventLogInvalid {
                reason: "mempool event variant is unsupported for replay retention".to_owned(),
            });
        }
    }
    Ok(())
}

fn prunable_through(
    latest_inspected_sequence: u64,
    captured_head: u64,
    active_add_sequences: &ActiveAddSequences,
) -> u64 {
    let before_head = latest_inspected_sequence.min(captured_head.saturating_sub(1));
    active_add_sequences
        .earliest_sequence()
        .map_or(before_head, |earliest_active_add| {
            before_head.min(earliest_active_add.saturating_sub(1))
        })
}

#[allow(
    unreachable_patterns,
    reason = "MempoolEvent is non-exhaustive outside zinder-store; unrecognized variants fail closed before durable append."
)]
fn validate_append_event(event: &MempoolEvent) -> Result<(), CanonicalStoreError> {
    match event {
        MempoolEvent::Added { .. }
        | MempoolEvent::Invalidated { .. }
        | MempoolEvent::Mined { .. } => Ok(()),
        _ => Err(CanonicalStoreError::MempoolEventLogInvalid {
            reason: "mempool event variant is unsupported".to_owned(),
        }),
    }
}

fn current_mempool_event_sequence(
    store: &RocksDbCanonicalStore,
) -> Result<u64, CanonicalStoreError> {
    let sequence_pointer_bytes = store
        .bounded_open
        .db
        .get(MEMPOOL_EVENT_SEQUENCE_KEY)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "mempool event sequence read",
            source,
        })?;
    match sequence_pointer_bytes {
        None => Ok(0),
        Some(bytes) => {
            let sequence = decode_u64(&bytes, "mempool event sequence pointer")?;
            if sequence == 0 {
                return Err(CanonicalStoreError::MempoolEventLogInvalid {
                    reason: "mempool event sequence pointer must be absent or nonzero".to_owned(),
                });
            }
            Ok(sequence)
        }
    }
}

fn mempool_event_retention_floor(
    store: &RocksDbCanonicalStore,
    current_event_sequence: u64,
) -> Result<u64, CanonicalStoreError> {
    let retention_floor_bytes = store
        .bounded_open
        .db
        .get(MEMPOOL_EVENT_RETENTION_FLOOR_KEY)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "mempool event retention floor read",
            source,
        })?;
    let floor = retention_floor_bytes.map_or(Ok(1), |bytes| {
        decode_u64(&bytes, "mempool event retention floor")
    })?;
    if floor == 0 || floor > current_event_sequence {
        return Err(CanonicalStoreError::MempoolEventLogInvalid {
            reason: "mempool event retention floor is outside retained history".to_owned(),
        });
    }
    Ok(floor)
}

fn validate_mempool_resume_cursor(
    store: &RocksDbCanonicalStore,
    cursor: &StreamCursorTokenV1,
    current_event_sequence: u64,
    oldest_retained_sequence: u64,
) -> Result<u64, CanonicalStoreError> {
    let payload = cursor
        .decode_mempool_event(store.network(), store.cursor_auth_key)
        .map_err(|_| CanonicalStoreError::MempoolEventCursorInvalid {
            reason: "cursor token failed validation",
        })?;
    if payload.event_sequence > current_event_sequence {
        return Err(CanonicalStoreError::MempoolEventCursorInvalid {
            reason: "cursor sequence is ahead of retained history",
        });
    }
    if payload.event_sequence < oldest_retained_sequence {
        return Err(CanonicalStoreError::MempoolEventCursorExpired {
            event_sequence: payload.event_sequence,
            oldest_retained_sequence,
        });
    }
    let envelope = read_mempool_event(store, payload.event_sequence)?;
    if envelope.transaction_id() != payload.last_transaction_id {
        return Err(CanonicalStoreError::MempoolEventCursorInvalid {
            reason: "cursor transaction id does not match the retained event",
        });
    }
    payload
        .event_sequence
        .checked_add(1)
        .ok_or(CanonicalStoreError::MempoolEventSequenceOverflow)
}

fn mempool_event_cursor(
    store: &RocksDbCanonicalStore,
    event_sequence: u64,
    transaction_id: TransactionId,
) -> Result<StreamCursorTokenV1, CanonicalStoreError> {
    StreamCursorTokenV1::mempool_event(
        store.network(),
        event_sequence,
        transaction_id,
        store.cursor_auth_key,
    )
    .map_err(|_| CanonicalStoreError::MempoolEventLogInvalid {
        reason: "cursor authentication key could not initialize the MAC".to_owned(),
    })
}

fn read_mempool_event(
    store: &RocksDbCanonicalStore,
    event_sequence: u64,
) -> Result<MempoolEventEnvelope, CanonicalStoreError> {
    let encoded_event = read_encoded_mempool_event(store, event_sequence)?.ok_or_else(|| {
        CanonicalStoreError::MempoolEventLogInvalid {
            reason: format!("mempool event {event_sequence} is absent"),
        }
    })?;
    decode_mempool_event(store, event_sequence, &encoded_event)
}

fn read_encoded_mempool_event(
    store: &RocksDbCanonicalStore,
    event_sequence: u64,
) -> Result<Option<Vec<u8>>, CanonicalStoreError> {
    let event_family = column_family(&store.bounded_open.db, MEMPOOL_EVENT_COLUMN_FAMILY)?;
    let encoded_event = store
        .bounded_open
        .db
        .get_cf(&event_family, event_sequence.to_be_bytes())
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "mempool event read",
            source,
        })?;
    store.mempool_event_read_counters.record_event_row_read();
    Ok(encoded_event)
}

fn decode_mempool_event(
    store: &RocksDbCanonicalStore,
    event_sequence: u64,
    encoded_event: &[u8],
) -> Result<MempoolEventEnvelope, CanonicalStoreError> {
    store.mempool_event_read_counters.record_envelope_decode();
    let envelope = decode_mempool_event_envelope(
        &StoreKey::mempool_event(event_sequence),
        encoded_event,
        store.network(),
        store.cursor_auth_key,
    )
    .map_err(|error| CanonicalStoreError::MempoolEventLogInvalid {
        reason: error.to_string(),
    })?;
    if envelope.event_sequence != event_sequence {
        return Err(CanonicalStoreError::MempoolEventLogInvalid {
            reason: format!(
                "mempool event key {event_sequence} does not match envelope sequence {}",
                envelope.event_sequence
            ),
        });
    }
    Ok(envelope)
}

fn event_sequence_from_key(key: &[u8]) -> Result<u64, CanonicalStoreError> {
    let key =
        <[u8; 8]>::try_from(key).map_err(|_| CanonicalStoreError::MempoolEventLogInvalid {
            reason: "mempool event key must be exactly 8 bytes".to_owned(),
        })?;
    Ok(u64::from_be_bytes(key))
}

fn decode_u64(bytes: &[u8], field: &'static str) -> Result<u64, CanonicalStoreError> {
    let bytes =
        <[u8; 8]>::try_from(bytes).map_err(|_| CanonicalStoreError::MempoolEventLogInvalid {
            reason: format!("{field} must be exactly 8 bytes"),
        })?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_admission_control_sequence(
    db: &DB,
    key: &[u8],
    field: &'static str,
) -> Result<Option<u64>, CanonicalStoreError> {
    db.get(key)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "mempool event admission metadata read",
            source,
        })?
        .map(|bytes| decode_u64(&bytes, field))
        .transpose()
}

fn validate_admission_event_row(
    network: Network,
    cursor_auth_key: [u8; 32],
    expected_sequence: u64,
    row: &(Box<[u8]>, Box<[u8]>),
) -> Result<(), CanonicalStoreError> {
    let observed_sequence = event_sequence_from_key(&row.0)?;
    if observed_sequence != expected_sequence {
        return Err(CanonicalStoreError::MempoolEventLogInvalid {
            reason: format!(
                "mempool event physical key {observed_sequence} does not match admitted sequence {expected_sequence}"
            ),
        });
    }
    let envelope = decode_mempool_event_envelope(
        &StoreKey::mempool_event(observed_sequence),
        &row.1,
        network,
        cursor_auth_key,
    )
    .map_err(|error| CanonicalStoreError::MempoolEventLogInvalid {
        reason: error.to_string(),
    })?;
    if envelope.event_sequence != observed_sequence {
        return Err(CanonicalStoreError::MempoolEventLogInvalid {
            reason: format!(
                "mempool event physical key {observed_sequence} does not match envelope sequence {}",
                envelope.event_sequence
            ),
        });
    }
    Ok(())
}

fn mempool_event_retention_report_locked(
    store: &RocksDbCanonicalStore,
) -> Result<MempoolEventRetentionReport, CanonicalStoreError> {
    let current_event_sequence = current_mempool_event_sequence(store)?;
    if current_event_sequence == 0 {
        return Ok(MempoolEventRetentionReport::default());
    }
    let oldest_retained_sequence = mempool_event_retention_floor(store, current_event_sequence)?;
    let oldest = read_mempool_event(store, oldest_retained_sequence)?;
    Ok(MempoolEventRetentionReport {
        current_event_sequence,
        oldest_retained_sequence: Some(oldest_retained_sequence),
        oldest_retained_observed_at: Some(UnixTimestampMillis::new(
            oldest.source_observed_unix_millis,
        )),
        retained_event_count: current_event_sequence
            .saturating_sub(oldest_retained_sequence)
            .saturating_add(1),
        pruned_added_count: 0,
        pruned_mined_count: 0,
        pruned_invalidated_count: 0,
    })
}

#[allow(
    unreachable_patterns,
    reason = "MempoolEvent is non-exhaustive outside zinder-store; unrecognized variants do not receive an invented retention policy."
)]
fn retention_window_for(
    event: &MempoolEvent,
    retention: MempoolEventRetentionConfig,
) -> Result<Option<std::time::Duration>, CanonicalStoreError> {
    match event {
        MempoolEvent::Added { .. } => Ok(retention.added_retention),
        MempoolEvent::Mined { .. } => Ok(retention.mined_retention),
        MempoolEvent::Invalidated { .. } => Ok(retention.invalidated_retention),
        _ => Err(CanonicalStoreError::MempoolEventLogInvalid {
            reason: "mempool event variant is unsupported".to_owned(),
        }),
    }
}

fn age_exceeds_window(
    now: UnixTimestampMillis,
    observed_at_millis: u64,
    window: std::time::Duration,
) -> bool {
    let age_millis = now.value().saturating_sub(observed_at_millis);
    let window_millis = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
    age_millis > window_millis
}

#[allow(
    unreachable_patterns,
    reason = "MempoolEvent is non-exhaustive outside zinder-store; unrecognized variants fail closed during retention."
)]
fn increment_pruned_count(
    event: &MempoolEvent,
    added: &mut u64,
    mined: &mut u64,
    invalidated: &mut u64,
) -> Result<(), CanonicalStoreError> {
    match event {
        MempoolEvent::Added { .. } => *added = added.saturating_add(1),
        MempoolEvent::Mined { .. } => *mined = mined.saturating_add(1),
        MempoolEvent::Invalidated { .. } => *invalidated = invalidated.saturating_add(1),
        _ => {
            return Err(CanonicalStoreError::MempoolEventLogInvalid {
                reason: "mempool event variant is unsupported".to_owned(),
            });
        }
    }
    Ok(())
}

fn write_mempool_lifecycle_batch(
    store: &RocksDbCanonicalStore,
    batch: &WriteBatch,
    _operation: &'static str,
) -> Result<(), CanonicalStoreError> {
    let mut options = WriteOptions::default();
    options.disable_wal(false);
    options.set_sync(true);
    store
        .bounded_open
        .db
        .write_opt(batch, &options)
        .map_err(
            |source| CanonicalStoreError::LiveCommitWriteOutcomeUnknown {
                path: store.bounded_open.db.path().to_path_buf(),
                source,
            },
        )
}
