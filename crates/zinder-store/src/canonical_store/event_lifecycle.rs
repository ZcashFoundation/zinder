//! Retained canonical event reads and projection-build lease protection.
//!
//! The canonical event column family is append-only by sequence. Projection
//! builders persist the compact versioned cursor below and retain their build
//! anchor with an expiring lease. Pruning and lease changes share one primary
//! lifecycle lock, so pruning can never pass a live anchor.

use std::num::NonZeroU32;

use rust_rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions};
use zinder_core::{BlockHeight, BlockHeightRange, ChainEpochId, UnixTimestampMillis};

use super::{
    CanonicalEventFence, CanonicalStoreError, CanonicalStoreReadyEvidence, RocksDbCanonicalStore,
    mempool_lifecycle::{MEMPOOL_EVENT_RETENTION_FLOOR_KEY, MEMPOOL_EVENT_SEQUENCE_KEY},
    publication::{canonical_event_created_at, column_family, decode_chain_event},
    rocksdb::{CHAIN_EVENT_COLUMN_FAMILY, STORE_CONTROL_KEY},
};

const CURSOR_VERSION: u8 = 1;
const CURSOR_BYTES: usize = 1 + size_of::<u64>();
const EVENT_VERSION: u8 = 1;
const EVENT_BYTES: usize = 1 + 1 + 8 + 8 + 1 + 4 + 4 + 4 + 4 + 4 + 32 + 8 + 32;
const EVENT_KIND_COMMITTED: u8 = 1;
const EVENT_KIND_REORGED: u8 = 2;
const REVERTED_RANGE_ABSENT: u8 = 0;
const REVERTED_RANGE_PRESENT: u8 = 1;
pub(super) const RETENTION_FLOOR_KEY: &[u8] = b"canonical_event_retention_floor_v1";
pub(super) const PROJECTION_BUILD_LEASE_PREFIX: &[u8] = b"projection_build_lease_v1/";
pub(super) const PROJECTION_BUILD_LEASE_GENERATION_KEY: &[u8] =
    b"projection_build_lease_generation_v1";
const LEASE_VALUE_VERSION: u8 = 2;
const LEASE_VALUE_BYTES: usize = 1 + 8 + 8 + 8 + 8;
/// A projector's four-hour lease fits comfortably below this hard bound.
pub(super) const MAX_PROJECTION_BUILD_LEASE_DURATION_MILLIS: u64 = 24 * 60 * 60 * 1_000;
/// One writer must not retain an unbounded default-column-family lease set.
pub(super) const MAX_LIVE_PROJECTION_BUILD_LEASES: usize = 1_024;

/// Opaque, versioned position persisted by a projection after applying an event.
///
/// The cursor names an exact event sequence. Event reads always resume strictly
/// after it; it is intentionally not a generic range cursor.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CanonicalEventCursor([u8; CURSOR_BYTES]);

impl CanonicalEventCursor {
    /// Creates the v1 persisted cursor for one nonzero event sequence.
    pub fn at(event_sequence: u64) -> Result<Self, CanonicalStoreError> {
        if event_sequence == 0 {
            return Err(CanonicalStoreError::CanonicalEventCursorMalformed {
                reason: "event sequence must be nonzero",
            });
        }
        let mut encoded = [0; CURSOR_BYTES];
        encoded[0] = CURSOR_VERSION;
        encoded[1..].copy_from_slice(&event_sequence.to_be_bytes());
        Ok(Self(encoded))
    }

    /// Decodes the exact bytes persisted by a projection source-position row.
    pub fn from_persisted(bytes: &[u8]) -> Result<Self, CanonicalStoreError> {
        let Some(version) = bytes.first().copied() else {
            return Err(CanonicalStoreError::CanonicalEventCursorMalformed {
                reason: "cursor is empty",
            });
        };
        if version != CURSOR_VERSION {
            return Err(CanonicalStoreError::CanonicalEventCursorUnknownVersion { version });
        }
        let encoded = <[u8; CURSOR_BYTES]>::try_from(bytes).map_err(|_| {
            CanonicalStoreError::CanonicalEventCursorMalformed {
                reason: "cursor has the wrong byte length",
            }
        })?;
        let cursor = Self(encoded);
        if cursor.event_sequence() == 0 {
            return Err(CanonicalStoreError::CanonicalEventCursorMalformed {
                reason: "event sequence must be nonzero",
            });
        }
        Ok(cursor)
    }

    /// Returns the exact stable bytes to persist with a projection transition.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; CURSOR_BYTES] {
        self.0
    }

    /// Returns the event sequence this cursor names.
    #[must_use]
    pub const fn event_sequence(self) -> u64 {
        u64::from_be_bytes([
            self.0[1], self.0[2], self.0[3], self.0[4], self.0[5], self.0[6], self.0[7], self.0[8],
        ])
    }
}

/// Bounded retained canonical-event read request.
#[derive(Clone, Copy, Debug)]
pub struct CanonicalEventHistoryRequest<'cursor> {
    /// Persisted cursor to resume strictly after, or `None` for the retention floor.
    pub from_cursor: Option<&'cursor [u8]>,
    /// Maximum events returned by this page.
    pub max_events: NonZeroU32,
}

impl<'cursor> CanonicalEventHistoryRequest<'cursor> {
    /// Creates a bounded retained-event request.
    #[must_use]
    pub const fn new(from_cursor: Option<&'cursor [u8]>, max_events: NonZeroU32) -> Self {
        Self {
            from_cursor,
            max_events,
        }
    }
}

/// Exact canonical transition kind carried by a retained event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanonicalEventKind {
    /// Canonical history advanced without a replacement.
    Committed,
    /// An unsettled canonical suffix was replaced atomically.
    Reorged,
}

/// One validated retained canonical event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalRetainedEvent {
    cursor: CanonicalEventCursor,
    resulting_epoch_id: ChainEpochId,
    previous_epoch_id: Option<ChainEpochId>,
    kind: CanonicalEventKind,
    reverted_range: Option<BlockHeightRange>,
    committed_range: BlockHeightRange,
    resulting_fence: CanonicalEventFence,
}

impl CanonicalRetainedEvent {
    /// Cursor persisted after this transition has been durably applied.
    #[must_use]
    pub const fn cursor(self) -> CanonicalEventCursor {
        self.cursor
    }

    /// Canonical epoch made visible by this transition.
    #[must_use]
    pub const fn resulting_epoch_id(self) -> ChainEpochId {
        self.resulting_epoch_id
    }

    /// Previous visible epoch, absent only for the baseline event.
    #[must_use]
    pub const fn previous_epoch_id(self) -> Option<ChainEpochId> {
        self.previous_epoch_id
    }

    /// Transition category.
    #[must_use]
    pub const fn kind(self) -> CanonicalEventKind {
        self.kind
    }

    /// Inclusive range reverted by a reorg transition.
    #[must_use]
    pub const fn reverted_range(self) -> Option<BlockHeightRange> {
        self.reverted_range
    }

    /// Inclusive or anchored-empty range committed by this transition.
    #[must_use]
    pub const fn committed_range(self) -> BlockHeightRange {
        self.committed_range
    }

    /// Exact authenticated canonical fence produced by this retained event.
    #[must_use]
    pub const fn resulting_fence(self) -> CanonicalEventFence {
        self.resulting_fence
    }

    /// Creates the lease anchor that protects this retained event.
    #[must_use]
    pub const fn projection_build_anchor(self) -> ProjectionBuildAnchor {
        ProjectionBuildAnchor::new(self.resulting_epoch_id, self.cursor)
    }
}

/// Canonical event and epoch pinned by a projection construction generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionBuildAnchor {
    chain_epoch_id: ChainEpochId,
    event_cursor: CanonicalEventCursor,
}

impl ProjectionBuildAnchor {
    /// Creates an anchor. Lease acquisition verifies it against retained canonical history.
    #[must_use]
    pub const fn new(chain_epoch_id: ChainEpochId, event_cursor: CanonicalEventCursor) -> Self {
        Self {
            chain_epoch_id,
            event_cursor,
        }
    }

    /// Returns the pinned canonical epoch.
    #[must_use]
    pub const fn chain_epoch_id(self) -> ChainEpochId {
        self.chain_epoch_id
    }

    /// Returns the retained event that produced the pinned epoch.
    #[must_use]
    pub const fn event_cursor(self) -> CanonicalEventCursor {
        self.event_cursor
    }
}

/// Opaque durable identity for one projection construction generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProjectionBuildLeaseId([u8; 16]);

impl ProjectionBuildLeaseId {
    /// Creates an opaque projection-build generation identity.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the exact stable identity bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Durable lease that prevents pruning through one construction anchor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionBuildLease {
    id: ProjectionBuildLeaseId,
    generation: u64,
    anchor: ProjectionBuildAnchor,
    expires_at: UnixTimestampMillis,
}

impl ProjectionBuildLease {
    /// Creates a durable projection-build lease request.
    #[must_use]
    pub const fn new(
        id: ProjectionBuildLeaseId,
        anchor: ProjectionBuildAnchor,
        expires_at: UnixTimestampMillis,
    ) -> Self {
        Self {
            id,
            generation: 0,
            anchor,
            expires_at,
        }
    }

    /// Attaches the opaque durable generation returned by the canonical writer.
    #[must_use]
    pub const fn with_generation(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }

    /// Returns the construction-generation identity.
    #[must_use]
    pub const fn id(self) -> ProjectionBuildLeaseId {
        self.id
    }

    /// Returns the durable generation assigned atomically by acquisition.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Returns the canonical event and epoch protected by this lease.
    #[must_use]
    pub const fn anchor(self) -> ProjectionBuildAnchor {
        self.anchor
    }

    /// Returns the exclusive lease expiry.
    #[must_use]
    pub const fn expires_at(self) -> UnixTimestampMillis {
        self.expires_at
    }
}

/// Retained-event state after a pruning pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalEventRetentionReport {
    /// Latest durable event sequence at the time of the pass.
    pub current_event_sequence: u64,
    /// Inclusive first retained event sequence after the pass.
    pub oldest_retained_sequence: u64,
    /// Number of event rows pruned by the pass.
    pub pruned_event_count: u64,
    /// Earliest still-live lease anchor that limited this pass, if any.
    pub lease_protected_anchor: Option<ProjectionBuildAnchor>,
}

impl RocksDbCanonicalStore {
    /// Reads retained canonical events in exact sequence order.
    ///
    /// A supplied cursor resumes strictly after the persisted sequence it
    /// names. The reader rejects malformed cursor encodings, unknown cursor
    /// versions, expired positions, unknown event versions, and malformed
    /// event ranges without manufacturing a replacement transition.
    pub fn canonical_event_history(
        &self,
        request: CanonicalEventHistoryRequest<'_>,
    ) -> Result<Vec<CanonicalRetainedEvent>, CanonicalStoreError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        canonical_event_history_from_db(
            &self.bounded_open.db,
            self.ready_evidence.visible_event_sequence,
            request,
        )
    }

    /// Returns the inclusive retained-event floor without mutating state.
    pub fn canonical_event_retention_floor(&self) -> Result<u64, CanonicalStoreError> {
        canonical_event_retention_floor_from_db(
            &self.bounded_open.db,
            self.ready_evidence.visible_event_sequence,
        )
    }

    /// Acquires a durable lease for one inactive projection generation.
    ///
    /// The exact anchored event must still be retained and must produce the
    /// supplied epoch. A live lease with the same generation identity cannot
    /// be acquired by a competing builder; renewal is a distinct operation.
    pub fn acquire_projection_build_lease(
        &self,
        lease: ProjectionBuildLease,
        now: UnixTimestampMillis,
    ) -> Result<ProjectionBuildLease, CanonicalStoreError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        validate_live_lease_expiry(lease, now)?;
        let (live_leases, expired_lease_ids) = self.live_projection_build_lease_state(now)?;
        if read_projection_build_lease(self, lease.id)?
            .is_some_and(|existing| existing.expires_at > now)
        {
            return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
                reason: "lease identity is already held by a live builder",
            });
        }
        if live_leases.len() >= MAX_LIVE_PROJECTION_BUILD_LEASES {
            return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
                reason: "live projection build lease capacity is exhausted",
            });
        }
        validate_projection_build_anchor(self, lease.anchor)?;
        let generation = next_projection_build_lease_generation(self)?;
        let lease = lease.with_generation(generation);
        let mut batch = WriteBatch::default();
        for lease_id in expired_lease_ids {
            batch.delete(projection_build_lease_key(lease_id));
        }
        batch.put(
            PROJECTION_BUILD_LEASE_GENERATION_KEY,
            generation.to_be_bytes(),
        );
        batch.put(
            projection_build_lease_key(lease.id),
            encode_projection_build_lease(lease),
        );
        write_lifecycle_batch(self, &batch, "projection build lease acquire")?;
        Ok(lease)
    }

    /// Renews a live lease without changing the protected construction anchor.
    pub fn renew_projection_build_lease(
        &self,
        lease: ProjectionBuildLease,
        now: UnixTimestampMillis,
    ) -> Result<ProjectionBuildLease, CanonicalStoreError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        validate_live_lease_expiry(lease, now)?;
        let Some(existing) = read_projection_build_lease(self, lease.id)? else {
            return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
                reason: "lease identity is not held",
            });
        };
        if existing.expires_at <= now {
            return Err(CanonicalStoreError::ProjectionBuildLeaseExpired);
        }
        if existing.anchor != lease.anchor {
            return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
                reason: "lease renewal cannot change its canonical anchor",
            });
        }
        if existing.generation != lease.generation {
            return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
                reason: "lease renewal generation does not match the live lease",
            });
        }
        validate_projection_build_anchor(self, lease.anchor)?;
        write_projection_build_lease(self, lease)?;
        Ok(lease)
    }

    /// Releases a construction lease after promotion or abandoned construction.
    pub fn release_projection_build_lease(
        &self,
        lease: ProjectionBuildLease,
    ) -> Result<(), CanonicalStoreError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        let Some(existing) = read_projection_build_lease(self, lease.id)? else {
            return Ok(());
        };
        if existing.generation != lease.generation {
            return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
                reason: "lease release generation does not match the live lease",
            });
        }
        let mut batch = WriteBatch::default();
        batch.delete(projection_build_lease_key(lease.id));
        write_lifecycle_batch(self, &batch, "projection build lease release")
    }

    /// Returns a lease only while it remains live at `now`.
    pub fn active_projection_build_lease(
        &self,
        lease_id: ProjectionBuildLeaseId,
        now: UnixTimestampMillis,
    ) -> Result<Option<ProjectionBuildLease>, CanonicalStoreError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        Ok(read_projection_build_lease(self, lease_id)?.filter(|lease| lease.expires_at > now))
    }

    /// Prunes events before `requested_oldest_sequence`, respecting every live lease anchor.
    ///
    /// The requested floor is inclusive: pruning to `N` deletes exactly rows
    /// below `N`, preserves `N`, and never removes the latest event. Expired
    /// leases are discarded atomically with the pass; malformed live leases
    /// fail closed rather than weakening retention.
    pub fn prune_canonical_events_before(
        &self,
        requested_oldest_sequence: u64,
        now: UnixTimestampMillis,
    ) -> Result<CanonicalEventRetentionReport, CanonicalStoreError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        self.prune_canonical_events_before_locked(requested_oldest_sequence, now)
    }

    fn prune_canonical_events_before_locked(
        &self,
        requested_oldest_sequence: u64,
        now: UnixTimestampMillis,
    ) -> Result<CanonicalEventRetentionReport, CanonicalStoreError> {
        let current_event_sequence = self.ready_evidence.visible_event_sequence;
        let oldest_retained_sequence = self.canonical_event_retention_floor()?;
        if requested_oldest_sequence < oldest_retained_sequence
            || requested_oldest_sequence > current_event_sequence
        {
            return Err(CanonicalStoreError::CanonicalEventCursorMalformed {
                reason: "requested retention floor is outside retained event history",
            });
        }

        let (live_leases, expired_lease_ids) = self.live_projection_build_lease_state(now)?;
        let earliest_live_anchor = live_leases
            .iter()
            .map(|lease| lease.anchor)
            .min_by_key(|anchor| anchor.event_cursor.event_sequence());
        let retained_floor = earliest_live_anchor.map_or(requested_oldest_sequence, |anchor| {
            requested_oldest_sequence.min(anchor.event_cursor.event_sequence())
        });
        let mut batch = WriteBatch::default();
        let event_family = column_family(&self.bounded_open.db, CHAIN_EVENT_COLUMN_FAMILY)?;
        for event_sequence in oldest_retained_sequence..retained_floor {
            if event_sequence != 1 {
                batch.delete_cf(&event_family, event_sequence.to_be_bytes());
            }
        }
        // Epoch records are permanent cursor witnesses. A pruned event cursor
        // can still require the exact historical epoch whose branch is being
        // reverted; projecting the current epoch onto that range would lie
        // about its tip and settlement boundary.
        if retained_floor != oldest_retained_sequence {
            batch.put(RETENTION_FLOOR_KEY, retained_floor.to_be_bytes());
        }
        for lease_id in expired_lease_ids {
            batch.delete(projection_build_lease_key(lease_id));
        }
        if !batch.is_empty() {
            write_lifecycle_batch(self, &batch, "canonical event prune")?;
        }
        let first_deletable_sequence = oldest_retained_sequence.max(2);
        Ok(CanonicalEventRetentionReport {
            current_event_sequence,
            oldest_retained_sequence: retained_floor,
            pruned_event_count: retained_floor.saturating_sub(first_deletable_sequence),
            lease_protected_anchor: earliest_live_anchor,
        })
    }

    /// Prunes retained events older than `cutoff` using their resulting epoch timestamps.
    pub fn prune_canonical_events_before_created_at(
        &self,
        cutoff: UnixTimestampMillis,
        now: UnixTimestampMillis,
    ) -> Result<CanonicalEventRetentionReport, CanonicalStoreError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        let current = self.ready_evidence.visible_event_sequence;
        let floor = self.canonical_event_retention_floor()?;
        let mut requested_floor = current;
        for event_sequence in floor..=current {
            if canonical_event_created_at(&self.bounded_open.db, event_sequence)? >= cutoff {
                requested_floor = event_sequence;
                break;
            }
        }
        self.prune_canonical_events_before_locked(requested_floor, now)
    }

    fn live_projection_build_lease_state(
        &self,
        now: UnixTimestampMillis,
    ) -> Result<(Vec<ProjectionBuildLease>, Vec<ProjectionBuildLeaseId>), CanonicalStoreError> {
        let mut live_leases = Vec::new();
        let mut expired_lease_ids = Vec::new();
        let iterator = self.bounded_open.db.iterator(IteratorMode::From(
            PROJECTION_BUILD_LEASE_PREFIX,
            Direction::Forward,
        ));
        for row in iterator {
            let (key, lease_bytes) =
                row.map_err(|source| CanonicalStoreError::RocksDbOperation {
                    operation: "projection build lease scan",
                    source,
                })?;
            if !key.starts_with(PROJECTION_BUILD_LEASE_PREFIX) {
                break;
            }
            let lease_id = projection_build_lease_id_from_key(&key)?;
            let lease = decode_projection_build_lease(lease_id, &lease_bytes)?;
            if lease.expires_at <= now {
                expired_lease_ids.push(lease_id);
                continue;
            }
            validate_live_lease_expiry(lease, now)?;
            validate_projection_build_anchor(self, lease.anchor)?;
            live_leases.push(lease);
        }
        Ok((live_leases, expired_lease_ids))
    }
}

pub(super) fn canonical_event_history_from_db(
    db: &rust_rocksdb::DB,
    current_event_sequence: u64,
    request: CanonicalEventHistoryRequest<'_>,
) -> Result<Vec<CanonicalRetainedEvent>, CanonicalStoreError> {
    let oldest_retained_sequence =
        canonical_event_retention_floor_from_db(db, current_event_sequence)?;
    let start_sequence = match request.from_cursor {
        None => oldest_retained_sequence,
        Some(encoded_cursor) => {
            let cursor = CanonicalEventCursor::from_persisted(encoded_cursor)?;
            let event_sequence = cursor.event_sequence();
            if event_sequence > current_event_sequence {
                return Err(CanonicalStoreError::CanonicalEventCursorMalformed {
                    reason: "cursor sequence is ahead of canonical history",
                });
            }
            let next_sequence = event_sequence.checked_add(1).ok_or(
                CanonicalStoreError::CanonicalEventCursorMalformed {
                    reason: "cursor sequence cannot advance",
                },
            )?;
            if next_sequence < oldest_retained_sequence {
                return Err(CanonicalStoreError::CanonicalEventCursorExpired {
                    event_sequence,
                    oldest_retained_sequence,
                });
            }
            next_sequence
        }
    };
    if start_sequence > current_event_sequence {
        return Ok(Vec::new());
    }

    let max_events = usize::try_from(request.max_events.get()).map_err(|_| {
        CanonicalStoreError::CanonicalEventCursorMalformed {
            reason: "max event count cannot fit this platform",
        }
    })?;
    let mut events = Vec::with_capacity(max_events);
    let mut event_sequence = start_sequence;
    while event_sequence <= current_event_sequence && events.len() < max_events {
        events.push(read_retained_event_from_db(db, event_sequence)?);
        event_sequence = event_sequence.checked_add(1).ok_or(
            CanonicalStoreError::CanonicalEventCursorMalformed {
                reason: "event sequence cannot advance",
            },
        )?;
    }
    Ok(events)
}

pub(super) fn canonical_event_retention_floor_from_db(
    db: &rust_rocksdb::DB,
    current_event_sequence: u64,
) -> Result<u64, CanonicalStoreError> {
    let floor_bytes =
        db.get(RETENTION_FLOOR_KEY)
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "canonical event retention floor read",
                source,
            })?;
    let Some(floor_bytes) = floor_bytes else {
        return Ok(1);
    };
    let encoded = <[u8; 8]>::try_from(floor_bytes.as_slice()).map_err(|_| {
        CanonicalStoreError::CanonicalEventRecordMalformed {
            event_sequence: current_event_sequence,
            reason: "retention floor has the wrong byte length",
        }
    })?;
    let floor = u64::from_be_bytes(encoded);
    if floor == 0 || floor > current_event_sequence {
        return Err(CanonicalStoreError::CanonicalEventRecordMalformed {
            event_sequence: current_event_sequence,
            reason: "retention floor is outside canonical event history",
        });
    }
    Ok(floor)
}

pub(super) fn read_retained_event_from_db(
    db: &rust_rocksdb::DB,
    event_sequence: u64,
) -> Result<CanonicalRetainedEvent, CanonicalStoreError> {
    let family = column_family(db, CHAIN_EVENT_COLUMN_FAMILY)?;
    let encoded = db
        .get_cf(&family, event_sequence.to_be_bytes())
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "canonical retained event read",
            source,
        })?
        .ok_or(CanonicalStoreError::CanonicalEventRecordMalformed {
            event_sequence,
            reason: "retained event row is absent",
        })?;
    validate_event_record_shape(event_sequence, &encoded)?;
    let decoded = decode_chain_event(&encoded).map_err(|_| {
        CanonicalStoreError::CanonicalEventRecordMalformed {
            event_sequence,
            reason: "event record does not satisfy the version-1 transition contract",
        }
    })?;
    if decoded.resulting_epoch_id.value() != event_sequence
        || decoded.previous_epoch_id != event_sequence.saturating_sub(1)
    {
        return Err(CanonicalStoreError::CanonicalEventRecordMalformed {
            event_sequence,
            reason: "event sequence and epoch transition disagree",
        });
    }
    let kind = match decoded.kind {
        EVENT_KIND_COMMITTED => CanonicalEventKind::Committed,
        EVENT_KIND_REORGED => CanonicalEventKind::Reorged,
        _ => {
            return Err(CanonicalStoreError::CanonicalEventRecordMalformed {
                event_sequence,
                reason: "event kind is unsupported",
            });
        }
    };
    Ok(CanonicalRetainedEvent {
        cursor: CanonicalEventCursor::at(event_sequence)?,
        resulting_epoch_id: decoded.resulting_epoch_id,
        previous_epoch_id: (decoded.previous_epoch_id != 0)
            .then_some(ChainEpochId::new(decoded.previous_epoch_id)),
        kind,
        reverted_range: decoded.reverted_range,
        committed_range: decoded.committed_range,
        resulting_fence: CanonicalEventFence::from_persisted_event(
            decoded.resulting_epoch_id,
            event_sequence,
            decoded.visible_tip,
            decoded.visible_block_count,
            decoded.visible_sequence_digest,
        ),
    })
}

fn validate_event_record_shape(
    event_sequence: u64,
    encoded: &[u8],
) -> Result<(), CanonicalStoreError> {
    if encoded.len() != EVENT_BYTES {
        return Err(CanonicalStoreError::CanonicalEventRecordMalformed {
            event_sequence,
            reason: "event record has the wrong byte length",
        });
    }
    if encoded[0] != EVENT_VERSION {
        return Err(CanonicalStoreError::CanonicalEventVersionUnsupported {
            event_sequence,
            version: encoded[0],
        });
    }
    let kind = encoded[1];
    let previous_epoch_id = u64::from_le_bytes(encoded[10..18].try_into().map_err(|_| {
        CanonicalStoreError::CanonicalEventRecordMalformed {
            event_sequence,
            reason: "previous epoch has the wrong byte length",
        }
    })?);
    let reverted_range = match encoded[18] {
        REVERTED_RANGE_ABSENT => {
            if encoded[19..27].iter().any(|byte| *byte != 0) {
                return Err(CanonicalStoreError::CanonicalEventRecordMalformed {
                    event_sequence,
                    reason: "absent reverted range contains heights",
                });
            }
            None
        }
        REVERTED_RANGE_PRESENT => Some(validate_event_range(event_sequence, encoded, 19, false)?),
        _ => {
            return Err(CanonicalStoreError::CanonicalEventRecordMalformed {
                event_sequence,
                reason: "reverted range presence is unknown",
            });
        }
    };
    let _committed_range = validate_event_range(event_sequence, encoded, 27, true)?;
    match kind {
        EVENT_KIND_COMMITTED if reverted_range.is_none() => Ok(()),
        EVENT_KIND_REORGED if previous_epoch_id > 0 && reverted_range.is_some() => Ok(()),
        _ => Err(CanonicalStoreError::CanonicalEventRecordMalformed {
            event_sequence,
            reason: "event kind and range presence disagree",
        }),
    }
}

fn validate_event_range(
    event_sequence: u64,
    encoded: &[u8],
    offset: usize,
    allow_anchored_empty: bool,
) -> Result<BlockHeightRange, CanonicalStoreError> {
    let start = BlockHeight::new(u32::from_le_bytes(
        encoded[offset..offset + 4].try_into().map_err(|_| {
            CanonicalStoreError::CanonicalEventRecordMalformed {
                event_sequence,
                reason: "event range start has the wrong byte length",
            }
        })?,
    ));
    let end = BlockHeight::new(u32::from_le_bytes(
        encoded[offset + 4..offset + 8].try_into().map_err(|_| {
            CanonicalStoreError::CanonicalEventRecordMalformed {
                event_sequence,
                reason: "event range end has the wrong byte length",
            }
        })?,
    ));
    if start <= end || (allow_anchored_empty && end.next() == Some(start)) {
        return Ok(BlockHeightRange::inclusive(start, end));
    }
    Err(CanonicalStoreError::CanonicalEventRecordMalformed {
        event_sequence,
        reason: "event range is neither inclusive nor anchored empty",
    })
}

fn validate_live_lease_expiry(
    lease: ProjectionBuildLease,
    now: UnixTimestampMillis,
) -> Result<(), CanonicalStoreError> {
    if lease.expires_at <= now {
        return Err(CanonicalStoreError::ProjectionBuildLeaseExpired);
    }
    if lease.expires_at.value().saturating_sub(now.value())
        > MAX_PROJECTION_BUILD_LEASE_DURATION_MILLIS
    {
        return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "lease expiry exceeds the maximum duration",
        });
    }
    Ok(())
}

fn validate_projection_build_anchor(
    store: &RocksDbCanonicalStore,
    anchor: ProjectionBuildAnchor,
) -> Result<(), CanonicalStoreError> {
    let floor = store.canonical_event_retention_floor()?;
    validate_projection_build_anchor_from_db(
        &store.bounded_open.db,
        store.ready_evidence.visible_event_sequence,
        floor,
        anchor,
    )
}

fn validate_projection_build_anchor_from_db(
    db: &rust_rocksdb::DB,
    current_event_sequence: u64,
    retention_floor: u64,
    anchor: ProjectionBuildAnchor,
) -> Result<(), CanonicalStoreError> {
    let event_sequence = anchor.event_cursor.event_sequence();
    if event_sequence < retention_floor || event_sequence > current_event_sequence {
        return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "anchor event is outside retained canonical history",
        });
    }
    let event = read_retained_event_from_db(db, event_sequence)?;
    if event.resulting_epoch_id != anchor.chain_epoch_id {
        return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "anchor event does not produce the pinned canonical epoch",
        });
    }
    Ok(())
}

/// Validates lifecycle rows that share the default column family with store control.
///
/// Admission is intentionally strict: only the singleton retention floor and exact
/// version-1 lease keys may accompany store control. Every persisted value is decoded,
/// and an unexpired lease must still pin an exact retained canonical event.
pub(super) fn validate_projection_lifecycle_records(
    db: &rust_rocksdb::DB,
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> Result<(), CanonicalStoreError> {
    let retention_floor =
        canonical_event_retention_floor_from_db(db, ready_evidence.visible_event_sequence)?;
    let generation = db
        .get(PROJECTION_BUILD_LEASE_GENERATION_KEY)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "projection lifecycle generation admission read",
            source,
        })?;
    let generation = generation
        .as_deref()
        .map(decode_projection_build_lease_generation)
        .transpose()?;
    let now = UnixTimestampMillis::now();
    let mut live_lease_count = 0_usize;
    for row in db.iterator(IteratorMode::Start) {
        let (key, encoded) = row.map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "projection lifecycle admission scan",
            source,
        })?;
        match key.as_ref() {
            STORE_CONTROL_KEY
            | RETENTION_FLOOR_KEY
            | PROJECTION_BUILD_LEASE_GENERATION_KEY
            | MEMPOOL_EVENT_SEQUENCE_KEY
            | MEMPOOL_EVENT_RETENTION_FLOOR_KEY => {}
            key if key.starts_with(super::activation_schedule::ACTIVATION_SCHEDULE_KEY_PREFIX) => {}
            lease_key if is_projection_build_lease_key(lease_key) => {
                let lease_id = projection_build_lease_id_from_key(&key)?;
                let lease = decode_projection_build_lease(lease_id, &encoded)?;
                if generation.is_none_or(|generation| lease.generation > generation) {
                    return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
                        reason: "lease generation exceeds the generation singleton",
                    });
                }
                if lease.expires_at > now {
                    validate_live_lease_expiry(lease, now)?;
                    live_lease_count = live_lease_count.saturating_add(1);
                    if live_lease_count > MAX_LIVE_PROJECTION_BUILD_LEASES {
                        return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
                            reason: "live projection build lease capacity is exceeded",
                        });
                    }
                    validate_projection_build_anchor_from_db(
                        db,
                        ready_evidence.visible_event_sequence,
                        retention_floor,
                        lease.anchor,
                    )?;
                }
            }
            _ => {
                return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
                    reason: "unexpected lifecycle key escaped store admission",
                });
            }
        }
    }
    Ok(())
}

fn read_projection_build_lease(
    store: &RocksDbCanonicalStore,
    lease_id: ProjectionBuildLeaseId,
) -> Result<Option<ProjectionBuildLease>, CanonicalStoreError> {
    let lease_bytes = store
        .bounded_open
        .db
        .get(projection_build_lease_key(lease_id))
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "projection build lease read",
            source,
        })?;
    lease_bytes
        .as_deref()
        .map(|encoded| decode_projection_build_lease(lease_id, encoded))
        .transpose()
}

fn write_projection_build_lease(
    store: &RocksDbCanonicalStore,
    lease: ProjectionBuildLease,
) -> Result<(), CanonicalStoreError> {
    let mut batch = WriteBatch::default();
    batch.put(
        projection_build_lease_key(lease.id),
        encode_projection_build_lease(lease),
    );
    write_lifecycle_batch(store, &batch, "projection build lease write")
}

fn write_lifecycle_batch(
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

#[cfg(test)]
pub(super) fn seed_projection_build_leases_for_capacity_test(
    store: &RocksDbCanonicalStore,
    leases: &[ProjectionBuildLease],
) -> Result<(), CanonicalStoreError> {
    let _lifecycle_guard = store.lifecycle_lock.lock();
    let mut batch = WriteBatch::default();
    for lease in leases {
        batch.put(
            projection_build_lease_key(lease.id),
            encode_projection_build_lease(*lease),
        );
    }
    write_lifecycle_batch(store, &batch, "projection build lease capacity test seed")
}

fn projection_build_lease_key(lease_id: ProjectionBuildLeaseId) -> Vec<u8> {
    let mut key = Vec::with_capacity(PROJECTION_BUILD_LEASE_PREFIX.len() + 16);
    key.extend_from_slice(PROJECTION_BUILD_LEASE_PREFIX);
    key.extend_from_slice(&lease_id.as_bytes());
    key
}

pub(super) fn is_projection_build_lease_key(key: &[u8]) -> bool {
    key.strip_prefix(PROJECTION_BUILD_LEASE_PREFIX)
        .is_some_and(|identity| identity.len() == 16)
}

fn projection_build_lease_id_from_key(
    key: &[u8],
) -> Result<ProjectionBuildLeaseId, CanonicalStoreError> {
    let Some(id_bytes) = key.strip_prefix(PROJECTION_BUILD_LEASE_PREFIX) else {
        return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "lease key lacks the v1 prefix",
        });
    };
    let id_bytes = <[u8; 16]>::try_from(id_bytes).map_err(|_| {
        CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "lease key has the wrong identity length",
        }
    })?;
    Ok(ProjectionBuildLeaseId::from_bytes(id_bytes))
}

fn encode_projection_build_lease(lease: ProjectionBuildLease) -> [u8; LEASE_VALUE_BYTES] {
    let mut encoded = [0; LEASE_VALUE_BYTES];
    encoded[0] = LEASE_VALUE_VERSION;
    encoded[1..9].copy_from_slice(&lease.generation.to_be_bytes());
    encoded[9..17].copy_from_slice(&lease.anchor.chain_epoch_id.value().to_be_bytes());
    encoded[17..25].copy_from_slice(&lease.anchor.event_cursor.event_sequence().to_be_bytes());
    encoded[25..33].copy_from_slice(&lease.expires_at.value().to_be_bytes());
    encoded
}

fn decode_projection_build_lease(
    lease_id: ProjectionBuildLeaseId,
    encoded: &[u8],
) -> Result<ProjectionBuildLease, CanonicalStoreError> {
    if encoded.len() != LEASE_VALUE_BYTES {
        return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "lease value has the wrong byte length",
        });
    }
    if encoded[0] != LEASE_VALUE_VERSION {
        return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "lease value version is unsupported",
        });
    }
    let generation = u64::from_be_bytes(encoded[1..9].try_into().map_err(|_| {
        CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "lease generation has the wrong byte length",
        }
    })?);
    if generation == 0 {
        return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "lease generation must be nonzero",
        });
    }
    let epoch_id = ChainEpochId::new(u64::from_be_bytes(encoded[9..17].try_into().map_err(
        |_| CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "lease epoch has the wrong byte length",
        },
    )?));
    let event_sequence = u64::from_be_bytes(encoded[17..25].try_into().map_err(|_| {
        CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "lease event sequence has the wrong byte length",
        }
    })?);
    let expires_at =
        UnixTimestampMillis::new(u64::from_be_bytes(encoded[25..33].try_into().map_err(
            |_| CanonicalStoreError::ProjectionBuildLeaseInvalid {
                reason: "lease expiry has the wrong byte length",
            },
        )?));
    let cursor = CanonicalEventCursor::at(event_sequence)?;
    Ok(ProjectionBuildLease::new(
        lease_id,
        ProjectionBuildAnchor::new(epoch_id, cursor),
        expires_at,
    )
    .with_generation(generation))
}

fn next_projection_build_lease_generation(
    store: &RocksDbCanonicalStore,
) -> Result<u64, CanonicalStoreError> {
    let current = store
        .bounded_open
        .db
        .get(PROJECTION_BUILD_LEASE_GENERATION_KEY)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "projection build lease generation read",
            source,
        })?
        .map(|encoded| decode_projection_build_lease_generation(&encoded))
        .transpose()?
        .unwrap_or(0);
    current
        .checked_add(1)
        .ok_or(CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "projection build lease generation overflow",
        })
}

fn decode_projection_build_lease_generation(encoded: &[u8]) -> Result<u64, CanonicalStoreError> {
    let generation = u64::from_be_bytes(encoded.try_into().map_err(|_| {
        CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "lease generation singleton has the wrong byte length",
        }
    })?);
    if generation == 0 {
        return Err(CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "lease generation singleton must be nonzero",
        });
    }
    Ok(generation)
}
