//! Serialized canonical writer control plane.
//!
//! The gRPC adapter owns no `RocksDbCanonicalStore`. It sends bounded commands
//! to the one follower task that owns the primary handle, so projectors cannot
//! accidentally open a second primary merely to read retained events or manage
//! a construction lease.

use std::{
    fs,
    num::NonZeroU32,
    path::{Path, PathBuf},
    time::Duration,
};

use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status, service::interceptor::InterceptedService};
use zinder_core::{
    BlockHeightRange, ChainEpoch, ShieldedProtocol, UnixTimestampMillis,
    wire::encode_zinder_native_chain_name,
};
use zinder_proto::v1::ingest::{
    AcquireCanonicalProjectionBuildLeaseRequest, CanonicalCheckpointBlockId,
    CanonicalCheckpointFrontier, CanonicalCheckpointHistoryPredecessor,
    CanonicalCheckpointSequenceEvidence, CanonicalEventBlockRange, CanonicalEventPageRequest,
    CanonicalEventPageResponse, CanonicalOwnerCheckpointBuildPlanEvidence,
    CanonicalOwnerCheckpointReadyEvidence, CanonicalProjectionBuildLease,
    CanonicalProjectionBuildLeaseResponse, CanonicalRetainedEvent, CanonicalRetainedEventKind,
    CanonicalWriterFence, CanonicalWriterStatusRequest, CanonicalWriterStatusResponse,
    CreateCanonicalOwnerCheckpointRequest, CreateCanonicalOwnerCheckpointResponse,
    ReadmitCanonicalOwnerCheckpointRequest, ReleaseCanonicalProjectionBuildLeaseRequest,
    ReleaseCanonicalProjectionBuildLeaseResponse, RenewCanonicalProjectionBuildLeaseRequest,
    canonical_control_server::{CanonicalControl, CanonicalControlServer},
};
use zinder_runtime::{BearerToken, BearerTokenServerInterceptor};
use zinder_store::{
    CanonicalEventCursor, CanonicalEventFence, CanonicalEventHistoryRequest, CanonicalEventKind,
    CanonicalMempoolSnapshotStart, CanonicalOwnerCheckpointAdmission,
    CanonicalOwnerCheckpointEvidence, CanonicalStoreError, EventStreamStartPosition, MempoolEvent,
    MempoolEventEnvelope, MempoolEventHistoryRequest, MempoolEventPosition,
    MempoolEventRetentionConfig, MempoolEventRetentionStepBudget, MempoolEventRetentionStepOutcome,
    ProjectionBuildAnchor, ProjectionBuildLease, ProjectionBuildLeaseId, RocksDbCanonicalStore,
    RocksDbResourceBudget, StreamCursorTokenV1,
};

/// Bounded number of RPCs that may wait while the follower is preparing source work.
pub const CANONICAL_CONTROL_COMMAND_CAPACITY: usize = 64;
const CANONICAL_CONTROL_MAX_PAGE_EVENTS: u32 = 1_024;

/// Scheduling decision returned after one owner command is applied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CanonicalControlScheduling {
    /// Continue draining already queued control commands.
    ContinueDraining,
    /// Give canonical source work one turn before another maintenance step.
    YieldToCanonical,
}
const CANONICAL_CONTROL_SEND_TIMEOUT: Duration = Duration::from_secs(2);
/// Covers one bounded upstream request plus a command handoff without making
/// projectors reopen the primary during a transient follower busy period.
const CANONICAL_CONTROL_REPLY_TIMEOUT: Duration = Duration::from_mins(1);

/// One configured directory below which the canonical owner may stage
/// checkpoint candidates.
///
/// The control RPC accepts an opaque identifier, never a path. Keeping path
/// resolution in this private type prevents a control client from choosing an
/// arbitrary filesystem target.
#[derive(Clone, Debug)]
pub struct CanonicalCheckpointStagingRoot {
    path: PathBuf,
}

impl CanonicalCheckpointStagingRoot {
    /// Captures the configured checkpoint staging root. The directory is
    /// admitted immediately before each owner checkpoint so a missing or
    /// replaced root fails closed without creating a target.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn resolve_candidate(
        &self,
        candidate_id: &str,
        expected_root_binding: &[u8],
    ) -> Result<PathBuf, Status> {
        let candidate = self.resolve_candidate_directory(candidate_id, expected_root_binding)?;
        let mut entries = fs::read_dir(&candidate).map_err(|_| {
            Status::failed_precondition("canonical checkpoint candidate directory is unavailable")
        })?;
        if entries
            .next()
            .transpose()
            .map_err(|_| {
                Status::failed_precondition(
                    "canonical checkpoint candidate directory is unavailable",
                )
            })?
            .is_some()
        {
            return Err(Status::already_exists(
                "canonical checkpoint candidate contains an existing entry",
            ));
        }
        let target = candidate.join("canonical.rocksdb");
        match fs::symlink_metadata(&target) {
            Ok(_) => Err(Status::already_exists(
                "canonical checkpoint candidate already has a canonical.rocksdb entry",
            )),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(target),
            Err(_) => Err(Status::failed_precondition(
                "canonical checkpoint target is unavailable",
            )),
        }
    }

    fn resolve_existing_checkpoint(
        &self,
        candidate_id: &str,
        expected_root_binding: &[u8],
    ) -> Result<PathBuf, Status> {
        let candidate = self.resolve_candidate_directory(candidate_id, expected_root_binding)?;
        let mut entries = fs::read_dir(&candidate).map_err(|_| {
            Status::failed_precondition("canonical checkpoint candidate directory is unavailable")
        })?;
        let entry = entries
            .next()
            .transpose()
            .map_err(|_| {
                Status::failed_precondition(
                    "canonical checkpoint candidate directory is unavailable",
                )
            })?
            .ok_or_else(|| {
                Status::failed_precondition(
                    "canonical checkpoint candidate has no canonical checkpoint",
                )
            })?;
        if entries
            .next()
            .transpose()
            .map_err(|_| {
                Status::failed_precondition(
                    "canonical checkpoint candidate directory is unavailable",
                )
            })?
            .is_some()
            || entry.file_name() != "canonical.rocksdb"
        {
            return Err(Status::failed_precondition(
                "canonical checkpoint candidate must contain only canonical.rocksdb",
            ));
        }
        let target = entry.path();
        let metadata = fs::symlink_metadata(&target).map_err(|_| {
            Status::failed_precondition("canonical checkpoint target is unavailable")
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(Status::failed_precondition(
                "canonical checkpoint target must be an existing directory",
            ));
        }
        let target = fs::canonicalize(&target).map_err(|_| {
            Status::failed_precondition("canonical checkpoint target is unavailable")
        })?;
        let admitted_candidate = fs::canonicalize(&candidate).map_err(|_| {
            Status::failed_precondition("canonical checkpoint candidate directory is unavailable")
        })?;
        if target.parent() != Some(admitted_candidate.as_path()) {
            return Err(Status::failed_precondition(
                "canonical checkpoint target is outside its admitted candidate",
            ));
        }
        Ok(target)
    }

    fn resolve_candidate_directory(
        &self,
        candidate_id: &str,
        expected_root_binding: &[u8],
    ) -> Result<PathBuf, Status> {
        validate_checkpoint_candidate_id(candidate_id)?;
        let root = admitted_checkpoint_staging_root(&self.path)?;
        let observed_binding = checkpoint_staging_root_binding(&root);
        if expected_root_binding.len() != observed_binding.len()
            || expected_root_binding != observed_binding
        {
            return Err(Status::failed_precondition(
                "canonical checkpoint staging root does not match the projector capture root",
            ));
        }
        let candidate = root.join(candidate_id);
        let metadata = fs::symlink_metadata(&candidate).map_err(|_| {
            Status::failed_precondition("canonical checkpoint candidate directory is unavailable")
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(Status::failed_precondition(
                "canonical checkpoint candidate must be an existing directory",
            ));
        }
        Ok(candidate)
    }
}

fn checkpoint_staging_root_binding(root: &Path) -> Vec<u8> {
    Sha256::digest(root.as_os_str().as_encoded_bytes()).to_vec()
}

fn validate_checkpoint_candidate_id(candidate_id: &str) -> Result<(), Status> {
    let bytes = candidate_id.as_bytes();
    let valid_length = (1..=64).contains(&bytes.len());
    let valid_boundary = bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric);
    let valid_characters = bytes
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-');
    if valid_length && valid_boundary && valid_characters {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "checkpoint candidate_id must be 1-64 lowercase ASCII letters, digits, or hyphens and begin and end with an alphanumeric character",
        ))
    }
}

fn admitted_checkpoint_staging_root(configured_root: &Path) -> Result<PathBuf, Status> {
    let metadata = fs::symlink_metadata(configured_root).map_err(|_| {
        Status::failed_precondition("canonical checkpoint staging root is unavailable")
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Status::failed_precondition(
            "canonical checkpoint staging root must be an existing directory",
        ));
    }
    // RocksDB checkpoints are path-based, so an actor allowed to rename the
    // configured root or an ancestor after this admission can still race the
    // filesystem namespace. Production staging roots must therefore be owned
    // exclusively by the ingest/projector service account. We re-admit the
    // root and child before queueing, then the owner rechecks the final child
    // with `symlink_metadata` before RocksDB writes it.
    fs::canonicalize(configured_root).map_err(|_| {
        Status::failed_precondition("canonical checkpoint staging root is unavailable")
    })
}

/// Exact current canonical state read by the follower that owns the primary.
///
/// This stays inside the ingest process. It deliberately does not widen the
/// projector protocol: the public adapters translate it into their own wire
/// contracts after the primary has authenticated the read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalWriterSnapshot {
    pub(crate) chain_epoch: ChainEpoch,
    pub(crate) fence: CanonicalEventFence,
}

/// One retained transition together with the exact immutable epochs it names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalIngestEvent {
    pub(crate) cursor: CanonicalEventCursor,
    pub(crate) kind: CanonicalEventKind,
    pub(crate) resulting_epoch: ChainEpoch,
    pub(crate) previous_epoch: Option<ChainEpoch>,
    pub(crate) reverted_range: Option<BlockHeightRange>,
    pub(crate) committed_range: BlockHeightRange,
}

/// Bounded canonical event page used by the public ingest-control adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalIngestEventPage {
    pub(crate) events: Vec<CanonicalIngestEvent>,
    pub(crate) writer: CanonicalWriterSnapshot,
    pub(crate) oldest_retained_event_sequence: u64,
}

/// Result returned from the short owner-queue portion of a checkpoint RPC.
///
/// The physical checkpoint is immutable at this point. Its full cold
/// admission deliberately happens outside the writer queue.
struct CanonicalPhysicalCheckpoint {
    candidate_id: String,
    target: PathBuf,
    admission: CanonicalOwnerCheckpointAdmission,
}

/// Sender side of the canonical writer command channel.
#[derive(Clone)]
pub struct CanonicalControlHandle {
    sender: mpsc::Sender<CanonicalControlCommand>,
}

impl CanonicalControlHandle {
    /// Enqueues a status read against the follower's current primary handle.
    pub async fn writer_status(&self) -> Result<CanonicalWriterStatusResponse, Status> {
        self.request(|reply| CanonicalControlCommand::WriterStatus { reply })
            .await
    }

    /// Reads the exact current epoch from the follower-owned primary.
    pub(crate) async fn chain_epoch(&self) -> Result<CanonicalWriterSnapshot, Status> {
        self.request(|reply| CanonicalControlCommand::ChainEpoch { reply })
            .await
    }

    /// Reads a bounded retained-event page with every event's exact epoch.
    pub(crate) async fn ingest_event_page(
        &self,
        from_cursor: Option<Vec<u8>>,
        max_events: NonZeroU32,
    ) -> Result<CanonicalIngestEventPage, Status> {
        self.request(|reply| CanonicalControlCommand::IngestEventPage {
            from_cursor,
            max_events,
            reply,
        })
        .await
    }

    /// Appends one durable mempool event through the follower-owned primary.
    pub(crate) async fn append_mempool_event(
        &self,
        event: MempoolEvent,
        observed_at: UnixTimestampMillis,
    ) -> Result<MempoolEventEnvelope, Status> {
        self.request(|reply| CanonicalControlCommand::AppendMempoolEvent {
            event: Box::new(event),
            observed_at,
            reply,
        })
        .await
    }

    /// Persists an ordered mempool-transition batch in one synced write.
    pub(crate) async fn append_mempool_events(
        &self,
        events: Vec<(MempoolEvent, UnixTimestampMillis)>,
    ) -> Result<Vec<MempoolEventEnvelope>, Status> {
        self.request(|reply| CanonicalControlCommand::AppendMempoolEvents { events, reply })
            .await
    }

    /// Reads one bounded page from the durable mempool-event log.
    pub(crate) async fn mempool_event_page(
        &self,
        from_cursor: Option<Vec<u8>>,
        max_events: NonZeroU32,
    ) -> Result<Vec<MempoolEventEnvelope>, Status> {
        self.request(|reply| CanonicalControlCommand::MempoolEventPage {
            from_cursor,
            max_events,
            reply,
        })
        .await
    }

    /// Resolves a public mempool-event stream start through the durable log.
    pub(crate) async fn resolve_mempool_event_start(
        &self,
        start: EventStreamStartPosition,
    ) -> Result<Option<StreamCursorTokenV1>, Status> {
        self.request(|reply| CanonicalControlCommand::ResolveMempoolEventStart { start, reply })
            .await
    }

    /// Decodes or captures the durable anchor for one mempool snapshot walk.
    pub(crate) async fn begin_mempool_snapshot(
        &self,
        cursor: Vec<u8>,
    ) -> Result<CanonicalMempoolSnapshotStart, Status> {
        self.request(|reply| CanonicalControlCommand::BeginMempoolSnapshot { cursor, reply })
            .await
    }

    /// Mints the next authenticated cursor for a mempool snapshot walk.
    pub(crate) async fn encode_mempool_snapshot_next_cursor(
        &self,
        events_resume_anchor: Option<MempoolEventPosition>,
        after_transaction_id: zinder_core::TransactionId,
    ) -> Result<StreamCursorTokenV1, Status> {
        self.request(
            |reply| CanonicalControlCommand::EncodeMempoolSnapshotNextCursor {
                events_resume_anchor,
                after_transaction_id,
                reply,
            },
        )
        .await
    }

    /// Runs one durable mempool-event retention pass through the writer.
    pub(crate) async fn prune_mempool_events(
        &self,
        now: UnixTimestampMillis,
        retention: MempoolEventRetentionConfig,
        budget: MempoolEventRetentionStepBudget,
    ) -> Result<MempoolEventRetentionStepOutcome, Status> {
        self.request(|reply| CanonicalControlCommand::PruneMempoolEvents {
            now,
            retention,
            budget,
            reply,
        })
        .await
    }

    /// Enqueues one bounded retained-event page read.
    pub async fn event_page(
        &self,
        request: CanonicalEventPageRequest,
    ) -> Result<CanonicalEventPageResponse, Status> {
        self.request(|reply| CanonicalControlCommand::EventPage { request, reply })
            .await
    }

    /// Creates one physical canonical checkpoint through the follower that
    /// owns the only primary store handle. Cold admission runs after this
    /// short queue operation on an immutable copy.
    async fn create_owner_checkpoint_physical(
        &self,
        candidate_id: String,
        target: PathBuf,
        expected_fence: CanonicalWriterFence,
    ) -> Result<CanonicalPhysicalCheckpoint, Status> {
        self.request(|reply| CanonicalControlCommand::CreateOwnerCheckpoint {
            candidate_id,
            target,
            expected_fence,
            reply,
        })
        .await
    }

    /// Obtains fresh opaque admission context from the canonical primary owner
    /// immediately before cold re-admitting an existing checkpoint.
    async fn owner_checkpoint_readmission(
        &self,
        target: PathBuf,
    ) -> Result<CanonicalOwnerCheckpointAdmission, Status> {
        self.request(|reply| CanonicalControlCommand::OwnerCheckpointReadmission { target, reply })
            .await
    }

    /// Enqueues durable projection-build lease acquisition.
    pub async fn acquire_projection_build_lease(
        &self,
        lease: CanonicalProjectionBuildLease,
    ) -> Result<CanonicalProjectionBuildLeaseResponse, Status> {
        self.request(|reply| CanonicalControlCommand::AcquireLease { lease, reply })
            .await
    }

    /// Enqueues durable projection-build lease renewal.
    pub async fn renew_projection_build_lease(
        &self,
        lease: CanonicalProjectionBuildLease,
    ) -> Result<CanonicalProjectionBuildLeaseResponse, Status> {
        self.request(|reply| CanonicalControlCommand::RenewLease { lease, reply })
            .await
    }

    /// Enqueues durable projection-build lease release.
    pub async fn release_projection_build_lease(
        &self,
        lease: CanonicalProjectionBuildLease,
    ) -> Result<ReleaseCanonicalProjectionBuildLeaseResponse, Status> {
        self.request(|reply| CanonicalControlCommand::ReleaseLease { lease, reply })
            .await
    }

    async fn request<ResponseMessage>(
        &self,
        make_command: impl FnOnce(
            oneshot::Sender<Result<ResponseMessage, Status>>,
        ) -> CanonicalControlCommand,
    ) -> Result<ResponseMessage, Status> {
        let (reply, response) = oneshot::channel();
        tokio::time::timeout(
            CANONICAL_CONTROL_SEND_TIMEOUT,
            self.sender.send(make_command(reply)),
        )
        .await
        .map_err(|_| Status::resource_exhausted("canonical writer command queue is full"))?
        .map_err(|_| Status::unavailable("canonical writer is unavailable"))?;
        tokio::time::timeout(CANONICAL_CONTROL_REPLY_TIMEOUT, response)
            .await
            .map_err(|_| Status::deadline_exceeded("canonical writer did not service the request"))?
            .map_err(|_| Status::unavailable("canonical writer stopped before replying"))?
    }
}

/// Creates the bounded channel serviced by the follower that owns the primary.
#[must_use]
pub fn canonical_control_channel() -> (
    CanonicalControlHandle,
    mpsc::Receiver<CanonicalControlCommand>,
) {
    let (sender, receiver) = mpsc::channel(CANONICAL_CONTROL_COMMAND_CAPACITY);
    (CanonicalControlHandle { sender }, receiver)
}

/// One request for the follower-owned canonical primary.
#[allow(
    private_interfaces,
    reason = "The binary infers these command responses through canonical_control_channel; the exact private snapshots are intentionally not a public protocol surface."
)]
pub enum CanonicalControlCommand {
    /// Return the current authenticated writer fence and retained floor.
    WriterStatus {
        /// One-shot response delivered by the primary-owning follower.
        reply: oneshot::Sender<Result<CanonicalWriterStatusResponse, Status>>,
    },
    /// Read the exact current epoch without exposing the primary handle.
    ChainEpoch {
        /// One-shot response delivered by the primary-owning follower.
        reply: oneshot::Sender<Result<CanonicalWriterSnapshot, Status>>,
    },
    /// Read retained public chain-event data with exact historical epochs.
    IngestEventPage {
        /// Opaque canonical event cursor to resume strictly after.
        from_cursor: Option<Vec<u8>>,
        /// Bounded page capacity validated by the public adapter.
        max_events: NonZeroU32,
        /// One-shot response delivered by the primary-owning follower.
        reply: oneshot::Sender<Result<CanonicalIngestEventPage, Status>>,
    },
    /// Persist one mempool transition before the live owner publishes its index change.
    AppendMempoolEvent {
        /// Source-observed transition already preflighted against the live index.
        event: Box<MempoolEvent>,
        /// Source observation timestamp retained for time-window pruning.
        observed_at: UnixTimestampMillis,
        /// One-shot response carrying the durable position.
        reply: oneshot::Sender<Result<MempoolEventEnvelope, Status>>,
    },
    /// Persist an ordered mempool-transition batch before the live owner
    /// publishes the matching index changes.
    AppendMempoolEvents {
        /// Source-observed transitions already preflighted against the live
        /// index in this exact order.
        events: Vec<(MempoolEvent, UnixTimestampMillis)>,
        /// One-shot response carrying contiguous durable positions.
        reply: oneshot::Sender<Result<Vec<MempoolEventEnvelope>, Status>>,
    },
    /// Read one bounded durable mempool-event page.
    MempoolEventPage {
        /// Opaque cursor to resume strictly after.
        from_cursor: Option<Vec<u8>>,
        /// Bounded page capacity.
        max_events: NonZeroU32,
        /// One-shot response carrying retained event envelopes.
        reply: oneshot::Sender<Result<Vec<MempoolEventEnvelope>, Status>>,
    },
    /// Resolve one public mempool-event stream start position.
    ResolveMempoolEventStart {
        /// Typed stream selector decoded at the authenticated gRPC boundary.
        start: EventStreamStartPosition,
        /// One-shot response carrying the strict-after cursor.
        reply: oneshot::Sender<Result<Option<StreamCursorTokenV1>, Status>>,
    },
    /// Decode or capture the durable event anchor for a mempool snapshot walk.
    BeginMempoolSnapshot {
        /// Opaque client paging cursor, empty for the first page.
        cursor: Vec<u8>,
        /// One-shot response carrying the decoded paging and event positions.
        reply: oneshot::Sender<Result<CanonicalMempoolSnapshotStart, Status>>,
    },
    /// Mint one authenticated continuation cursor for a snapshot walk.
    EncodeMempoolSnapshotNextCursor {
        /// Event anchor captured on the first snapshot page.
        events_resume_anchor: Option<MempoolEventPosition>,
        /// Last transaction id emitted in the current page.
        after_transaction_id: zinder_core::TransactionId,
        /// One-shot response carrying the opaque next-page cursor.
        reply: oneshot::Sender<Result<StreamCursorTokenV1, Status>>,
    },
    /// Prune the durable mempool-event log with its configured windows.
    PruneMempoolEvents {
        /// Observation time used as the retention cutoff.
        now: UnixTimestampMillis,
        /// Per-variant retention windows.
        retention: MempoolEventRetentionConfig,
        /// Bounded event-count and encoded-byte work budget.
        budget: MempoolEventRetentionStepBudget,
        /// One-shot response carrying the resulting retention report.
        reply: oneshot::Sender<Result<MempoolEventRetentionStepOutcome, Status>>,
    },
    /// Read one ordered bounded retained-event page.
    EventPage {
        /// Validated bounded-page input received from the gRPC adapter.
        request: CanonicalEventPageRequest,
        /// One-shot response delivered by the primary-owning follower.
        reply: oneshot::Sender<Result<CanonicalEventPageResponse, Status>>,
    },
    /// Create one owner checkpoint at an already-confined staging target.
    ///
    /// Only the authenticated gRPC adapter resolves `candidate_id` into this
    /// path. The follower performs the physical checkpoint while holding the
    /// canonical primary ownership.
    CreateOwnerCheckpoint {
        /// Opaque operator-selected candidate identifier returned as evidence.
        candidate_id: String,
        /// Staging-root-confined and otherwise absent checkpoint target.
        target: PathBuf,
        /// Exact canonical writer fence required before physical checkpoint.
        expected_fence: CanonicalWriterFence,
        /// One-shot physical checkpoint context for background cold admission.
        reply: oneshot::Sender<Result<CanonicalPhysicalCheckpoint, Status>>,
    },
    /// Capture opaque primary-owner admission context for a cold re-admission.
    OwnerCheckpointReadmission {
        /// Existing staging-root-confined canonical checkpoint target.
        target: PathBuf,
        /// One-shot immutable context used only by complete cold admission.
        reply: oneshot::Sender<Result<CanonicalOwnerCheckpointAdmission, Status>>,
    },
    /// Acquire one durable projection-build retention lease.
    AcquireLease {
        /// Wire lease decoded and validated by the primary-owning follower.
        lease: CanonicalProjectionBuildLease,
        /// One-shot response delivered by the primary-owning follower.
        reply: oneshot::Sender<Result<CanonicalProjectionBuildLeaseResponse, Status>>,
    },
    /// Renew one durable projection-build retention lease.
    RenewLease {
        /// Wire lease decoded and validated by the primary-owning follower.
        lease: CanonicalProjectionBuildLease,
        /// One-shot response delivered by the primary-owning follower.
        reply: oneshot::Sender<Result<CanonicalProjectionBuildLeaseResponse, Status>>,
    },
    /// Release one durable projection-build retention lease.
    ReleaseLease {
        /// Full generation-bearing lease returned by acquisition or renewal.
        lease: CanonicalProjectionBuildLease,
        /// One-shot response delivered by the primary-owning follower.
        reply: oneshot::Sender<Result<ReleaseCanonicalProjectionBuildLeaseResponse, Status>>,
    },
}

/// Services one command with the follower's current primary store.
#[allow(
    clippy::too_many_lines,
    reason = "the exhaustive owner-command dispatch keeps every primary mutation on one auditable queue"
)]
pub(crate) fn apply_canonical_control_command(
    store: &mut RocksDbCanonicalStore,
    command: CanonicalControlCommand,
) -> CanonicalControlScheduling {
    let scheduling = if matches!(&command, CanonicalControlCommand::PruneMempoolEvents { .. }) {
        CanonicalControlScheduling::YieldToCanonical
    } else {
        CanonicalControlScheduling::ContinueDraining
    };
    match command {
        CanonicalControlCommand::WriterStatus { reply } => {
            send_control_response(reply, writer_status_response(store));
        }
        CanonicalControlCommand::ChainEpoch { reply } => {
            send_control_response(reply, chain_epoch_response(store));
        }
        CanonicalControlCommand::IngestEventPage {
            from_cursor,
            max_events,
            reply,
        } => {
            read_ingest_event_page(store, from_cursor.as_deref(), max_events, reply);
        }
        CanonicalControlCommand::AppendMempoolEvent {
            event,
            observed_at,
            reply,
        } => {
            append_mempool_event(store, event, observed_at, reply);
        }
        CanonicalControlCommand::AppendMempoolEvents { events, reply } => {
            append_mempool_events(store, events, reply);
        }
        CanonicalControlCommand::MempoolEventPage {
            from_cursor,
            max_events,
            reply,
        } => {
            read_mempool_event_page(store, from_cursor.as_deref(), max_events, reply);
        }
        CanonicalControlCommand::ResolveMempoolEventStart { start, reply } => {
            resolve_mempool_event_start(store, &start, reply);
        }
        CanonicalControlCommand::BeginMempoolSnapshot { cursor, reply } => {
            begin_mempool_snapshot(store, &cursor, reply);
        }
        CanonicalControlCommand::EncodeMempoolSnapshotNextCursor {
            events_resume_anchor,
            after_transaction_id,
            reply,
        } => {
            encode_mempool_snapshot_next_cursor(
                store,
                events_resume_anchor,
                after_transaction_id,
                reply,
            );
        }
        CanonicalControlCommand::PruneMempoolEvents {
            now,
            retention,
            budget,
            reply,
        } => {
            prune_mempool_events(store, now, retention, budget, reply);
        }
        CanonicalControlCommand::EventPage { request, reply } => {
            send_control_response(reply, event_page_response(store, &request));
        }
        CanonicalControlCommand::CreateOwnerCheckpoint {
            candidate_id,
            target,
            expected_fence,
            reply,
        } => {
            send_control_response(
                reply,
                create_owner_checkpoint_physical_response(
                    store,
                    candidate_id,
                    target,
                    &expected_fence,
                ),
            );
        }
        CanonicalControlCommand::OwnerCheckpointReadmission { target, reply } => {
            send_control_response(
                reply,
                store
                    .owner_checkpoint_readmission(&target)
                    .map_err(|error| map_store_error(&error)),
            );
        }
        CanonicalControlCommand::AcquireLease { lease, reply } => {
            send_control_response(reply, acquire_lease_response(store, &lease));
        }
        CanonicalControlCommand::RenewLease { lease, reply } => {
            send_control_response(reply, renew_lease_response(store, &lease));
        }
        CanonicalControlCommand::ReleaseLease { lease, reply } => {
            send_control_response(reply, release_lease_response(store, &lease));
        }
    }
    scheduling
}

fn read_ingest_event_page(
    store: &RocksDbCanonicalStore,
    from_cursor: Option<&[u8]>,
    max_events: NonZeroU32,
    reply: oneshot::Sender<Result<CanonicalIngestEventPage, Status>>,
) {
    send_control_response(
        reply,
        ingest_event_page_response(store, from_cursor, max_events),
    );
}

fn append_mempool_event(
    store: &RocksDbCanonicalStore,
    event: Box<MempoolEvent>,
    observed_at: UnixTimestampMillis,
    reply: oneshot::Sender<Result<MempoolEventEnvelope, Status>>,
) {
    send_control_response(
        reply,
        store
            .append_mempool_event(*event, observed_at)
            .map_err(|error| map_store_error(&error)),
    );
}

fn append_mempool_events(
    store: &RocksDbCanonicalStore,
    events: Vec<(MempoolEvent, UnixTimestampMillis)>,
    reply: oneshot::Sender<Result<Vec<MempoolEventEnvelope>, Status>>,
) {
    send_control_response(
        reply,
        store
            .append_mempool_events(events)
            .map_err(|error| map_store_error(&error)),
    );
}

fn read_mempool_event_page(
    store: &RocksDbCanonicalStore,
    from_cursor: Option<&[u8]>,
    max_events: NonZeroU32,
    reply: oneshot::Sender<Result<Vec<MempoolEventEnvelope>, Status>>,
) {
    send_control_response(
        reply,
        mempool_event_page_response(store, from_cursor, max_events),
    );
}

fn resolve_mempool_event_start(
    store: &RocksDbCanonicalStore,
    start: &EventStreamStartPosition,
    reply: oneshot::Sender<Result<Option<StreamCursorTokenV1>, Status>>,
) {
    send_control_response(
        reply,
        store
            .resolve_mempool_event_stream_start(start)
            .map_err(|error| map_store_error(&error)),
    );
}

fn begin_mempool_snapshot(
    store: &RocksDbCanonicalStore,
    cursor: &[u8],
    reply: oneshot::Sender<Result<CanonicalMempoolSnapshotStart, Status>>,
) {
    send_control_response(
        reply,
        store
            .begin_mempool_snapshot(cursor)
            .map_err(|error| map_store_error(&error)),
    );
}

fn encode_mempool_snapshot_next_cursor(
    store: &RocksDbCanonicalStore,
    events_resume_anchor: Option<MempoolEventPosition>,
    after_transaction_id: zinder_core::TransactionId,
    reply: oneshot::Sender<Result<StreamCursorTokenV1, Status>>,
) {
    send_control_response(
        reply,
        store
            .encode_mempool_snapshot_next_cursor(events_resume_anchor, after_transaction_id)
            .map_err(|error| map_store_error(&error)),
    );
}

fn prune_mempool_events(
    store: &RocksDbCanonicalStore,
    now: UnixTimestampMillis,
    retention: MempoolEventRetentionConfig,
    budget: MempoolEventRetentionStepBudget,
    reply: oneshot::Sender<Result<MempoolEventRetentionStepOutcome, Status>>,
) {
    send_control_response(
        reply,
        store
            .advance_mempool_event_retention(now, retention, budget)
            .map_err(|error| map_store_error(&error)),
    );
}

fn send_control_response<ResponseMessage>(
    reply: oneshot::Sender<Result<ResponseMessage, Status>>,
    response: Result<ResponseMessage, Status>,
) {
    let _ = reply.send(response);
}

/// Authenticated gRPC adapter over the follower command channel.
#[derive(Clone)]
pub struct CanonicalControlGrpcAdapter {
    handle: CanonicalControlHandle,
    checkpoint_staging_root: CanonicalCheckpointStagingRoot,
    checkpoint_admission_resource_budget: RocksDbResourceBudget,
    bearer_token: Option<BearerToken>,
    checkpoint_bearer_token: Option<BearerToken>,
}

impl CanonicalControlGrpcAdapter {
    /// Creates an adapter that never opens or owns canonical storage.
    #[must_use]
    pub fn new(
        handle: CanonicalControlHandle,
        checkpoint_staging_root: CanonicalCheckpointStagingRoot,
        checkpoint_admission_resource_budget: RocksDbResourceBudget,
    ) -> Self {
        Self {
            handle,
            checkpoint_staging_root,
            checkpoint_admission_resource_budget,
            bearer_token: None,
            checkpoint_bearer_token: None,
        }
    }

    /// Applies the optional private-control bearer token.
    #[must_use]
    pub fn with_bearer_token(mut self, bearer_token: Option<BearerToken>) -> Self {
        self.bearer_token = bearer_token;
        self
    }

    /// Applies the separate capability token for owner checkpoint creation.
    #[must_use]
    pub fn with_checkpoint_bearer_token(mut self, bearer_token: Option<BearerToken>) -> Self {
        self.checkpoint_bearer_token = bearer_token;
        self
    }

    /// Builds the bounded, authenticated tonic service.
    #[must_use]
    pub fn into_server(
        self,
    ) -> InterceptedService<CanonicalControlServer<Self>, BearerTokenServerInterceptor> {
        let interceptor = BearerTokenServerInterceptor::new(self.bearer_token.clone());
        let server = CanonicalControlServer::new(self)
            .max_decoding_message_size(zinder_runtime::MAX_DECODING_MESSAGE_BYTES);
        InterceptedService::new(server, interceptor)
    }
}

#[tonic::async_trait]
impl CanonicalControl for CanonicalControlGrpcAdapter {
    async fn writer_status(
        &self,
        _request: Request<CanonicalWriterStatusRequest>,
    ) -> Result<Response<CanonicalWriterStatusResponse>, Status> {
        self.handle.writer_status().await.map(Response::new)
    }

    async fn event_page(
        &self,
        request: Request<CanonicalEventPageRequest>,
    ) -> Result<Response<CanonicalEventPageResponse>, Status> {
        self.handle
            .event_page(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn create_owner_checkpoint(
        &self,
        request: Request<CreateCanonicalOwnerCheckpointRequest>,
    ) -> Result<Response<CreateCanonicalOwnerCheckpointResponse>, Status> {
        let checkpoint_bearer_token = self.checkpoint_bearer_token.as_ref().ok_or_else(|| {
            Status::unauthenticated("canonical owner checkpoint capability is not configured")
        })?;
        checkpoint_bearer_token.verify_bearer_metadata(
            request.metadata().get("x-zinder-checkpoint-authorization"),
            "x-zinder-checkpoint-authorization",
        )?;
        let request = request.into_inner();
        let candidate_id = request.candidate_id;
        let expected_fence = request
            .expected_fence
            .ok_or_else(|| Status::invalid_argument("expected_fence is required"))?;
        let target = self
            .checkpoint_staging_root
            .resolve_candidate(&candidate_id, &request.staging_root_binding)?;
        let physical_checkpoint = self
            .handle
            .create_owner_checkpoint_physical(candidate_id, target, expected_fence)
            .await?;
        let CanonicalPhysicalCheckpoint {
            candidate_id,
            target,
            admission,
        } = physical_checkpoint;
        let admission_resource_budget = self.checkpoint_admission_resource_budget;
        let evidence = tokio::task::spawn_blocking(move || {
            RocksDbCanonicalStore::cold_admit_owner_checkpoint(
                target,
                &admission,
                admission_resource_budget,
            )
        })
        .await
        .map_err(|_| Status::internal("canonical checkpoint cold admission task failed"))?
        .map_err(|error| map_store_error(&error))?;
        Ok(Response::new(owner_checkpoint_response(
            candidate_id,
            &evidence,
        )))
    }

    async fn readmit_owner_checkpoint(
        &self,
        request: Request<ReadmitCanonicalOwnerCheckpointRequest>,
    ) -> Result<Response<CreateCanonicalOwnerCheckpointResponse>, Status> {
        let checkpoint_bearer_token = self.checkpoint_bearer_token.as_ref().ok_or_else(|| {
            Status::unauthenticated("canonical owner checkpoint capability is not configured")
        })?;
        checkpoint_bearer_token.verify_bearer_metadata(
            request.metadata().get("x-zinder-checkpoint-authorization"),
            "x-zinder-checkpoint-authorization",
        )?;
        let request = request.into_inner();
        if request.expected_database_identity.is_empty()
            || request.expected_database_identity.len() > 256
        {
            return Err(Status::invalid_argument(
                "expected canonical checkpoint database identity must contain 1-256 bytes",
            ));
        }
        let expected_fence = request
            .expected_fence
            .ok_or_else(|| Status::invalid_argument("expected_fence is required"))?;
        let candidate_id = request.candidate_id;
        let target = self
            .checkpoint_staging_root
            .resolve_existing_checkpoint(&candidate_id, &request.staging_root_binding)?;
        let admission = self
            .handle
            .owner_checkpoint_readmission(target.clone())
            .await?;
        let admission_resource_budget = self.checkpoint_admission_resource_budget;
        let evidence = tokio::task::spawn_blocking(move || {
            RocksDbCanonicalStore::cold_admit_owner_checkpoint(
                target,
                &admission,
                admission_resource_budget,
            )
        })
        .await
        .map_err(|_| Status::internal("canonical checkpoint cold re-admission task failed"))?
        .map_err(|error| map_store_error(&error))?;
        verify_owner_checkpoint_readmission(
            &evidence,
            &request.expected_database_identity,
            &expected_fence,
        )?;
        Ok(Response::new(owner_checkpoint_response(
            candidate_id,
            &evidence,
        )))
    }

    async fn acquire_projection_build_lease(
        &self,
        request: Request<AcquireCanonicalProjectionBuildLeaseRequest>,
    ) -> Result<Response<CanonicalProjectionBuildLeaseResponse>, Status> {
        let lease = request
            .into_inner()
            .lease
            .ok_or_else(|| Status::invalid_argument("lease is required"))?;
        self.handle
            .acquire_projection_build_lease(lease)
            .await
            .map(Response::new)
    }

    async fn renew_projection_build_lease(
        &self,
        request: Request<RenewCanonicalProjectionBuildLeaseRequest>,
    ) -> Result<Response<CanonicalProjectionBuildLeaseResponse>, Status> {
        let lease = request
            .into_inner()
            .lease
            .ok_or_else(|| Status::invalid_argument("lease is required"))?;
        self.handle
            .renew_projection_build_lease(lease)
            .await
            .map(Response::new)
    }

    async fn release_projection_build_lease(
        &self,
        request: Request<ReleaseCanonicalProjectionBuildLeaseRequest>,
    ) -> Result<Response<ReleaseCanonicalProjectionBuildLeaseResponse>, Status> {
        self.handle
            .release_projection_build_lease(
                request
                    .into_inner()
                    .lease
                    .ok_or_else(|| Status::invalid_argument("lease is required"))?,
            )
            .await
            .map(Response::new)
    }
}

fn writer_status_response(
    store: &RocksDbCanonicalStore,
) -> Result<CanonicalWriterStatusResponse, Status> {
    let retention_floor = store
        .canonical_event_retention_floor()
        .map_err(|error| map_store_error(&error))?;
    Ok(CanonicalWriterStatusResponse {
        network_name: encode_zinder_native_chain_name(store.network()).to_owned(),
        fence: Some(writer_fence_message(store)),
        oldest_retained_event_sequence: retention_floor,
        network_upgrade_schedule: store
            .network_upgrade_schedule()
            .map(network_upgrade_schedule_message),
    })
}

fn chain_epoch_response(store: &RocksDbCanonicalStore) -> Result<CanonicalWriterSnapshot, Status> {
    Ok(CanonicalWriterSnapshot {
        chain_epoch: store
            .chain_epoch()
            .map_err(|error| map_store_error(&error))?,
        fence: store.event_fence(),
    })
}

fn ingest_event_page_response(
    store: &RocksDbCanonicalStore,
    from_cursor: Option<&[u8]>,
    max_events: NonZeroU32,
) -> Result<CanonicalIngestEventPage, Status> {
    let events = store
        .canonical_event_history(CanonicalEventHistoryRequest::new(from_cursor, max_events))
        .map_err(|error| map_store_error(&error))?
        .into_iter()
        .map(|event| {
            let resulting_epoch = store
                .chain_epoch_at(event.resulting_epoch_id())
                .map_err(|error| map_store_error(&error))?;
            let previous_epoch = event
                .previous_epoch_id()
                .map(|epoch_id| store.chain_epoch_at(epoch_id))
                .transpose()
                .map_err(|error| map_store_error(&error))?;
            Ok(CanonicalIngestEvent {
                cursor: event.cursor(),
                kind: event.kind(),
                resulting_epoch,
                previous_epoch,
                reverted_range: event.reverted_range(),
                committed_range: event.committed_range(),
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    let oldest_retained_event_sequence = store
        .canonical_event_retention_floor()
        .map_err(|error| map_store_error(&error))?;
    Ok(CanonicalIngestEventPage {
        events,
        writer: chain_epoch_response(store)?,
        oldest_retained_event_sequence,
    })
}

fn mempool_event_page_response(
    store: &RocksDbCanonicalStore,
    from_cursor: Option<&[u8]>,
    max_events: NonZeroU32,
) -> Result<Vec<MempoolEventEnvelope>, Status> {
    let cursor = from_cursor.map(|bytes| StreamCursorTokenV1::from_bytes(bytes.to_vec()));
    store
        .mempool_event_history(MempoolEventHistoryRequest::new(cursor.as_ref(), max_events))
        .map_err(|error| map_store_error(&error))
}

fn event_page_response(
    store: &RocksDbCanonicalStore,
    request: &CanonicalEventPageRequest,
) -> Result<CanonicalEventPageResponse, Status> {
    let max_events = NonZeroU32::new(request.max_events)
        .filter(|max_events| max_events.get() <= CANONICAL_CONTROL_MAX_PAGE_EVENTS)
        .ok_or_else(|| Status::invalid_argument("max_events must be between 1 and 1024"))?;
    let from_cursor = (!request.from_cursor.is_empty()).then_some(request.from_cursor.as_slice());
    let events = store
        .canonical_event_history(CanonicalEventHistoryRequest::new(from_cursor, max_events))
        .map_err(|error| map_store_error(&error))?
        .into_iter()
        .map(retained_event_message)
        .collect();
    let retention_floor = store
        .canonical_event_retention_floor()
        .map_err(|error| map_store_error(&error))?;
    Ok(CanonicalEventPageResponse {
        events,
        writer_fence: Some(writer_fence_message(store)),
        oldest_retained_event_sequence: retention_floor,
    })
}

fn create_owner_checkpoint_physical_response(
    store: &mut RocksDbCanonicalStore,
    candidate_id: String,
    target: PathBuf,
    expected_fence: &CanonicalWriterFence,
) -> Result<CanonicalPhysicalCheckpoint, Status> {
    if writer_fence_message(store) != *expected_fence {
        return Err(Status::failed_precondition(
            "canonical writer advanced beyond the projector capture fence",
        ));
    }
    let admission = store
        .create_owner_checkpoint_physical(&target)
        .map_err(|error| map_store_error(&error))?;
    Ok(CanonicalPhysicalCheckpoint {
        candidate_id,
        target,
        admission,
    })
}

fn owner_checkpoint_response(
    candidate_id: String,
    evidence: &CanonicalOwnerCheckpointEvidence,
) -> CreateCanonicalOwnerCheckpointResponse {
    CreateCanonicalOwnerCheckpointResponse {
        candidate_id,
        store_identity: evidence.store_identity.to_owned(),
        schema_version: u32::from(evidence.schema_version),
        workload: evidence.workload.as_str().to_owned(),
        network_name: encode_zinder_native_chain_name(evidence.build_plan.network()).to_owned(),
        ready_evidence: Some(checkpoint_ready_evidence_message(&evidence.ready_evidence)),
        build_plan: Some(checkpoint_build_plan_message(&evidence.build_plan)),
        database_identity: evidence.database_identity.clone(),
        network_upgrade_schedule: evidence
            .activation_schedule
            .as_ref()
            .map(network_upgrade_schedule_message),
        effective_reorg_window_blocks: (evidence.reorg_policy
            != evidence.build_plan.reorg_policy())
        .then(|| evidence.reorg_policy.reorg_window_blocks()),
    }
}

/// Confirms that repeat cold admission matches the originally returned
/// physical checkpoint evidence.
///
/// The caller supplies only opaque evidence. Filesystem authority remains
/// confined to [`CanonicalCheckpointStagingRoot`].
fn verify_owner_checkpoint_readmission(
    evidence: &CanonicalOwnerCheckpointEvidence,
    expected_database_identity: &[u8],
    expected_fence: &CanonicalWriterFence,
) -> Result<(), Status> {
    if evidence.database_identity != expected_database_identity {
        return Err(Status::failed_precondition(
            "canonical checkpoint database identity changed before wallet checkpoint capture",
        ));
    }
    let observed_fence = checkpoint_ready_evidence_message(&evidence.ready_evidence)
        .visible_fence
        .ok_or_else(|| {
            Status::internal("canonical checkpoint cold admission omitted its visible fence")
        })?;
    if observed_fence != *expected_fence {
        return Err(Status::failed_precondition(
            "canonical checkpoint fence changed before wallet checkpoint capture",
        ));
    }
    Ok(())
}

fn checkpoint_ready_evidence_message(
    evidence: &zinder_store::CanonicalStoreReadyEvidence,
) -> CanonicalOwnerCheckpointReadyEvidence {
    let sequence_checkpoint = evidence.sequence_checkpoint;
    CanonicalOwnerCheckpointReadyEvidence {
        first_retained_block: Some(checkpoint_block_id_message(evidence.first_retained_block)),
        visible_fence: Some(CanonicalWriterFence {
            chain_epoch_id: evidence.visible_epoch.value(),
            event_sequence: evidence.visible_event_sequence,
            visible_tip_height: evidence.visible_tip.height.value(),
            visible_tip_hash: evidence.visible_tip.hash.as_bytes().to_vec(),
            canonical_sequence_digest: evidence.visible_sequence_digest.to_vec(),
            visible_block_count: evidence.visible_block_count,
        }),
        block_digest_version: u32::from(evidence.block_digest_version.value()),
        replay_format_version: evidence.replay_format_version.value(),
        sequence_digest_version: u32::from(evidence.sequence_digest_version.value()),
        visible_logical_replay_bytes: evidence.visible_logical_replay_bytes,
        sequence_checkpoint: Some(CanonicalCheckpointSequenceEvidence {
            through: Some(checkpoint_block_id_message(sequence_checkpoint.through())),
            retained_block_count: sequence_checkpoint.retained_block_count(),
            sequence_digest: sequence_checkpoint.sequence_digest().as_bytes().to_vec(),
            logical_replay_bytes: sequence_checkpoint.logical_replay_bytes(),
        }),
        construction_manifest_version: u32::from(evidence.construction_manifest_version),
        construction_manifest_sha256: evidence.construction_manifest_sha256.to_vec(),
    }
}

fn checkpoint_build_plan_message(
    build_plan: &zinder_store::CanonicalStoreBuildPlan,
) -> CanonicalOwnerCheckpointBuildPlanEvidence {
    let activation_fingerprint = build_plan.network_upgrade_activations_fingerprint();
    let history_predecessor = build_plan.history_predecessor();
    CanonicalOwnerCheckpointBuildPlanEvidence {
        activation_fingerprint_version: u32::from(activation_fingerprint.version().value()),
        activation_fingerprint: activation_fingerprint.as_bytes().to_vec(),
        reorg_window_blocks: build_plan.reorg_policy().reorg_window_blocks(),
        history_preceding_checkpoint: build_plan
            .history_bounds()
            .preceding_checkpoint()
            .map(checkpoint_block_id_message),
        history_predecessor: Some(CanonicalCheckpointHistoryPredecessor {
            block_id: Some(checkpoint_block_id_message(history_predecessor.block_id)),
            block_time_seconds: history_predecessor.block_time_seconds,
            sapling_frontier: history_predecessor
                .frontiers
                .get(ShieldedProtocol::Sapling)
                .map(|frontier| CanonicalCheckpointFrontier {
                    final_root: frontier.final_root().as_bytes().to_vec(),
                    final_state: frontier.final_state_bytes().to_vec(),
                }),
            orchard_frontier: history_predecessor
                .frontiers
                .get(ShieldedProtocol::Orchard)
                .map(|frontier| CanonicalCheckpointFrontier {
                    final_root: frontier.final_root().as_bytes().to_vec(),
                    final_state: frontier.final_state_bytes().to_vec(),
                }),
            ironwood_frontier: history_predecessor
                .frontiers
                .get(ShieldedProtocol::Ironwood)
                .map(|frontier| CanonicalCheckpointFrontier {
                    final_root: frontier.final_root().as_bytes().to_vec(),
                    final_state: frontier.final_state_bytes().to_vec(),
                }),
        }),
        build_tip: Some(checkpoint_block_id_message(build_plan.build_tip())),
        raw_blob_retention: build_plan.raw_blob_retention().as_kebab_case().to_owned(),
    }
}

fn checkpoint_block_id_message(block_id: zinder_core::BlockId) -> CanonicalCheckpointBlockId {
    CanonicalCheckpointBlockId {
        height: block_id.height.value(),
        hash: block_id.hash.as_bytes().to_vec(),
    }
}

fn acquire_lease_response(
    store: &RocksDbCanonicalStore,
    lease: &CanonicalProjectionBuildLease,
) -> Result<CanonicalProjectionBuildLeaseResponse, Status> {
    let lease = projection_build_lease_from_message(lease)?;
    let lease = store
        .acquire_projection_build_lease(lease, UnixTimestampMillis::now())
        .map_err(|error| map_store_error(&error))?;
    Ok(CanonicalProjectionBuildLeaseResponse {
        lease: Some(projection_build_lease_message(lease)),
    })
}

fn renew_lease_response(
    store: &RocksDbCanonicalStore,
    lease: &CanonicalProjectionBuildLease,
) -> Result<CanonicalProjectionBuildLeaseResponse, Status> {
    let lease = projection_build_lease_from_message(lease)?;
    let lease = store
        .renew_projection_build_lease(lease, UnixTimestampMillis::now())
        .map_err(|error| map_store_error(&error))?;
    Ok(CanonicalProjectionBuildLeaseResponse {
        lease: Some(projection_build_lease_message(lease)),
    })
}

fn release_lease_response(
    store: &RocksDbCanonicalStore,
    lease: &CanonicalProjectionBuildLease,
) -> Result<ReleaseCanonicalProjectionBuildLeaseResponse, Status> {
    let lease = projection_build_lease_from_message(lease)?;
    store
        .release_projection_build_lease(lease)
        .map_err(|error| map_store_error(&error))?;
    Ok(ReleaseCanonicalProjectionBuildLeaseResponse {})
}

fn writer_fence_message(store: &RocksDbCanonicalStore) -> CanonicalWriterFence {
    writer_fence_from_store_fence(store.event_fence())
}

fn writer_fence_from_store_fence(fence: zinder_store::CanonicalEventFence) -> CanonicalWriterFence {
    CanonicalWriterFence {
        chain_epoch_id: fence.chain_epoch_id().value(),
        event_sequence: fence.chain_event_sequence(),
        visible_tip_height: fence.visible_tip().height.value(),
        visible_tip_hash: fence.visible_tip().hash.as_bytes().to_vec(),
        canonical_sequence_digest: fence.sequence_digest().as_bytes().to_vec(),
        visible_block_count: fence.sequence_digest().block_count(),
    }
}

fn retained_event_message(event: zinder_store::CanonicalRetainedEvent) -> CanonicalRetainedEvent {
    CanonicalRetainedEvent {
        cursor: event.cursor().as_bytes().to_vec(),
        resulting_epoch_id: event.resulting_epoch_id().value(),
        previous_epoch_id: event
            .previous_epoch_id()
            .map(zinder_core::ChainEpochId::value),
        kind: match event.kind() {
            CanonicalEventKind::Committed => CanonicalRetainedEventKind::Committed as i32,
            CanonicalEventKind::Reorged => CanonicalRetainedEventKind::Reorged as i32,
        },
        reverted_range: event.reverted_range().map(block_range_message),
        committed_range: Some(block_range_message(event.committed_range())),
        resulting_fence: Some(writer_fence_from_store_fence(event.resulting_fence())),
    }
}

fn block_range_message(range: zinder_core::BlockHeightRange) -> CanonicalEventBlockRange {
    CanonicalEventBlockRange {
        start_height: range.start.value(),
        end_height: range.end.value(),
    }
}

fn projection_build_lease_from_message(
    lease: &CanonicalProjectionBuildLease,
) -> Result<ProjectionBuildLease, Status> {
    let lease_id = projection_build_lease_id_from_message(&lease.lease_id)?;
    let cursor = zinder_store::CanonicalEventCursor::from_persisted(&lease.anchor_event_cursor)
        .map_err(|error| map_store_error(&error))?;
    Ok(ProjectionBuildLease::new(
        lease_id,
        ProjectionBuildAnchor::new(
            zinder_core::ChainEpochId::new(lease.anchor_chain_epoch_id),
            cursor,
        ),
        UnixTimestampMillis::new(lease.expires_at_unix_millis),
    )
    .with_generation(lease.generation))
}

fn projection_build_lease_id_from_message(bytes: &[u8]) -> Result<ProjectionBuildLeaseId, Status> {
    let bytes = <[u8; 16]>::try_from(bytes)
        .map_err(|_| Status::invalid_argument("lease_id must be exactly 16 bytes"))?;
    Ok(ProjectionBuildLeaseId::from_bytes(bytes))
}

fn projection_build_lease_message(lease: ProjectionBuildLease) -> CanonicalProjectionBuildLease {
    CanonicalProjectionBuildLease {
        lease_id: lease.id().as_bytes().to_vec(),
        anchor_chain_epoch_id: lease.anchor().chain_epoch_id().value(),
        anchor_event_cursor: lease.anchor().event_cursor().as_bytes().to_vec(),
        expires_at_unix_millis: lease.expires_at().value(),
        generation: lease.generation(),
    }
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "canonical store errors are non-exhaustive; unknown future failures must remain private internal errors at this authenticated boundary"
)]
fn map_store_error(error: &CanonicalStoreError) -> Status {
    match error {
        CanonicalStoreError::CheckpointTargetExists { .. } => {
            Status::already_exists("canonical checkpoint candidate already exists")
        }
        CanonicalStoreError::CheckpointFailed { .. } => Status::failed_precondition(
            "canonical checkpoint could not be created or cold-admitted",
        ),
        CanonicalStoreError::CanonicalEventCursorExpired { .. } => {
            Status::failed_precondition("canonical event cursor has expired")
        }
        CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "lease identity is already held by a live builder",
        } => Status::already_exists("projection build lease is already held"),
        CanonicalStoreError::ProjectionBuildLeaseInvalid {
            reason: "lease identity is not held",
        } => Status::failed_precondition("projection build lease is not held"),
        CanonicalStoreError::CanonicalEventCursorMalformed { .. }
        | CanonicalStoreError::CanonicalEventCursorUnknownVersion { .. }
        | CanonicalStoreError::ProjectionBuildLeaseInvalid { .. } => {
            Status::invalid_argument("canonical control request is invalid")
        }
        CanonicalStoreError::ProjectionBuildLeaseExpired => {
            Status::failed_precondition("projection build lease has expired")
        }
        CanonicalStoreError::CanonicalEventVersionUnsupported { .. }
        | CanonicalStoreError::CanonicalEventRecordMalformed { .. } => {
            Status::failed_precondition("canonical retained event history is unavailable")
        }
        CanonicalStoreError::CanonicalEpochNotRetained { .. } => {
            Status::failed_precondition("canonical retained event epoch is unavailable")
        }
        CanonicalStoreError::MempoolEventCursorExpired { .. } => {
            Status::failed_precondition("mempool event cursor has expired")
        }
        CanonicalStoreError::MempoolSnapshotCursorExpired { .. } => {
            Status::failed_precondition("mempool snapshot page cursor has expired")
        }
        CanonicalStoreError::MempoolEventCursorInvalid { .. } => {
            Status::invalid_argument("mempool event cursor is invalid")
        }
        CanonicalStoreError::MempoolSnapshotCursorInvalid { .. } => {
            Status::invalid_argument("mempool snapshot page cursor is invalid")
        }
        CanonicalStoreError::MempoolEventSequenceOverflow => {
            Status::resource_exhausted("mempool event sequence is exhausted")
        }
        CanonicalStoreError::MempoolEventLogInvalid { .. } => {
            Status::failed_precondition("mempool event history is unavailable")
        }
        _ => Status::internal("canonical writer operation failed"),
    }
}

fn network_upgrade_schedule_message(
    schedule: &zinder_store::CanonicalActivationSchedule,
) -> zinder_proto::v1::ingest::CanonicalNetworkUpgradeSchedule {
    zinder_proto::wire::encode_canonical_network_upgrade_schedule(
        schedule.activations(),
        schedule.revision(),
    )
}

#[cfg(test)]
/// Fixture-backed canonical control helpers and loopback coverage shared by
/// canonical writer control-plane tests.
pub(crate) mod test_support {
    use std::{
        fs,
        num::{NonZeroU32, NonZeroU64},
        str::FromStr as _,
        time::Duration,
    };

    use rust_rocksdb::{DB, Options};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tokio_util::sync::CancellationToken;
    use tonic::{Request, transport::Server};
    use zinder_core::{
        BlockHash, BlockHeaderArtifact, BlockHeight, BlockId, CanonicalBlockFacts,
        CanonicalBlockFactsDigestVersion, CanonicalBlockReplayFormatVersion,
        CanonicalTransactionFacts, ChainTipMetadata, CompactBlockArtifact, CompactChainMetadata,
        CompactTransactionData, LockTime, MempoolEntry, MempoolEvictionReason, MempoolObservation,
        Network, PrivacyShape, RawTransactionBytes, SerializedBytesDigest, TransactionBlobArtifact,
        TransactionComponentCounts, TransactionId, TransactionIntrinsicValueBalances,
        TransactionLocation, TransactionPublicFacts, TransactionVersion, UnixTimestampMillis,
        UnsupportedSection, encode_canonical_block_replay,
    };
    use zinder_proto::v1::ingest::{
        AcquireCanonicalProjectionBuildLeaseRequest, CanonicalEventPageRequest,
        CanonicalProjectionBuildLease, CanonicalWriterStatusRequest,
        CreateCanonicalOwnerCheckpointRequest, ReadmitCanonicalOwnerCheckpointRequest,
        ReleaseCanonicalProjectionBuildLeaseRequest, RenewCanonicalProjectionBuildLeaseRequest,
        canonical_control_client::CanonicalControlClient,
    };
    use zinder_store::{
        CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalReorgPolicy,
        CanonicalStoreBuildPlan, CanonicalStoreError, CanonicalStoreWorkload, MempoolEvent,
        MempoolEventHistoryRequest, MempoolEventRetentionConfig, MempoolEventRetentionReport,
        MempoolEventRetentionStepStop, RocksDbCanonicalBuilder, RocksDbCanonicalStore,
        RocksDbResourceBudget,
    };

    use super::*;

    #[test]
    fn canonical_mempool_event_batch_assigns_contiguous_positions()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let transitions = (0_u8..=2)
            .map(|transaction_id_byte| {
                (
                    MempoolEvent::Invalidated {
                        transaction_id: TransactionId::from_bytes([transaction_id_byte; 32]),
                        reason: MempoolEvictionReason::Unknown,
                    },
                    UnixTimestampMillis::new(1_000 + u64::from(transaction_id_byte)),
                )
            })
            .collect();

        let envelopes = store.append_mempool_events(transitions)?;
        assert_eq!(
            envelopes
                .iter()
                .map(|envelope| envelope.event_sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let history =
            store.mempool_event_history(MempoolEventHistoryRequest::with_default_limit(None))?;
        assert_eq!(history, envelopes);
        Ok(())
    }

    #[test]
    fn retention_control_step_yields_before_more_control_commands()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (reply, _response) = oneshot::channel();
        let scheduling = apply_canonical_control_command(
            &mut store,
            CanonicalControlCommand::PruneMempoolEvents {
                now: UnixTimestampMillis::new(10_000),
                retention: MempoolEventRetentionConfig::new(
                    Some(Duration::from_millis(1)),
                    Some(Duration::from_millis(1)),
                ),
                budget: MempoolEventRetentionStepBudget::new(
                    NonZeroU32::new(1).ok_or("retention event budget must be nonzero")?,
                    NonZeroU64::new(1).ok_or("retention byte budget must be nonzero")?,
                ),
                reply,
            },
        );

        assert_eq!(scheduling, CanonicalControlScheduling::YieldToCanonical);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn loopback_control_serializes_fixture_backed_leases_and_requires_bearer()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let checkpoint_staging_root = temporary.path().join("checkpoint-staging");
        fs::create_dir(&checkpoint_staging_root)?;
        fs::create_dir(checkpoint_staging_root.join("bundle-a1"))?;
        fs::create_dir(checkpoint_staging_root.join("bundle-b2"))?;
        let delayed_target = checkpoint_staging_root.join("bundle-b2/canonical.rocksdb");
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (handle, mut commands) = canonical_control_channel();
        let status_handle = handle.clone();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                apply_canonical_control_command(&mut store, command);
            }
        });
        let bearer_token = BearerToken::from_str("fixture-control-token")?;
        let checkpoint_bearer_token = BearerToken::from_str("fixture-checkpoint-token")?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let listen_addr = listener.local_addr()?;
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_task = tokio::spawn(async move {
            let adapter = CanonicalControlGrpcAdapter::new(
                handle,
                CanonicalCheckpointStagingRoot::new(checkpoint_staging_root),
                RocksDbResourceBudget::for_local_tests(),
            )
            .with_bearer_token(Some(bearer_token))
            .with_checkpoint_bearer_token(Some(checkpoint_bearer_token));
            let _ = Server::builder()
                .add_service(adapter.into_server())
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    server_cancel.cancelled_owned(),
                )
                .await;
        });
        tokio::time::sleep(Duration::from_millis(25)).await;

        let mut client = CanonicalControlClient::connect(format!("http://{listen_addr}")).await?;
        let unauthenticated = client.writer_status(CanonicalWriterStatusRequest {}).await;
        assert_eq!(
            unauthenticated.err().map(|status| status.code()),
            Some(tonic::Code::Unauthenticated)
        );

        let status = client
            .writer_status(authenticated(CanonicalWriterStatusRequest {}))
            .await?
            .into_inner();
        assert_eq!(status.network_name, "zcash-testnet");
        assert_eq!(status.oldest_retained_event_sequence, 1);
        assert_eq!(status.fence.map(|fence| fence.event_sequence), Some(1));

        let page = client
            .event_page(authenticated(CanonicalEventPageRequest {
                from_cursor: Vec::new(),
                max_events: 1,
            }))
            .await?
            .into_inner();
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].resulting_epoch_id, 1);
        assert_eq!(
            page.events[0]
                .resulting_fence
                .as_ref()
                .map(|fence| fence.event_sequence),
            Some(1)
        );

        assert_fixture_lease_lifecycle(
            &mut client,
            page.events[0].resulting_epoch_id,
            page.events[0].cursor.clone(),
        )
        .await?;

        assert_owner_checkpoint_control_boundary(&mut client, temporary.path()).await?;
        assert_cold_admission_does_not_hold_the_owner_queue(status_handle, delayed_target).await?;

        cancel.cancel();
        server_task.await?;
        command_task.abort();
        Ok(())
    }

    async fn assert_cold_admission_does_not_hold_the_owner_queue(
        handle: CanonicalControlHandle,
        target: std::path::PathBuf,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected_fence = handle
            .writer_status()
            .await?
            .fence
            .ok_or("fixture writer status omitted fence")?;
        let physical = handle
            .create_owner_checkpoint_physical("bundle-b2".to_owned(), target, expected_fence)
            .await?;
        let CanonicalPhysicalCheckpoint {
            candidate_id: _,
            target,
            admission,
        } = physical;
        let delayed_admission = tokio::task::spawn_blocking(move || {
            std::thread::sleep(Duration::from_millis(100));
            RocksDbCanonicalStore::cold_admit_owner_checkpoint(
                target,
                &admission,
                RocksDbResourceBudget::for_local_tests(),
            )
        });
        let status = handle.writer_status().await?;
        assert_eq!(status.fence.map(|fence| fence.event_sequence), Some(1));
        let _evidence = delayed_admission.await??;
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the fixture proves the complete two-token checkpoint boundary and returned evidence in one lifecycle"
    )]
    async fn assert_owner_checkpoint_control_boundary(
        client: &mut CanonicalControlClient<tonic::transport::Channel>,
        temporary_path: &std::path::Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected_fence = client
            .writer_status(authenticated(CanonicalWriterStatusRequest {}))
            .await?
            .into_inner()
            .fence
            .ok_or("fixture writer status omitted fence")?;
        assert_checkpoint_candidate_ids_are_confined(
            client,
            temporary_path,
            expected_fence.clone(),
        )
        .await?;
        assert_checkpoint_symlink_candidates_are_preserved(
            client,
            temporary_path,
            expected_fence.clone(),
        )
        .await?;

        let ordinary_only = client
            .create_owner_checkpoint(authenticated(checkpoint_request(
                "bundle-a1",
                temporary_path,
                expected_fence.clone(),
            )?))
            .await
            .err()
            .ok_or("ordinary ingest-control token unexpectedly authorized a checkpoint")?;
        assert_eq!(ordinary_only.code(), tonic::Code::Unauthenticated);

        let checkpoint_only = client
            .create_owner_checkpoint(checkpoint_only(checkpoint_request(
                "bundle-a1",
                temporary_path,
                expected_fence.clone(),
            )?))
            .await
            .err()
            .ok_or("checkpoint capability unexpectedly bypassed ordinary control auth")?;
        assert_eq!(checkpoint_only.code(), tonic::Code::Unauthenticated);

        let checkpoint = client
            .create_owner_checkpoint(checkpoint_authenticated(checkpoint_request(
                "bundle-a1",
                temporary_path,
                expected_fence.clone(),
            )?))
            .await?
            .into_inner();
        assert_owner_checkpoint_readmission(&mut *client, temporary_path, &checkpoint).await?;
        assert_eq!(checkpoint.candidate_id, "bundle-a1");
        assert_eq!(checkpoint.store_identity, "canonical");
        assert_eq!(
            checkpoint.schema_version,
            u32::from(zinder_store::CANONICAL_STORE_SCHEMA_VERSION)
        );
        assert_eq!(checkpoint.workload, "wallet");
        assert_eq!(checkpoint.network_name, "zcash-testnet");
        assert!(!checkpoint.database_identity.is_empty());
        let ready_evidence = checkpoint
            .ready_evidence
            .ok_or("checkpoint response omitted ready evidence")?;
        assert_eq!(
            ready_evidence
                .visible_fence
                .as_ref()
                .map(|fence| fence.event_sequence),
            Some(1)
        );
        assert_eq!(
            ready_evidence
                .sequence_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.retained_block_count),
            Some(1)
        );
        assert_eq!(checkpoint.effective_reorg_window_blocks, None);
        let build_plan = checkpoint
            .build_plan
            .ok_or("checkpoint response omitted build-plan identity")?;
        assert_eq!(build_plan.activation_fingerprint_version, 1);
        assert_eq!(build_plan.raw_blob_retention, "transactions");
        assert_eq!(build_plan.reorg_window_blocks, 1);
        assert_eq!(
            build_plan
                .history_predecessor
                .as_ref()
                .and_then(|predecessor| predecessor.block_id.as_ref())
                .map(|block_id| block_id.height),
            Some(0)
        );
        assert_eq!(
            build_plan
                .build_tip
                .as_ref()
                .map(|block_id| block_id.height),
            Some(1)
        );
        assert!(
            temporary_path
                .join("checkpoint-staging/bundle-a1/canonical.rocksdb")
                .is_dir()
        );

        let duplicate = client
            .create_owner_checkpoint(checkpoint_authenticated(checkpoint_request(
                "bundle-a1",
                temporary_path,
                expected_fence,
            )?))
            .await
            .err()
            .ok_or("existing checkpoint candidate unexpectedly succeeded")?;
        assert_eq!(duplicate.code(), tonic::Code::AlreadyExists);
        assert!(
            temporary_path
                .join("checkpoint-staging/bundle-a1/canonical.rocksdb")
                .is_dir(),
            "an existing checkpoint candidate must remain intact"
        );
        Ok(())
    }

    async fn assert_checkpoint_candidate_ids_are_confined(
        client: &mut CanonicalControlClient<tonic::transport::Channel>,
        temporary_path: &std::path::Path,
        expected_fence: CanonicalWriterFence,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let traversal = client
            .create_owner_checkpoint(checkpoint_authenticated(checkpoint_request(
                "../outside",
                temporary_path,
                expected_fence.clone(),
            )?))
            .await
            .err()
            .ok_or("checkpoint traversal candidate unexpectedly succeeded")?;
        assert_eq!(traversal.code(), tonic::Code::InvalidArgument);
        assert!(
            !temporary_path.join("outside").exists(),
            "invalid candidate must not create a target outside staging"
        );

        let absolute = client
            .create_owner_checkpoint(checkpoint_authenticated(checkpoint_request(
                "/absolute",
                temporary_path,
                expected_fence,
            )?))
            .await
            .err()
            .ok_or("absolute checkpoint candidate unexpectedly succeeded")?;
        assert_eq!(absolute.code(), tonic::Code::InvalidArgument);
        Ok(())
    }

    async fn assert_owner_checkpoint_readmission(
        client: &mut CanonicalControlClient<tonic::transport::Channel>,
        temporary_path: &std::path::Path,
        checkpoint: &zinder_proto::v1::ingest::CreateCanonicalOwnerCheckpointResponse,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected_fence = checkpoint
            .ready_evidence
            .as_ref()
            .and_then(|ready| ready.visible_fence.clone())
            .ok_or("checkpoint response omitted its visible fence")?;
        let request = checkpoint_readmission_request(
            checkpoint,
            temporary_path,
            expected_fence.clone(),
            checkpoint.database_identity.clone(),
        )?;
        let ordinary_only = client
            .readmit_owner_checkpoint(authenticated(request.clone()))
            .await
            .err()
            .ok_or(
                "ordinary ingest-control token unexpectedly authorized a checkpoint re-admission",
            )?;
        assert_eq!(ordinary_only.code(), tonic::Code::Unauthenticated);
        let checkpoint_only = client
            .readmit_owner_checkpoint(checkpoint_only(request.clone()))
            .await
            .err()
            .ok_or("checkpoint capability unexpectedly bypassed ordinary control auth for re-admission")?;
        assert_eq!(checkpoint_only.code(), tonic::Code::Unauthenticated);
        let readmitted = client
            .readmit_owner_checkpoint(checkpoint_authenticated(request))
            .await?
            .into_inner();
        assert_eq!(readmitted, checkpoint.clone());

        let mut wrong_identity = checkpoint.database_identity.clone();
        wrong_identity[0] ^= 0x01;
        let identity_drift = client
            .readmit_owner_checkpoint(checkpoint_authenticated(checkpoint_readmission_request(
                checkpoint,
                temporary_path,
                expected_fence.clone(),
                wrong_identity,
            )?))
            .await
            .err()
            .ok_or("canonical identity drift unexpectedly passed re-admission")?;
        assert_eq!(identity_drift.code(), tonic::Code::FailedPrecondition);
        let mut wrong_fence = expected_fence;
        wrong_fence.event_sequence = wrong_fence.event_sequence.saturating_add(1);
        let fence_drift = client
            .readmit_owner_checkpoint(checkpoint_authenticated(checkpoint_readmission_request(
                checkpoint,
                temporary_path,
                wrong_fence,
                checkpoint.database_identity.clone(),
            )?))
            .await
            .err()
            .ok_or("canonical fence drift unexpectedly passed re-admission")?;
        assert_eq!(fence_drift.code(), tonic::Code::FailedPrecondition);
        Ok(())
    }

    #[cfg(unix)]
    async fn assert_checkpoint_symlink_candidates_are_preserved(
        client: &mut CanonicalControlClient<tonic::transport::Channel>,
        temporary_path: &std::path::Path,
        expected_fence: CanonicalWriterFence,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let staging_root = temporary_path.join("checkpoint-staging");
        let broken_candidate = staging_root.join("broken-link");
        symlink(temporary_path.join("outside-root"), &broken_candidate)?;
        let candidate_status = client
            .create_owner_checkpoint(checkpoint_authenticated(checkpoint_request(
                "broken-link",
                temporary_path,
                expected_fence.clone(),
            )?))
            .await
            .err()
            .ok_or("symlink checkpoint candidate unexpectedly succeeded")?;
        assert_eq!(candidate_status.code(), tonic::Code::FailedPrecondition);
        assert!(
            fs::symlink_metadata(&broken_candidate)?
                .file_type()
                .is_symlink()
        );

        let child_candidate = staging_root.join("child-link");
        fs::create_dir(&child_candidate)?;
        let child = child_candidate.join("canonical.rocksdb");
        symlink(temporary_path.join("outside-child"), &child)?;
        let child_status = client
            .create_owner_checkpoint(checkpoint_authenticated(checkpoint_request(
                "child-link",
                temporary_path,
                expected_fence,
            )?))
            .await
            .err()
            .ok_or("symlink checkpoint child unexpectedly succeeded")?;
        assert_eq!(child_status.code(), tonic::Code::AlreadyExists);
        assert!(fs::symlink_metadata(&child)?.file_type().is_symlink());
        Ok(())
    }

    #[cfg(not(unix))]
    async fn assert_checkpoint_symlink_candidates_are_preserved(
        _client: &mut CanonicalControlClient<tonic::transport::Channel>,
        _temporary_path: &std::path::Path,
        _expected_fence: CanonicalWriterFence,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn assert_fixture_lease_lifecycle(
        client: &mut CanonicalControlClient<tonic::transport::Channel>,
        anchor_chain_epoch_id: u64,
        anchor_event_cursor: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expires_at = UnixTimestampMillis::now().value().saturating_add(60_000);
        let acquired = client
            .acquire_projection_build_lease(authenticated(
                AcquireCanonicalProjectionBuildLeaseRequest {
                    lease: Some(CanonicalProjectionBuildLease {
                        lease_id: vec![7; 16],
                        anchor_chain_epoch_id,
                        anchor_event_cursor,
                        expires_at_unix_millis: expires_at,
                        generation: 0,
                    }),
                },
            ))
            .await?
            .into_inner()
            .lease
            .ok_or("acquire response omitted lease")?;
        assert_ne!(acquired.generation, 0);

        let mut renewed_request_lease = acquired.clone();
        renewed_request_lease.expires_at_unix_millis = expires_at.saturating_add(1_000);
        let renewed = client
            .renew_projection_build_lease(authenticated(
                RenewCanonicalProjectionBuildLeaseRequest {
                    lease: Some(renewed_request_lease),
                },
            ))
            .await?
            .into_inner()
            .lease
            .ok_or("renew response omitted lease")?;
        assert_eq!(renewed.generation, acquired.generation);

        let mut stale_release = renewed.clone();
        stale_release.generation = stale_release.generation.saturating_add(1);
        let stale_status = client
            .release_projection_build_lease(authenticated(
                ReleaseCanonicalProjectionBuildLeaseRequest {
                    lease: Some(stale_release),
                },
            ))
            .await
            .err()
            .ok_or("stale release unexpectedly succeeded")?;
        assert_eq!(stale_status.code(), tonic::Code::InvalidArgument);
        client
            .release_projection_build_lease(authenticated(
                ReleaseCanonicalProjectionBuildLeaseRequest {
                    lease: Some(renewed),
                },
            ))
            .await?;
        Ok(())
    }

    #[test]
    fn cold_checkpoint_admission_rejects_a_replaced_same_plan_checkpoint()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let mut source = published_fixture_store(&temporary.path().join("source"))?;
        let mut replacement = published_fixture_store(&temporary.path().join("replacement"))?;
        let target = temporary.path().join("candidate/canonical.rocksdb");
        fs::create_dir(temporary.path().join("candidate"))?;
        let admission = source.create_owner_checkpoint_physical(&target)?;
        let replacement_target = temporary.path().join("replacement-checkpoint");
        let _replacement_admission =
            replacement.create_owner_checkpoint_physical(&replacement_target)?;
        fs::remove_dir_all(&target)?;
        fs::rename(&replacement_target, &target)?;

        let error = RocksDbCanonicalStore::cold_admit_owner_checkpoint(
            &target,
            &admission,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("replaced checkpoint unexpectedly passed cold admission")?;
        assert!(matches!(
            error,
            CanonicalStoreError::AdmissionRefused { .. }
        ));
        Ok(())
    }

    #[test]
    fn canonical_mempool_history_survives_restart_and_pruned_cursor_expires()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = published_fixture_store(&store_path)?;
        let chain_epoch = store.chain_epoch()?;
        let transaction_id = TransactionId::from_bytes([0xB1; 32]);
        let first = store.append_mempool_event(
            MempoolEvent::Added {
                entry: MempoolEntry::new(
                    transaction_id,
                    None,
                    RawTransactionBytes::new(vec![0xB1; 8]),
                    CompactTransactionData::default(),
                    MempoolObservation {
                        first_seen_unix_millis: UnixTimestampMillis::new(1_000),
                        first_seen_chain_epoch: chain_epoch,
                    },
                )?,
            },
            UnixTimestampMillis::new(1_000),
        )?;
        let _second = store.append_mempool_event(
            MempoolEvent::Invalidated {
                transaction_id,
                reason: MempoolEvictionReason::Unknown,
            },
            UnixTimestampMillis::new(2_000),
        )?;
        drop(store);

        let activations = fixture_activations()?;
        let reopened = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(1)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let retained = reopened.mempool_event_history(MempoolEventHistoryRequest::new(
            None,
            NonZeroU32::new(8).ok_or("mempool history test limit must be nonzero")?,
        ))?;
        assert_eq!(retained.len(), 2, "restart must retain appended history");
        assert_eq!(retained[0].cursor, first.cursor);

        let retention = MempoolEventRetentionConfig::new(
            Some(Duration::from_millis(1)),
            Some(Duration::from_millis(1)),
        );
        let budget = MempoolEventRetentionStepBudget::new(
            NonZeroU32::new(8).ok_or("retention event budget must be nonzero")?,
            NonZeroU64::new(1_000_000).ok_or("retention byte budget must be nonzero")?,
        );
        let mut report = MempoolEventRetentionReport::default();
        for _step in 0..4 {
            let outcome = reopened.advance_mempool_event_retention(
                UnixTimestampMillis::new(10_000),
                retention,
                budget,
            )?;
            report = outcome.report;
            if !outcome.has_immediate_work() {
                break;
            }
        }
        assert_eq!(report.oldest_retained_sequence, Some(2));
        let expired = reopened
            .mempool_event_history(MempoolEventHistoryRequest::new(
                Some(&first.cursor),
                NonZeroU32::new(8).ok_or("mempool history test limit must be nonzero")?,
            ))
            .err()
            .ok_or("pruned mempool cursor unexpectedly resumed")?;
        assert!(matches!(
            expired,
            CanonicalStoreError::MempoolEventCursorExpired { .. }
        ));
        Ok(())
    }

    #[test]
    fn bounded_retention_preserves_active_anchor_until_its_terminal_event_is_scanned()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let chain_epoch = store.chain_epoch()?;
        let active_transaction_id = TransactionId::from_bytes([0xA1; 32]);
        let head_transaction_id = TransactionId::from_bytes([0xA2; 32]);
        for event in [
            MempoolEvent::Added {
                entry: retention_test_entry(active_transaction_id, 0xA1, chain_epoch)?,
            },
            MempoolEvent::Invalidated {
                transaction_id: TransactionId::from_bytes([0xB2; 32]),
                reason: MempoolEvictionReason::Unknown,
            },
            MempoolEvent::Invalidated {
                transaction_id: TransactionId::from_bytes([0xB3; 32]),
                reason: MempoolEvictionReason::Unknown,
            },
            MempoolEvent::Invalidated {
                transaction_id: TransactionId::from_bytes([0xB4; 32]),
                reason: MempoolEvictionReason::Unknown,
            },
            MempoolEvent::Invalidated {
                transaction_id: active_transaction_id,
                reason: MempoolEvictionReason::Unknown,
            },
            MempoolEvent::Added {
                entry: retention_test_entry(head_transaction_id, 0xA2, chain_epoch)?,
            },
        ] {
            let _envelope = store.append_mempool_event(event, UnixTimestampMillis::new(1_000))?;
        }
        let retention = MempoolEventRetentionConfig::new(
            Some(Duration::from_millis(1)),
            Some(Duration::from_millis(1)),
        );
        let budget = MempoolEventRetentionStepBudget::new(
            NonZeroU32::new(2).ok_or("retention event budget must be nonzero")?,
            NonZeroU64::new(1_000_000).ok_or("retention byte budget must be nonzero")?,
        );

        for _step in 0..2 {
            let outcome = store.advance_mempool_event_retention(
                UnixTimestampMillis::new(10_000),
                retention,
                budget,
            )?;
            assert_eq!(outcome.stop, MempoolEventRetentionStepStop::BudgetExhausted);
            assert_eq!(outcome.report.oldest_retained_sequence, Some(1));
            assert!(outcome.examined_event_count <= 2);
        }

        let mut pruned_total = 0_u64;
        let mut final_report = MempoolEventRetentionReport::default();
        for _step in 0..6 {
            let outcome = store.advance_mempool_event_retention(
                UnixTimestampMillis::new(10_000),
                retention,
                budget,
            )?;
            assert!(outcome.examined_event_count <= 2);
            assert!(outcome.report.pruned_total() <= 2);
            pruned_total = pruned_total.saturating_add(outcome.report.pruned_total());
            final_report = outcome.report;
            if !outcome.has_immediate_work() {
                break;
            }
        }

        assert_eq!(pruned_total, 5);
        assert_eq!(final_report.oldest_retained_sequence, Some(6));
        let retained = store.mempool_event_history(MempoolEventHistoryRequest::new(
            None,
            NonZeroU32::new(8).ok_or("mempool history limit must be nonzero")?,
        ))?;
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].transaction_id(), head_transaction_id);
        Ok(())
    }

    #[test]
    fn retention_gap_fails_closed_without_committing_partial_floor_progress()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = published_fixture_store(&store_path)?;
        for transaction_tag in 1_u8..=5 {
            let _envelope = store.append_mempool_event(
                MempoolEvent::Invalidated {
                    transaction_id: TransactionId::from_bytes([transaction_tag; 32]),
                    reason: MempoolEvictionReason::Unknown,
                },
                UnixTimestampMillis::new(1_000),
            )?;
        }
        drop(store);
        let removed_event = swap_raw_mempool_event(&store_path, 3, None)?
            .ok_or("corruption target event must exist")?;

        let activations = fixture_activations()?;
        let corrupted = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(1)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let error = corrupted
            .advance_mempool_event_retention(
                UnixTimestampMillis::new(10_000),
                MempoolEventRetentionConfig::new(
                    Some(Duration::from_millis(1)),
                    Some(Duration::from_millis(1)),
                ),
                MempoolEventRetentionStepBudget::new(
                    NonZeroU32::new(16).ok_or("retention event budget must be nonzero")?,
                    NonZeroU64::new(1_000_000).ok_or("retention byte budget must be nonzero")?,
                ),
            )
            .err()
            .ok_or("retention unexpectedly crossed an interior history gap")?;
        assert!(matches!(
            error,
            CanonicalStoreError::MempoolEventLogInvalid { .. }
        ));
        drop(corrupted);

        let replaced_event = swap_raw_mempool_event(&store_path, 3, Some(&removed_event))?;
        assert!(replaced_event.is_none());
        let repaired = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(1)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let retained = repaired.mempool_event_history(MempoolEventHistoryRequest::new(
            None,
            NonZeroU32::new(8).ok_or("mempool history limit must be nonzero")?,
        ))?;
        assert_eq!(retained.len(), 5);
        assert_eq!(retained[0].position().event_sequence, 1);
        Ok(())
    }

    #[test]
    fn bounded_retention_restart_rescans_from_the_durable_floor()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = published_fixture_store(&store_path)?;
        for transaction_tag in 1_u8..=5 {
            let _envelope = store.append_mempool_event(
                MempoolEvent::Invalidated {
                    transaction_id: TransactionId::from_bytes([transaction_tag; 32]),
                    reason: MempoolEvictionReason::Unknown,
                },
                UnixTimestampMillis::new(1_000),
            )?;
        }
        let retention = MempoolEventRetentionConfig::new(
            Some(Duration::from_millis(1)),
            Some(Duration::from_millis(1)),
        );
        let budget = MempoolEventRetentionStepBudget::new(
            NonZeroU32::new(2).ok_or("retention event budget must be nonzero")?,
            NonZeroU64::new(1_000_000).ok_or("retention byte budget must be nonzero")?,
        );

        let first_step = store.advance_mempool_event_retention(
            UnixTimestampMillis::new(10_000),
            retention,
            budget,
        )?;
        assert!(first_step.has_immediate_work());
        assert_eq!(first_step.report.oldest_retained_sequence, Some(2));
        drop(store);

        let activations = fixture_activations()?;
        let reopened = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            CanonicalStoreWorkload::Wallet,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(1)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let mut final_report = MempoolEventRetentionReport::default();
        for _step in 0..10 {
            let outcome = reopened.advance_mempool_event_retention(
                UnixTimestampMillis::new(10_000),
                retention,
                budget,
            )?;
            assert!(outcome.examined_event_count <= 2);
            final_report = outcome.report;
            if !outcome.has_immediate_work() {
                break;
            }
        }

        assert_eq!(final_report.oldest_retained_sequence, Some(5));
        let retained = reopened.mempool_event_history(MempoolEventHistoryRequest::new(
            None,
            NonZeroU32::new(8).ok_or("mempool history limit must be nonzero")?,
        ))?;
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].position().event_sequence, 5);
        Ok(())
    }

    #[test]
    fn bounded_retention_interleaved_readd_becomes_the_new_replay_anchor()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let chain_epoch = store.chain_epoch()?;
        let transaction_id = TransactionId::from_bytes([0xD1; 32]);
        append_retention_test_events(
            &store,
            [
                MempoolEvent::Added {
                    entry: retention_test_entry(transaction_id, 0xD1, chain_epoch)?,
                },
                MempoolEvent::Invalidated {
                    transaction_id: TransactionId::from_bytes([0xD2; 32]),
                    reason: MempoolEvictionReason::Unknown,
                },
                MempoolEvent::Invalidated {
                    transaction_id: TransactionId::from_bytes([0xD3; 32]),
                    reason: MempoolEvictionReason::Unknown,
                },
            ],
        )?;
        let retention = MempoolEventRetentionConfig::new(
            Some(Duration::from_millis(1)),
            Some(Duration::from_millis(1)),
        );
        let budget = MempoolEventRetentionStepBudget::new(
            NonZeroU32::new(2).ok_or("retention event budget must be nonzero")?,
            NonZeroU64::new(1_000_000).ok_or("retention byte budget must be nonzero")?,
        );

        let first_step = store.advance_mempool_event_retention(
            UnixTimestampMillis::new(10_000),
            retention,
            budget,
        )?;
        assert!(first_step.has_immediate_work());
        append_retention_test_events(
            &store,
            [
                MempoolEvent::Invalidated {
                    transaction_id,
                    reason: MempoolEvictionReason::Unknown,
                },
                MempoolEvent::Added {
                    entry: retention_test_entry(transaction_id, 0xD4, chain_epoch)?,
                },
                MempoolEvent::Invalidated {
                    transaction_id: TransactionId::from_bytes([0xD5; 32]),
                    reason: MempoolEvictionReason::Unknown,
                },
            ],
        )?;

        let captured_head_step = store.advance_mempool_event_retention(
            UnixTimestampMillis::new(10_000),
            retention,
            budget,
        )?;
        assert!(!captured_head_step.has_immediate_work());
        assert_eq!(captured_head_step.report.oldest_retained_sequence, Some(1));

        let mut final_report = MempoolEventRetentionReport::default();
        for _step in 0..12 {
            let outcome = store.advance_mempool_event_retention(
                UnixTimestampMillis::new(10_000),
                retention,
                budget,
            )?;
            final_report = outcome.report;
            if !outcome.has_immediate_work() {
                break;
            }
        }

        assert_eq!(final_report.oldest_retained_sequence, Some(5));
        let retained = store.mempool_event_history(MempoolEventHistoryRequest::new(
            None,
            NonZeroU32::new(8).ok_or("mempool history limit must be nonzero")?,
        ))?;
        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].position().event_sequence, 5);
        assert_eq!(retained[0].transaction_id(), transaction_id);
        assert_eq!(retained[1].position().event_sequence, 6);
        Ok(())
    }

    #[test]
    fn first_unexpired_event_blocks_pruning_of_later_expired_events()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        for (transaction_tag, observed_at) in
            [(0xE1, 1_000), (0xE2, 9_999), (0xE3, 1_000), (0xE4, 1_000)]
        {
            let _envelope = store.append_mempool_event(
                MempoolEvent::Invalidated {
                    transaction_id: TransactionId::from_bytes([transaction_tag; 32]),
                    reason: MempoolEvictionReason::Unknown,
                },
                UnixTimestampMillis::new(observed_at),
            )?;
        }
        let retention = MempoolEventRetentionConfig::new(
            Some(Duration::from_millis(100)),
            Some(Duration::from_millis(100)),
        );
        let budget = MempoolEventRetentionStepBudget::new(
            NonZeroU32::new(8).ok_or("retention event budget must be nonzero")?,
            NonZeroU64::new(1_000_000).ok_or("retention byte budget must be nonzero")?,
        );

        let outcome = store.advance_mempool_event_retention(
            UnixTimestampMillis::new(10_000),
            retention,
            budget,
        )?;
        assert_eq!(
            outcome.stop,
            MempoolEventRetentionStepStop::ReachedUnexpiredEvent
        );
        assert_eq!(outcome.report.oldest_retained_sequence, Some(2));
        let retained = store.mempool_event_history(MempoolEventHistoryRequest::new(
            None,
            NonZeroU32::new(8).ok_or("mempool history limit must be nonzero")?,
        ))?;
        assert_eq!(retained.len(), 3);
        assert_eq!(retained[0].position().event_sequence, 2);
        assert_eq!(retained[2].position().event_sequence, 4);
        Ok(())
    }

    #[test]
    fn retention_step_uses_remaining_budget_after_a_terminal_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let chain_epoch = store.chain_epoch()?;
        for transaction_tag in 1_u8..=3 {
            let transaction_id = TransactionId::from_bytes([transaction_tag; 32]);
            for event in [
                MempoolEvent::Added {
                    entry: retention_test_entry(transaction_id, transaction_tag, chain_epoch)?,
                },
                MempoolEvent::Invalidated {
                    transaction_id,
                    reason: MempoolEvictionReason::Unknown,
                },
            ] {
                let _envelope =
                    store.append_mempool_event(event, UnixTimestampMillis::new(1_000))?;
            }
        }
        let _head = store.append_mempool_event(
            MempoolEvent::Invalidated {
                transaction_id: TransactionId::from_bytes([0xF0; 32]),
                reason: MempoolEvictionReason::Unknown,
            },
            UnixTimestampMillis::new(1_000),
        )?;

        let outcome = store.advance_mempool_event_retention(
            UnixTimestampMillis::new(10_000),
            MempoolEventRetentionConfig::new(
                Some(Duration::from_millis(1)),
                Some(Duration::from_millis(1)),
            ),
            MempoolEventRetentionStepBudget::new(
                NonZeroU32::new(16).ok_or("retention event budget must be nonzero")?,
                NonZeroU64::new(1_000_000).ok_or("retention byte budget must be nonzero")?,
            ),
        )?;

        assert_eq!(outcome.stop, MempoolEventRetentionStepStop::ReachedHead);
        assert_eq!(outcome.report.pruned_total(), 6);
        assert_eq!(outcome.report.oldest_retained_sequence, Some(7));
        assert!(outcome.examined_event_count > 2);
        assert!(outcome.examined_event_count <= 16);
        Ok(())
    }

    #[test]
    fn retention_byte_target_allows_one_row_overshoot_for_progress()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let _head = store.append_mempool_event(
            MempoolEvent::Invalidated {
                transaction_id: TransactionId::from_bytes([0xC1; 32]),
                reason: MempoolEvictionReason::Unknown,
            },
            UnixTimestampMillis::new(1_000),
        )?;
        let outcome = store.advance_mempool_event_retention(
            UnixTimestampMillis::new(10_000),
            MempoolEventRetentionConfig::new(
                Some(Duration::from_millis(1)),
                Some(Duration::from_millis(1)),
            ),
            MempoolEventRetentionStepBudget::new(
                NonZeroU32::new(1).ok_or("retention event budget must be nonzero")?,
                NonZeroU64::new(1).ok_or("retention byte budget must be nonzero")?,
            ),
        )?;

        assert_eq!(outcome.examined_event_count, 1);
        assert!(outcome.examined_encoded_bytes > 1);
        assert_eq!(outcome.stop, MempoolEventRetentionStepStop::ReachedHead);
        assert_eq!(outcome.report.oldest_retained_sequence, Some(1));
        Ok(())
    }

    fn retention_test_entry(
        transaction_id: TransactionId,
        transaction_tag: u8,
        chain_epoch: ChainEpoch,
    ) -> Result<MempoolEntry, zinder_core::MempoolEntryBuildError> {
        MempoolEntry::new(
            transaction_id,
            None,
            RawTransactionBytes::new(vec![transaction_tag; 8]),
            CompactTransactionData::default(),
            MempoolObservation {
                first_seen_unix_millis: UnixTimestampMillis::new(1_000),
                first_seen_chain_epoch: chain_epoch,
            },
        )
    }

    fn append_retention_test_events(
        store: &RocksDbCanonicalStore,
        events: impl IntoIterator<Item = MempoolEvent>,
    ) -> Result<(), CanonicalStoreError> {
        for event in events {
            let _envelope = store.append_mempool_event(event, UnixTimestampMillis::new(1_000))?;
        }
        Ok(())
    }

    fn swap_raw_mempool_event(
        store_path: &std::path::Path,
        event_sequence: u64,
        replacement: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let column_families = DB::list_cf(&Options::default(), store_path)?;
        let database = DB::open_cf(&Options::default(), store_path, column_families)?;
        let event_family = database
            .cf_handle("mempool_event")
            .ok_or("mempool event column family must exist")?;
        let key = event_sequence.to_be_bytes();
        let previous = database.get_cf(&event_family, key)?;
        if let Some(replacement) = replacement {
            database.put_cf(&event_family, key, replacement)?;
        } else {
            database.delete_cf(&event_family, key)?;
        }
        database.flush_cf(&event_family)?;
        Ok(previous)
    }

    fn authenticated<Message>(message: Message) -> Request<Message> {
        let mut request = Request::new(message);
        request.metadata_mut().insert(
            "authorization",
            tonic::metadata::MetadataValue::from_static("Bearer fixture-control-token"),
        );
        request
    }

    fn checkpoint_authenticated<Message>(message: Message) -> Request<Message> {
        let mut request = authenticated(message);
        request.metadata_mut().insert(
            "x-zinder-checkpoint-authorization",
            tonic::metadata::MetadataValue::from_static("Bearer fixture-checkpoint-token"),
        );
        request
    }

    fn checkpoint_only<Message>(message: Message) -> Request<Message> {
        let mut request = Request::new(message);
        request.metadata_mut().insert(
            "x-zinder-checkpoint-authorization",
            tonic::metadata::MetadataValue::from_static("Bearer fixture-checkpoint-token"),
        );
        request
    }

    fn checkpoint_request(
        candidate_id: &str,
        temporary_path: &std::path::Path,
        expected_fence: CanonicalWriterFence,
    ) -> Result<CreateCanonicalOwnerCheckpointRequest, std::io::Error> {
        let root = fs::canonicalize(temporary_path.join("checkpoint-staging"))?;
        Ok(CreateCanonicalOwnerCheckpointRequest {
            candidate_id: candidate_id.to_owned(),
            staging_root_binding: checkpoint_staging_root_binding(&root),
            expected_fence: Some(expected_fence),
        })
    }

    fn checkpoint_readmission_request(
        checkpoint: &zinder_proto::v1::ingest::CreateCanonicalOwnerCheckpointResponse,
        temporary_path: &std::path::Path,
        expected_fence: CanonicalWriterFence,
        expected_database_identity: Vec<u8>,
    ) -> Result<ReadmitCanonicalOwnerCheckpointRequest, std::io::Error> {
        let root = fs::canonicalize(temporary_path.join("checkpoint-staging"))?;
        Ok(ReadmitCanonicalOwnerCheckpointRequest {
            candidate_id: checkpoint.candidate_id.clone(),
            staging_root_binding: checkpoint_staging_root_binding(&root),
            expected_fence: Some(expected_fence),
            expected_database_identity,
        })
    }

    pub(crate) fn published_fixture_store(
        path: &std::path::Path,
    ) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
        let activations = fixture_activations()?;
        let tip = BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32]));
        let plan = CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            tip,
            zinder_store::RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(1)?,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            path,
            CanonicalStoreWorkload::Wallet,
            plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        builder.bulk_load_blocks([Ok::<_, std::io::Error>(fixture_block())])?;
        builder.load_subtree_roots(std::iter::empty())?;
        builder.confirm_source_tip_checkpoint(&zinder_core::CommitmentTreeCheckpoint::new(
            tip,
            1,
            zinder_core::CommitmentTreeFrontiers::default(),
        ))?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            tip,
            UnixTimestampMillis::new(1_750_000_000_000),
        ))?;
        Ok(validated.publish_baseline(publication)?)
    }

    fn fixture_activations()
    -> Result<zinder_core::NetworkUpgradeActivations, zinder_core::NetworkUpgradeActivationsError>
    {
        let activations = [
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
        .map(|(index, name)| zinder_core::NetworkUpgradeActivation {
            branch_id: zinder_core::ConsensusBranchId::new(
                u32::try_from(index).unwrap_or(u32::MAX).saturating_add(1),
            ),
            activation_height: BlockHeight::new(
                u32::try_from(index).unwrap_or(u32::MAX).saturating_add(1),
            ),
            name: name.to_owned(),
        })
        .collect();
        zinder_core::NetworkUpgradeActivations::new(Network::ZcashTestnet, activations)
    }

    fn fixture_block() -> CanonicalBuildBlock {
        let raw_transaction_bytes = vec![7];
        let transaction_id = TransactionId::from_bytes([7; 32]);
        let transaction = fixture_transaction(&raw_transaction_bytes, transaction_id);
        let height = BlockHeight::new(1);
        let facts = CanonicalBlockFacts {
            block_header: BlockHeaderArtifact::new(
                height,
                BlockHash::from_bytes([1; 32]),
                Network::ZcashTestnet.genesis_hash(),
                [3; 32],
                [4; 32],
                1,
                0x1d00_ffff,
                [5; 32],
                4,
                128,
            ),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[]),
            transactions: vec![transaction],
        };
        let replay_envelope = encode_canonical_block_replay(
            &facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        );
        CanonicalBuildBlock {
            compact_block: CompactBlockArtifact::empty(
                BlockId::new(height, facts.block_header.block_hash),
                facts.block_header.parent_hash,
                1,
                CompactChainMetadata {
                    sapling_commitment_tree_size: 0,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            ),
            replay_envelope,
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            tree_state_checkpoint: Some(zinder_core::CommitmentTreeCheckpoint::new(
                BlockId::new(height, facts.block_header.block_hash),
                1,
                zinder_core::CommitmentTreeFrontiers::default(),
            )),
            block_final_note_commitment_roots: None,
            transaction_blobs: vec![TransactionBlobArtifact::new(
                TransactionLocation::new(transaction_id, height, facts.block_header.block_hash, 0),
                raw_transaction_bytes,
            )],
            block_blob: None,
            facts,
        }
    }

    fn fixture_transaction(
        raw_transaction_bytes: &[u8],
        transaction_id: TransactionId,
    ) -> CanonicalTransactionFacts {
        CanonicalTransactionFacts {
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
                size_bytes: 1,
                counts: TransactionComponentCounts::EMPTY,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                privacy_shape: PrivacyShape::Unclassified,
                is_coinbase: true,
                unsupported_sections: vec![UnsupportedSection::FutureVersionHeader],
            },
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                raw_transaction_bytes,
            ),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs: Vec::new(),
            transparent_outputs: Vec::new(),
        }
    }
}
