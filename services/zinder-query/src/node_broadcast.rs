//! Transaction broadcast through the ordered node list, with every endpoint
//! verified on first contact.

use std::sync::atomic::{AtomicU64, Ordering};
use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use futures_util::future::join_all;
use tokio::{
    sync::{Mutex, watch},
    time::Instant,
};
use zebra_chain::{serialization::ZcashDeserializeInto as _, transaction::Transaction};
use zinder_core::{
    BlockHeight, BroadcastAccepted, BroadcastRejected, BroadcastRejectionReason, ChainEpochId,
    ConsensusBranchId, NetworkUpgradeActivations, RawTransactionBytes, StandardFee,
    TransactionBroadcastOutcome, TransactionId,
};
use zinder_source::{
    BroadcastNodeProbe, NodeCapability, NodeSet, NodeSetSwitch, NodeTarget, SourceError,
    SourceFailureClass, TransactionBroadcaster, ZebraJsonRpcSource,
};
use zinder_store::{CanonicalActivationSchedule, CanonicalEventFence};

use crate::{
    UpstreamNodeCapabilities, WalletServingPairSlot, WalletServingReadPair,
    broadcast_byte_budget::BroadcastReservation,
};

/// First exclusion of an endpoint that answered verification with evidence
/// that it must not receive transactions. Each later exclusion doubles it.
const EXCLUSION_INITIAL_BACKOFF: Duration = Duration::from_secs(30);

/// Longest an endpoint stays excluded before it is verified again.
const EXCLUSION_MAX_BACKOFF: Duration = Duration::from_mins(5);

/// Longest an endpoint stays verified against an earlier basis. Light-wallet node
/// verification uses the same bound for capability and schedule evidence at a fixed URL.
///
/// It counts from when the serving pair was first seen on a new basis, or
/// from the start of an inconclusive repeat against the same basis, and
/// covers a verification that has not returned yet as well as one that was
/// inconclusive, for example because the node was one block behind.
const STALE_VERIFICATION_GRACE: Duration = Duration::from_mins(1);

/// Interval between verification rounds while an endpoint is verified only
/// against an earlier basis.
pub(crate) const STALE_VERIFICATION_RETRY: Duration = Duration::from_secs(2);

const ATTEMPTS_COUNTER: &str = "zinder_wallet_broadcast_endpoint_attempts_total";
const SWITCHES_COUNTER: &str = "zinder_wallet_broadcast_endpoint_switches_total";
const STATE_GAUGE: &str = "zinder_wallet_broadcast_endpoint_state";

/// The ordered node endpoints a wallet query broadcasts transactions through.
///
/// Position 0 is `[node]`, followed by the `[node_fallbacks.N]` endpoints in
/// position order. A reader starts without contacting any of them, so each
/// endpoint is verified on first contact instead: `rpc.discover` must list
/// `sendrawtransaction`, the node's activation table must agree with the
/// writer-admitted schedule of the serving pair through the height after its
/// visible tip, which is the block a broadcast transaction targets, and the
/// node's best chain must hold the pair's visible tip block. A broadcast
/// reaches only verified endpoints, so no transaction bytes reach a node on
/// another network or chain.
///
/// A broadcast tries the verified endpoints in order, starting at the
/// preferred one, which is position 0 until a failover moves it. It moves
/// to the next endpoint when one is unreachable, including when its broadcast
/// timeout expires, or rejects the transaction for its consensus branch, and
/// returns the first other answer. When the broadcast moved past an earlier
/// endpoint for one of those two causes, the endpoint that answered becomes
/// the preferred one, with no timed failback; an endpoint skipped only
/// because it is not verified leaves the preference in place. When an earlier
/// endpoint in the same request was unreachable, it may have accepted the
/// transaction, so a later duplicate or queued answer is reported as accepted
/// with the transaction id computed from the submitted bytes, and a broadcast
/// no endpoint answered fails as unreachable even when another endpoint
/// rejected it for its consensus branch.
///
/// A passed verification keeps the endpoint's activation table. A broadcast
/// reaches the endpoint only while that table agrees with the writer-admitted
/// schedule of the newest captured serving pair, through the height a
/// broadcast targets under that pair. A schedule revision, or a tip that
/// reaches an activation in either table, therefore stops broadcasts to a
/// disagreeing endpoint at once, without contacting it. The endpoint is
/// verified again once the pair's schedule revision or the network upgrade
/// active at the height after its tip changes. Until that check passes, the
/// endpoint keeps its earlier verification for up to one minute from when
/// the new basis was first seen, while the check waits on the node and after
/// it was inconclusive, but only while its kept table agrees. An endpoint
/// that lacks `sendrawtransaction` or whose table disagrees is excluded, and
/// verified again after a backoff that starts at 30 seconds and doubles to 5
/// minutes. Clones share every endpoint's verification and the preferred
/// position.
pub struct OrderedBroadcaster<Source> {
    inner: Arc<OrderedBroadcasterInner<Source>>,
}

struct OrderedBroadcasterInner<Source> {
    nodes: NodeSet<Source>,
    /// Moves the set's active endpoint, which is the preferred one, to the
    /// endpoint that answered a broadcast.
    preference: NodeSetSwitch<Source>,
    verifications: EndpointVerifications,
}

impl<Source> Clone for OrderedBroadcaster<Source> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<Source> OrderedBroadcaster<Source> {
    /// Broadcasts through `primary`, position 0, and then `fallbacks` in
    /// order, every endpoint unverified.
    #[must_use]
    pub fn new(primary: Source, fallbacks: impl IntoIterator<Item = Source>) -> Self {
        let (nodes, preference) = NodeSet::new(primary, fallbacks);
        Self::from_node_set(nodes, preference)
    }

    fn from_node_set(nodes: NodeSet<Source>, preference: NodeSetSwitch<Source>) -> Self {
        let verifications = EndpointVerifications {
            endpoints: (0..nodes.endpoint_count())
                .map(EndpointVerification::new)
                .collect(),
            preferred: nodes.subscribe_switches(),
            admission: Arc::new(parking_lot::Mutex::new(None)),
        };
        Self {
            inner: Arc::new(OrderedBroadcasterInner {
                nodes,
                preference,
                verifications,
            }),
        }
    }

    /// Returns how many endpoints the broadcaster holds, `[node]` included.
    #[must_use]
    pub fn endpoint_count(&self) -> usize {
        self.inner.nodes.endpoint_count()
    }

    /// Returns the position a broadcast tries first.
    #[must_use]
    pub fn preferred_position(&self) -> usize {
        self.inner.nodes.active_position()
    }

    /// Returns the capabilities that the first verified endpoint in
    /// preference order advertised when it passed verification, or `None`
    /// while no endpoint is verified.
    #[must_use]
    pub fn verified_capabilities(&self) -> Option<UpstreamNodeCapabilities> {
        self.inner.verifications.verified_capabilities()
    }

    /// Returns the verified endpoints with their positions, in preference
    /// order.
    pub(crate) fn verified_endpoints(&self) -> impl Iterator<Item = (usize, &Source)> {
        let verifications = &self.inner.verifications;
        verifications
            .preference_order()
            .filter(|position| verifications.is_verified(*position))
            .filter_map(|position| {
                self.inner
                    .nodes
                    .endpoint(position)
                    .map(|source| (position, source))
            })
    }

    /// Returns every position once, in broadcast preference order.
    pub(crate) fn preference_order(&self) -> impl Iterator<Item = usize> {
        self.inner.verifications.preference_order()
    }

    /// Returns whether the endpoint at `position` is verified only against
    /// an earlier basis: the serving pair moved to another basis, or a repeat
    /// against the same basis was inconclusive.
    pub(crate) fn awaits_reverification(&self, position: usize) -> bool {
        self.inner
            .verifications
            .endpoints
            .get(position)
            .is_some_and(EndpointVerification::awaits_reverification)
    }

    /// Returns whether `pair` ends what the last verification of the
    /// endpoint at `position`, made against `basis`, established: `pair`
    /// changes the verification basis, or a broadcast under `pair` targets a
    /// height at which the endpoint's table disagrees with the
    /// writer-admitted schedule.
    pub(crate) fn verification_ended_by(
        &self,
        position: usize,
        basis: VerificationBasis,
        pair: &WalletServingReadPair,
    ) -> bool {
        VerificationBasis::of(pair) != basis
            || self
                .inner
                .verifications
                .endpoints
                .get(position)
                .is_some_and(|verification| {
                    !verification.table_agrees_through(
                        pair.network_upgrade_activations(),
                        broadcast_height(pair.canonical_fence()),
                    )
                })
    }

    /// Makes `position` the preferred endpoint after it answered while every
    /// verified endpoint before it in preference order was unreachable.
    pub(crate) fn prefer_after_unreachable(&self, position: usize) {
        self.prefer(position, PassOver::NodeUnreachable);
    }

    pub(crate) fn verifications(&self) -> EndpointVerifications {
        self.inner.verifications.clone()
    }
}

impl OrderedBroadcaster<ZebraJsonRpcSource> {
    pub(crate) fn primary_source(&self) -> Option<&ZebraJsonRpcSource> {
        self.inner.nodes.endpoint(0)
    }

    /// Builds the JSON-RPC source of `primary`, position 0, and of every
    /// fallback target after it, without contacting any node.
    ///
    /// # Errors
    ///
    /// Returns the source construction failure of the first target whose
    /// address or authentication cannot be used.
    pub fn connect(primary: &NodeTarget, fallbacks: &[NodeTarget]) -> Result<Self, SourceError> {
        let (nodes, preference) = NodeSet::connect(primary, fallbacks)?;
        Ok(Self::from_node_set(nodes, preference))
    }
}

impl<Source> OrderedBroadcaster<Source>
where
    Source: BroadcastNodeProbe,
{
    /// Verifies every endpoint that needs it against the pair `pair_slot`
    /// currently serves: an unverified endpoint, one verified against an
    /// earlier schedule revision or broadcast-height network upgrade, and an
    /// excluded endpoint whose backoff has passed.
    ///
    /// Endpoints are verified concurrently, and the call returns once every
    /// one has an outcome. Concurrent calls share each endpoint's
    /// verification, and an endpoint whose verification still holds is not
    /// contacted.
    ///
    /// # Errors
    ///
    /// Fails only when no endpoint holds a verification, with the failure of
    /// the first endpoint in preference order: its transport or protocol
    /// failure, [`SourceError::NodeCapabilityMissing`] when it does not list
    /// `sendrawtransaction`, [`SourceError::BlockUnavailable`] when it has not
    /// reached the visible tip, or [`SourceError::SourceProtocolMismatch`]
    /// when its activation table differs from the writer-admitted schedule at
    /// or below the height after the visible tip or its best chain holds
    /// another block at the visible tip height.
    pub async fn verify(&self, pair_slot: &WalletServingPairSlot) -> Result<(), SourceError> {
        let target = self.inner.verifications.target(pair_slot);
        let outcomes = join_all(
            self.inner
                .verifications
                .preference_order()
                .map(|position| self.verify_against(position, &target)),
        )
        .await;
        let mut first_failure = None;
        for outcome in outcomes {
            match outcome {
                Ok(()) => return Ok(()),
                Err(error) => {
                    first_failure.get_or_insert(error);
                }
            }
        }
        Err(first_failure.unwrap_or_else(no_verified_endpoint))
    }

    /// Verifies the endpoint at `position` alone, as [`Self::verify`]
    /// verifies each endpoint, so a slow endpoint delays no other one.
    pub(crate) async fn verify_endpoint(
        &self,
        position: usize,
        pair_slot: &WalletServingPairSlot,
    ) -> Result<(), SourceError> {
        let target = self.inner.verifications.target(pair_slot);
        self.verify_against(position, &target).await
    }

    async fn verify_against(
        &self,
        position: usize,
        target: &VerificationTarget,
    ) -> Result<(), SourceError> {
        let Some(source) = self.inner.nodes.endpoint(position) else {
            return Err(no_verified_endpoint());
        };
        self.inner
            .verifications
            .verify(position, source, target)
            .await
    }
}

#[async_trait]
impl<Source> TransactionBroadcaster for OrderedBroadcaster<Source>
where
    Source: TransactionBroadcaster,
{
    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, SourceError> {
        let mut attempt = BroadcastAttempt::default();
        for position in self.inner.verifications.preference_order() {
            let Some(source) = self.inner.nodes.endpoint(position) else {
                continue;
            };
            if !self.inner.verifications.is_verified(position) {
                continue;
            }
            let outcome = zinder_runtime::trace_node_rpc(
                source.broadcast_transaction(raw_transaction.clone()),
            )
            .await;
            record_attempt(position, &outcome);
            match outcome {
                Ok(TransactionBroadcastOutcome::Rejected(rejected))
                    if rejected.kind == BroadcastRejectionReason::BadConsensusBranch =>
                {
                    attempt.pass_over(PassOver::BadConsensusBranch);
                    attempt.branch_rejection = Some(rejected);
                }
                Ok(answer) => {
                    if let Some(cause) = attempt.first_pass_over {
                        self.prefer(position, cause);
                    }
                    return Ok(attempt.settle(answer, raw_transaction, position).await);
                }
                Err(error)
                    if error.upstream_classification() == SourceFailureClass::NodeUnreachable =>
                {
                    attempt.pass_over(PassOver::NodeUnreachable);
                    attempt.unreachable = Some(error);
                }
                Err(error)
                    if error.upstream_classification() == SourceFailureClass::NodeRateLimited =>
                {
                    attempt.pass_over(PassOver::NodeRateLimited);
                    attempt.rate_limited = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        attempt.exhausted()
    }

    /// Reads the recommended fee from the first verified endpoint, in
    /// preference order, that answers.
    ///
    /// Moves to the next endpoint when one is unreachable, refuses the call
    /// with HTTP 429, or does not implement `getstandardfee`, and leaves the
    /// preferred endpoint in place.
    /// Fails as unreachable when an endpoint was unreachable and none
    /// answered. Also fails as unreachable when an endpoint still awaits its
    /// first-contact verification and none answered, because that endpoint
    /// may implement the method. Fails as a missing capability only when
    /// every endpoint that could answer is verified and lacks the method.
    /// Any other failure, such as an unusable reply, ends the read with that
    /// endpoint's error.
    async fn standard_fee(&self) -> Result<StandardFee, SourceError> {
        let verifications = &self.inner.verifications;
        let mut unreachable = None;
        let mut capability_missing = None;
        let mut support_unknown = false;
        for position in verifications.preference_order() {
            let Some(source) = self.inner.nodes.endpoint(position) else {
                continue;
            };
            if !verifications.is_verified(position) {
                support_unknown |= verifications.awaits_verification(position);
                continue;
            }
            match source.standard_fee().await {
                Ok(fee) => return Ok(fee),
                Err(error)
                    if matches!(
                        error.upstream_classification(),
                        SourceFailureClass::NodeUnreachable | SourceFailureClass::NodeRateLimited
                    ) =>
                {
                    unreachable = Some(error);
                }
                Err(error @ SourceError::NodeCapabilityMissing { .. }) => {
                    capability_missing = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(error) = unreachable {
            return Err(error);
        }
        match capability_missing {
            Some(error) if !support_unknown => Err(error),
            Some(_) | None => Err(SourceError::NodeUnavailable {
                reason: "no node endpoint that may implement getstandardfee has passed \
                         first-contact verification"
                    .to_owned(),
            }),
        }
    }
}

impl<Source> OrderedBroadcaster<Source> {
    /// Makes `position` the preferred endpoint after it answered a broadcast
    /// that passed over the endpoints before it for `cause`.
    fn prefer(&self, position: usize, cause: PassOver) {
        let from_position = self.preferred_position();
        if !self.inner.preference.switch_to(position) {
            return;
        }
        let cause = cause.label();
        metrics::counter!(
            SWITCHES_COUNTER,
            "from_position" => from_position.to_string(),
            "to_position" => position.to_string(),
            "cause" => cause,
        )
        .increment(1);
        tracing::warn!(
            target: "zinder::wallet_serving",
            event = "broadcast_endpoint_switched",
            from_position,
            to_position = position,
            cause,
            "transaction broadcast now prefers another node endpoint"
        );
    }
}

/// Why a broadcast moved past an endpoint without returning its answer.
#[derive(Clone, Copy, Debug)]
enum PassOver {
    NodeUnreachable,
    NodeRateLimited,
    BadConsensusBranch,
}

impl PassOver {
    const fn label(self) -> &'static str {
        match self {
            Self::NodeUnreachable => "node_unreachable",
            Self::NodeRateLimited => "node_rate_limited",
            Self::BadConsensusBranch => "bad_consensus_branch",
        }
    }
}

/// What one broadcast learned from the endpoints it moved past.
#[derive(Default)]
struct BroadcastAttempt {
    first_pass_over: Option<PassOver>,
    /// The last failure of an unreachable endpoint. Such an endpoint may have
    /// received the transaction before it failed.
    unreachable: Option<SourceError>,
    /// The last HTTP 429 refusal. The refusing endpoint did not read the
    /// transaction, so the refusal is retryable.
    rate_limited: Option<SourceError>,
    branch_rejection: Option<BroadcastRejected>,
}

impl BroadcastAttempt {
    fn pass_over(&mut self, cause: PassOver) {
        self.first_pass_over.get_or_insert(cause);
    }

    /// Returns `answer`, reporting a duplicate or queued answer as accepted
    /// when an earlier endpoint may have accepted the transaction already.
    #[allow(
        clippy::wildcard_enum_match_arm,
        reason = "only duplicate and queued answers can follow an earlier delivery"
    )]
    async fn settle(
        self,
        answer: TransactionBroadcastOutcome,
        raw_transaction: RawTransactionBytes,
        position: usize,
    ) -> TransactionBroadcastOutcome {
        if self.unreachable.is_none() {
            return answer;
        }
        match answer {
            TransactionBroadcastOutcome::Duplicate(_) | TransactionBroadcastOutcome::Queued(_) => {
                let Some(transaction_id) = submitted_transaction_id(raw_transaction).await else {
                    return answer;
                };
                tracing::info!(
                    target: "zinder::wallet_serving",
                    event = "broadcast_reported_accepted_after_unreachable_endpoint",
                    position,
                    "node endpoint already knew a transaction an unreachable endpoint may have accepted; reporting it accepted"
                );
                TransactionBroadcastOutcome::Accepted(BroadcastAccepted { transaction_id })
            }
            answer => answer,
        }
    }

    /// Ends a broadcast that no endpoint answered with anything other than a
    /// consensus-branch rejection. An unreachable endpoint may have accepted
    /// the transaction, so its failure takes precedence over a rejection. A
    /// rate-limited endpoint may accept the transaction on a retry, so its
    /// refusal also takes precedence over a rejection.
    fn exhausted(self) -> Result<TransactionBroadcastOutcome, SourceError> {
        if let Some(error) = self.unreachable.or(self.rate_limited) {
            return Err(error);
        }
        self.branch_rejection
            .map(TransactionBroadcastOutcome::Rejected)
            .ok_or_else(no_verified_endpoint)
    }
}

fn no_verified_endpoint() -> SourceError {
    SourceError::NodeUnavailable {
        reason: "no node endpoint has passed first-contact verification; \
                 the transaction was not sent"
            .to_owned(),
    }
}

/// Computes the transaction id of the submitted bytes, or `None` when they do
/// not parse as a transaction this build decodes or the computation does not
/// finish.
///
/// Parsing and hashing a large shielded transaction can hold a thread for
/// milliseconds, so it runs on the blocking pool instead of the async worker
/// serving other wallet requests. That task outlives a cancelled broadcast
/// and keeps the transaction until it runs, so it also keeps the broadcast's
/// budget reservation.
async fn submitted_transaction_id(raw_transaction: RawTransactionBytes) -> Option<TransactionId> {
    let reservation = BroadcastReservation::current();
    tokio::task::spawn_blocking(move || {
        let _reservation = reservation;
        raw_transaction
            .as_slice()
            .zcash_deserialize_into::<Transaction>()
            .ok()
            .map(|transaction| TransactionId::from_bytes(transaction.hash().0))
    })
    .await
    .ok()
    .flatten()
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "non-exhaustive broadcast outcomes from zinder-core are counted as unknown"
)]
pub(crate) fn record_attempt(
    position: usize,
    outcome: &Result<TransactionBroadcastOutcome, SourceError>,
) {
    let outcome = match outcome {
        Ok(TransactionBroadcastOutcome::Accepted(_)) => "accepted",
        Ok(TransactionBroadcastOutcome::Duplicate(_)) => "duplicate",
        Ok(TransactionBroadcastOutcome::InvalidEncoding(_)) => "invalid_encoding",
        Ok(TransactionBroadcastOutcome::Queued(_)) => "queued",
        Ok(TransactionBroadcastOutcome::Rejected(rejected))
            if rejected.kind == BroadcastRejectionReason::BadConsensusBranch =>
        {
            "bad_consensus_branch"
        }
        Ok(TransactionBroadcastOutcome::Rejected(_)) => "rejected",
        Ok(_) => "unknown",
        Err(error) => error.upstream_classification().label(),
    };
    metrics::counter!(
        ATTEMPTS_COUNTER,
        "position" => position.to_string(),
        "outcome" => outcome,
    )
    .increment(1);
}

/// The verification state of every endpoint, in position order, and the
/// preferred position, shared by every clone of one broadcaster.
#[derive(Clone)]
pub(crate) struct EndpointVerifications {
    endpoints: Arc<[EndpointVerification]>,
    preferred: watch::Receiver<usize>,
    /// The evidence of the newest serving pair, by chain epoch and then
    /// schedule revision, that a verification or a wallet broadcast
    /// captured, which a broadcast checks every kept activation table
    /// against.
    ///
    /// An endpoint's verification state changes only while this lock is
    /// held, so a verification whose basis differs from this pair's is
    /// always marked stale.
    admission: Arc<parking_lot::Mutex<Option<Arc<VerificationTarget>>>>,
}

impl EndpointVerifications {
    /// Returns every position once, starting at the preferred one.
    fn preference_order(&self) -> impl Iterator<Item = usize> + use<> {
        let count = self.endpoints.len();
        let preferred = *self.preferred.borrow();
        (0..count).map(move |offset| (preferred + offset) % count)
    }

    fn is_verified(&self, position: usize) -> bool {
        self.verified_capabilities_at(position).is_some()
    }

    /// Returns whether the endpoint at `position` is not in force yet and a
    /// later verification can still admit it, so what it implements is
    /// unknown. An excluded endpoint was refused and is not waiting.
    fn awaits_verification(&self, position: usize) -> bool {
        self.endpoints.get(position).is_some_and(|verification| {
            !matches!(
                **verification.state.load(),
                VerificationState::Excluded { .. }
            )
        })
    }

    pub(crate) fn verified_capabilities(&self) -> Option<UpstreamNodeCapabilities> {
        self.preference_order()
            .find_map(|position| self.verified_capabilities_at(position))
    }

    fn verified_capabilities_at(&self, position: usize) -> Option<UpstreamNodeCapabilities> {
        // Held through the state load: `track` and `record` take the same
        // lock, so the check never pairs a superseded schedule with a
        // verification recorded after a newer one was tracked.
        let admission = self.admission.lock();
        self.endpoints
            .get(position)?
            .state
            .load()
            .in_force(admission.as_deref()?)
            .cloned()
    }

    /// Admits a broadcast only to endpoints whose kept table agrees with the
    /// writer-admitted schedule of `pair` through the height a broadcast
    /// targets under it.
    pub(crate) fn track_broadcast_height(&self, pair: &WalletServingReadPair) {
        self.track(&Arc::new(VerificationTarget::of(pair)));
    }

    /// Tracks `captured` unless a newer pair was tracked, so a caller that
    /// captured an older pair cannot replace a newer one, and starts the
    /// grace of every verification of another basis.
    fn track(&self, captured: &Arc<VerificationTarget>) {
        let mut admission = self.admission.lock();
        if admission
            .as_ref()
            .is_some_and(|tracked| tracked.publication_order() > captured.publication_order())
        {
            return;
        }
        let seen_at = Instant::now();
        for verification in self.endpoints.iter() {
            verification.supersede(captured.basis, seen_at);
        }
        *admission = Some(Arc::clone(captured));
    }

    /// Captures the evidence a verification checks against and tracks it.
    fn target(&self, pair_slot: &WalletServingPairSlot) -> Arc<VerificationTarget> {
        let target = Arc::new(VerificationTarget::of(&pair_slot.capture_ephemeral()));
        self.track(&target);
        target
    }

    /// Verifies the endpoint at `position`, reached through `source`, against
    /// `target`, unless its verification still holds against it.
    async fn verify(
        &self,
        position: usize,
        source: &impl BroadcastNodeProbe,
        target: &VerificationTarget,
    ) -> Result<(), SourceError> {
        let Some(verification) = self.endpoints.get(position) else {
            return Err(no_verified_endpoint());
        };
        if let Some(settled) = verification.settled(target) {
            return settled;
        }
        let _attempt = verification.attempt.lock().await;
        if let Some(settled) = verification.settled(target) {
            return settled;
        }
        // The attempt lock is shared with readiness probes, so the attempt
        // calls through the endpoint's verification view, never behind
        // forwarded calls that hold its shared call slots.
        let view = source.verification_view();
        let outcome = verify_node_endpoint(view.as_ref().unwrap_or(source), target).await;
        let admission = self.admission.lock();
        verification.record(outcome, target, admission.as_deref())
    }
}

/// The verification of one endpoint.
struct EndpointVerification {
    position: usize,
    /// Held while a verification runs, so concurrent callers share it.
    attempt: Mutex<()>,
    state: ArcSwap<VerificationState>,
}

#[derive(Clone)]
enum VerificationState {
    /// No verification has run yet.
    Unchecked,
    /// The last verification could not verify the endpoint.
    Unverified,
    Verified {
        capabilities: UpstreamNodeCapabilities,
        basis: VerificationBasis,
        /// The endpoint's activation table as the verification fetched it.
        node_activations: NetworkUpgradeActivations,
        /// When the serving pair was first seen on another basis, or when a
        /// repeat against `basis` was first inconclusive, or `None` while
        /// `basis` is the latest one seen.
        stale_since: Option<Instant>,
        /// Whether a repeat since `stale_since` was inconclusive.
        retrying: bool,
    },
    Excluded {
        refusal: VerificationRefusal,
        retry_at: Instant,
        backoff: Duration,
    },
}

impl VerificationState {
    const fn label(&self) -> &'static str {
        match self {
            Self::Unchecked | Self::Unverified => "unverified",
            Self::Verified { .. } => "verified",
            Self::Excluded { .. } => "excluded",
        }
    }

    /// Returns the capabilities of a verification a broadcast under
    /// `admission` may rely on now: one of the latest basis seen, or one of
    /// an earlier basis whose [`STALE_VERIFICATION_GRACE`] has not ended,
    /// whose kept table agrees with the writer-admitted schedule of
    /// `admission` through the height a broadcast targets.
    ///
    /// The grace ends on time even while no later verification completes, so
    /// a slow verification round cannot extend it.
    fn in_force(&self, admission: &VerificationTarget) -> Option<&UpstreamNodeCapabilities> {
        match self {
            Self::Verified {
                capabilities,
                node_activations,
                stale_since,
                ..
            } if !stale_since.is_some_and(stale_grace_ended)
                && admission.agrees_with(node_activations) =>
            {
                Some(capabilities)
            }
            Self::Unchecked | Self::Unverified | Self::Verified { .. } | Self::Excluded { .. } => {
                None
            }
        }
    }
}

/// Returns whether the grace of a verification first left stale at `since`
/// has ended.
fn stale_grace_ended(since: Instant) -> bool {
    since.elapsed() >= STALE_VERIFICATION_GRACE
}

impl EndpointVerification {
    fn new(position: usize) -> Self {
        let verification = Self {
            position,
            attempt: Mutex::new(()),
            state: ArcSwap::from_pointee(VerificationState::Unchecked),
        };
        verification.record_state_gauge(VerificationState::Unchecked.label());
        verification
    }

    /// Returns whether a kept table agrees with `schedule` through
    /// `broadcast_height`; an endpoint without a verification has no table
    /// to disagree.
    fn table_agrees_through(
        &self,
        schedule: &NetworkUpgradeActivations,
        broadcast_height: BlockHeight,
    ) -> bool {
        match &**self.state.load() {
            VerificationState::Verified {
                node_activations, ..
            } => node_activations.preserves_through(schedule, broadcast_height),
            VerificationState::Unchecked
            | VerificationState::Unverified
            | VerificationState::Excluded { .. } => true,
        }
    }

    fn awaits_reverification(&self) -> bool {
        matches!(
            **self.state.load(),
            VerificationState::Verified {
                stale_since: Some(_),
                ..
            }
        )
    }

    /// Starts the grace of a current verification of a basis other than
    /// `seen`, the basis of a serving pair seen at `seen_at`.
    fn supersede(&self, seen: VerificationBasis, seen_at: Instant) {
        let state = self.state.load();
        if let VerificationState::Verified {
            capabilities,
            basis,
            node_activations,
            stale_since: None,
            ..
        } = &**state
            && *basis != seen
        {
            self.state.store(Arc::new(VerificationState::Verified {
                capabilities: capabilities.clone(),
                basis: *basis,
                node_activations: node_activations.clone(),
                stale_since: Some(seen_at),
                retrying: false,
            }));
        }
    }

    /// Returns the outcome of a verification that still holds against
    /// `target`, or `None` when the endpoint must be verified again.
    ///
    /// A stale verification is never settled, even when the pair returns to
    /// its basis, so a later round either renews it or lets its grace end.
    fn settled(&self, target: &VerificationTarget) -> Option<Result<(), SourceError>> {
        match &**self.state.load() {
            VerificationState::Verified {
                basis: verified,
                node_activations,
                stale_since: None,
                ..
            } if *verified == target.basis && target.agrees_with(node_activations) => Some(Ok(())),
            VerificationState::Excluded {
                refusal, retry_at, ..
            } if Instant::now() < *retry_at => Some(Err(refusal.error())),
            VerificationState::Unchecked
            | VerificationState::Unverified
            | VerificationState::Verified { .. }
            | VerificationState::Excluded { .. } => None,
        }
    }

    /// Records the outcome of a verification against `target` while a
    /// broadcast checks kept tables against `admission`.
    ///
    /// A pass against a basis other than the one of `admission`, which the
    /// serving pair moved to while the verification ran, is stale from when
    /// `target` was captured.
    fn record(
        &self,
        outcome: Result<PassedVerification, VerificationFailure>,
        target: &VerificationTarget,
        admission: Option<&VerificationTarget>,
    ) -> Result<(), SourceError> {
        let previous = self.state.load_full();
        let (next, verification) = match outcome {
            Ok(PassedVerification {
                capabilities,
                node_activations,
            }) => {
                if !matches!(*previous, VerificationState::Verified { .. }) {
                    tracing::info!(
                        target: "zinder::wallet_serving",
                        event = "wallet_node_verified",
                        position = self.position,
                        "node endpoint passed first-contact verification; transaction broadcast reaches it"
                    );
                }
                let stale_since = admission
                    .is_some_and(|admission| admission.basis != target.basis)
                    .then_some(target.captured_at);
                (
                    VerificationState::Verified {
                        capabilities,
                        basis: target.basis,
                        node_activations,
                        stale_since,
                        retrying: false,
                    },
                    Ok(()),
                )
            }
            Err(VerificationFailure::Refused(refusal)) => self.excluded(&previous, refusal),
            Err(VerificationFailure::Inconclusive(error)) => {
                self.inconclusive(&previous, error, target)
            }
        };
        self.record_state_gauge(next.label());
        self.state.store(Arc::new(next));
        verification
    }

    fn excluded(
        &self,
        previous: &VerificationState,
        refusal: VerificationRefusal,
    ) -> (VerificationState, Result<(), SourceError>) {
        let backoff = match previous {
            VerificationState::Excluded { backoff, .. } => {
                backoff.saturating_mul(2).min(EXCLUSION_MAX_BACKOFF)
            }
            VerificationState::Unchecked
            | VerificationState::Unverified
            | VerificationState::Verified { .. } => EXCLUSION_INITIAL_BACKOFF,
        };
        let error = refusal.error();
        tracing::warn!(
            target: "zinder::wallet_serving",
            event = "wallet_node_excluded",
            position = self.position,
            failure_class = error.upstream_classification().label(),
            retry_after_secs = backoff.as_secs(),
            error = %error,
            "node endpoint failed first-contact verification; it receives no transaction until it is verified again"
        );
        (
            VerificationState::Excluded {
                refusal,
                retry_at: Instant::now() + backoff,
                backoff,
            },
            Err(error),
        )
    }

    /// Keeps a verification of an earlier basis through
    /// [`STALE_VERIFICATION_GRACE`], since an endpoint one block behind or
    /// mid-reorg at an activation is inconclusive without being wrong.
    ///
    /// A verification whose kept table disagrees with the writer-admitted
    /// schedule of `target` through the height a broadcast targets is wrong
    /// there, so it is not kept.
    fn inconclusive(
        &self,
        previous: &VerificationState,
        error: SourceError,
        target: &VerificationTarget,
    ) -> (VerificationState, Result<(), SourceError>) {
        if let VerificationState::Verified {
            capabilities,
            basis,
            node_activations,
            stale_since,
            retrying,
        } = previous
            && target.agrees_with(node_activations)
        {
            let since = stale_since.unwrap_or(target.captured_at);
            if !stale_grace_ended(since) {
                if !retrying {
                    tracing::warn!(
                        target: "zinder::wallet_serving",
                        event = "wallet_node_reverification_inconclusive",
                        position = self.position,
                        failure_class = error.upstream_classification().label(),
                        error = %error,
                        "node endpoint could not be verified again; it keeps its earlier verification while it is retried"
                    );
                }
                let retained = VerificationState::Verified {
                    capabilities: capabilities.clone(),
                    basis: *basis,
                    node_activations: node_activations.clone(),
                    stale_since: Some(since),
                    retrying: true,
                };
                return (retained, Ok(()));
            }
        }
        if !matches!(previous, VerificationState::Unverified) {
            tracing::warn!(
                target: "zinder::wallet_serving",
                event = "wallet_node_verification_failed",
                position = self.position,
                failure_class = error.upstream_classification().label(),
                error = %error,
                "node endpoint has not passed first-contact verification; transaction broadcast skips it until it does"
            );
        }
        (VerificationState::Unverified, Err(error))
    }

    fn record_state_gauge(&self, current: &'static str) {
        for state in ["verified", "unverified", "excluded"] {
            metrics::gauge!(
                STATE_GAUGE,
                "position" => self.position.to_string(),
                "state" => state,
            )
            .set(if state == current { 1.0 } else { 0.0 });
        }
    }
}

/// The serving-pair evidence a passed verification holds against.
///
/// A schedule revision changes only activations above the tip the writer
/// admitted it at, and the tip can then cross one of them, so a verification
/// is repeated whenever either the revision or the upgrade active at the
/// height after the tip changes. That height is the one a broadcast
/// transaction targets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct VerificationBasis {
    schedule_revision: Option<u64>,
    broadcast_branch: ConsensusBranchId,
}

impl VerificationBasis {
    pub(crate) fn of(pair: &WalletServingReadPair) -> Self {
        Self::new(
            pair.canonical()
                .network_upgrade_schedule()
                .map(CanonicalActivationSchedule::revision),
            pair.network_upgrade_activations(),
            broadcast_height(pair.canonical_fence()),
        )
    }

    fn new(
        schedule_revision: Option<u64>,
        activations: &NetworkUpgradeActivations,
        height: BlockHeight,
    ) -> Self {
        Self {
            schedule_revision,
            broadcast_branch: activations.consensus_branch_id_at(height),
        }
    }
}

/// Returns the height of the block a transaction broadcast now targets, the
/// one after the pair's visible tip.
fn broadcast_height(fence: CanonicalEventFence) -> BlockHeight {
    let tip_height = fence.visible_tip().height;
    tip_height.next().unwrap_or(tip_height)
}

/// The serving-pair evidence a verification checks endpoints against and a
/// broadcast checks kept tables against, captured without retaining the pair
/// across node calls.
pub(crate) struct VerificationTarget {
    basis: VerificationBasis,
    admitted_activations: NetworkUpgradeActivations,
    broadcast_height: BlockHeight,
    fence: CanonicalEventFence,
    captured_at: Instant,
}

impl VerificationTarget {
    pub(crate) fn from_canonical(reader: &dyn crate::CanonicalReader) -> Result<Self, SourceError> {
        let schedule =
            reader
                .network_upgrade_schedule()
                .ok_or(SourceError::SourceProtocolMismatch {
                    reason: "canonical reader has no admitted upgrade schedule",
                })?;
        Ok(Self::new(
            reader.event_fence(),
            schedule.activations(),
            Some(schedule.revision()),
        ))
    }

    pub(crate) fn still_served_by(&self, reader: &dyn crate::CanonicalReader) -> bool {
        self.fence == reader.event_fence()
            && self.basis.schedule_revision
                == reader
                    .network_upgrade_schedule()
                    .map(CanonicalActivationSchedule::revision)
    }

    fn of(pair: &WalletServingReadPair) -> Self {
        Self::new(
            pair.canonical_fence(),
            pair.network_upgrade_activations(),
            pair.canonical()
                .network_upgrade_schedule()
                .map(CanonicalActivationSchedule::revision),
        )
    }

    fn new(
        fence: CanonicalEventFence,
        activations: &NetworkUpgradeActivations,
        revision: Option<u64>,
    ) -> Self {
        let broadcast_height = broadcast_height(fence);
        Self {
            basis: VerificationBasis::new(revision, activations, broadcast_height),
            admitted_activations: activations.clone(),
            broadcast_height,
            fence,
            captured_at: Instant::now(),
        }
    }

    /// Returns whether `node_activations` agrees with the writer-admitted
    /// schedule through the height a broadcast targets.
    fn agrees_with(&self, node_activations: &NetworkUpgradeActivations) -> bool {
        node_activations.preserves_through(&self.admitted_activations, self.broadcast_height)
    }

    /// Orders captured pairs by when they were published: by chain epoch, and
    /// within one epoch by schedule revision, since a writer restart can
    /// admit a revision without moving the chain epoch.
    fn publication_order(&self) -> (ChainEpochId, Option<u64>) {
        (self.fence.chain_epoch_id(), self.basis.schedule_revision)
    }
}

/// Evidence that an endpoint must not receive transactions.
#[derive(Clone, Copy, Debug)]
enum VerificationRefusal {
    CapabilityMissing(NodeCapability),
    ScheduleMismatch,
}

impl VerificationRefusal {
    const fn error(self) -> SourceError {
        match self {
            Self::CapabilityMissing(capability) => {
                SourceError::NodeCapabilityMissing { capability }
            }
            Self::ScheduleMismatch => SourceError::SourceProtocolMismatch {
                reason: "node network-upgrade activations differ from the writer-admitted \
                         schedule at or below the height after the served tip",
            },
        }
    }
}

/// What a passed verification established about an endpoint.
struct PassedVerification {
    capabilities: UpstreamNodeCapabilities,
    node_activations: NetworkUpgradeActivations,
}

enum VerificationFailure {
    /// The endpoint answered with evidence that it must not receive
    /// transactions.
    Refused(VerificationRefusal),
    /// The endpoint could not be verified now, for example because it is
    /// unreachable or has not reached the served tip.
    Inconclusive(SourceError),
}

async fn verify_node_endpoint(
    source: &impl BroadcastNodeProbe,
    target: &VerificationTarget,
) -> Result<PassedVerification, VerificationFailure> {
    let passed = verify_node_schedule(source, target).await?;
    verify_served_tip(source, target)
        .await
        .map_err(VerificationFailure::Inconclusive)?;
    Ok(passed)
}

/// Checks that the endpoint lists `sendrawtransaction` and that its
/// activation table agrees with the writer-admitted schedule through the
/// height after the served tip, without checking the tip itself.
async fn verify_node_schedule(
    source: &impl BroadcastNodeProbe,
    target: &VerificationTarget,
) -> Result<PassedVerification, VerificationFailure> {
    let capabilities = source
        .probe_capabilities()
        .await
        .map_err(VerificationFailure::Inconclusive)?;
    if !capabilities.supports(NodeCapability::TransactionBroadcast) {
        return Err(VerificationFailure::Refused(
            VerificationRefusal::CapabilityMissing(NodeCapability::TransactionBroadcast),
        ));
    }
    let node_activations = source
        .fetch_network_upgrade_activations()
        .await
        .map_err(VerificationFailure::Inconclusive)?;
    if !target.agrees_with(&node_activations) {
        return Err(VerificationFailure::Refused(
            VerificationRefusal::ScheduleMismatch,
        ));
    }
    Ok(PassedVerification {
        capabilities: UpstreamNodeCapabilities::from_probed(capabilities),
        node_activations,
    })
}

async fn verify_served_tip(
    source: &impl BroadcastNodeProbe,
    target: &VerificationTarget,
) -> Result<(), SourceError> {
    if source.holds_block(target.fence.visible_tip()).await? {
        Ok(())
    } else {
        Err(served_tip_mismatch())
    }
}

fn served_tip_mismatch() -> SourceError {
    SourceError::SourceProtocolMismatch {
        reason: "node best chain holds another block at the served tip height",
    }
}

/// Verification of the one node a light-wallet reader forwards reads to.
///
/// The schedule check confirms that the node lists `sendrawtransaction` and
/// that its activation table agrees with the writer-admitted schedule through
/// the height after the served tip. A pass holds for
/// [`STALE_VERIFICATION_GRACE`] against the same basis, and concurrent
/// callers share one check. An inconclusive repeat, such as an unreachable,
/// rate-limited, or lagging node, fails the callers that waited on it but
/// never discards the earlier pass or forces later callers into a full
/// verification of their own.
///
/// The served-tip check confirms that the node's best chain holds a block.
/// Concurrent callers checking the same block share one `getblockhash` call,
/// and no answer is reused once that call returns.
#[derive(Default)]
pub(crate) struct LightWalletNodeVerification {
    schedule: Mutex<LightWalletNodeVerificationSchedule>,
    failed_attempts: AtomicU64,
    tip_checks: TipChecks,
}

#[derive(Default)]
enum LightWalletNodeVerificationSchedule {
    #[default]
    Unverified,
    Refused(VerificationBasis, VerificationRefusal),
    Verified(VerificationBasis, NetworkUpgradeActivations, Instant),
}

impl LightWalletNodeVerification {
    /// Verifies the node's capabilities and activation table against
    /// `target`, reusing a pass against the same basis for up to
    /// [`STALE_VERIFICATION_GRACE`].
    pub(crate) async fn verify_schedule_and_capabilities(
        &self,
        source: &impl BroadcastNodeProbe,
        target: &VerificationTarget,
    ) -> Result<(), SourceError> {
        // Callers already waiting on a failed attempt share its outcome. A caller
        // arriving after it can retry even while earlier waiters are draining.
        let observed_failures = self.failed_attempts.load(Ordering::Acquire);
        let mut schedule = self.schedule.lock().await;
        if matches!(
            &*schedule,
            LightWalletNodeVerificationSchedule::Verified(basis, activations, checked)
                if *basis == target.basis
                    && checked.elapsed() < STALE_VERIFICATION_GRACE
                    && target.agrees_with(activations)
        ) {
            drop(schedule);
            return Ok(());
        }

        if self.failed_attempts.load(Ordering::Relaxed) != observed_failures {
            let shared = match *schedule {
                LightWalletNodeVerificationSchedule::Refused(basis, refusal)
                    if basis == target.basis =>
                {
                    refusal.error()
                }
                LightWalletNodeVerificationSchedule::Unverified
                | LightWalletNodeVerificationSchedule::Refused(..)
                | LightWalletNodeVerificationSchedule::Verified(..) => {
                    SourceError::NodeUnavailable {
                        reason: "concurrent verification did not confirm the served schedule"
                            .to_owned(),
                    }
                }
            };
            drop(schedule);
            return Err(shared);
        }

        // The schedule lock is held across these calls and readiness probes
        // wait on it, so they go through the node's verification view, never
        // behind forwarded calls that hold its shared call slots.
        let view = source.verification_view();
        let outcome = match verify_node_schedule(view.as_ref().unwrap_or(source), target).await {
            Ok(evidence) => {
                *schedule = LightWalletNodeVerificationSchedule::Verified(
                    target.basis,
                    evidence.node_activations,
                    Instant::now(),
                );
                Ok(())
            }
            Err(failure) => {
                self.failed_attempts.fetch_add(1, Ordering::Release);
                match failure {
                    VerificationFailure::Refused(refusal) => {
                        *schedule =
                            LightWalletNodeVerificationSchedule::Refused(target.basis, refusal);
                        Err(refusal.error())
                    }
                    VerificationFailure::Inconclusive(error) => Err(error),
                }
            }
        };
        drop(schedule);
        outcome
    }

    /// Verifies the schedule as [`Self::verify_schedule_and_capabilities`]
    /// does, then that the node's best chain holds the served tip of
    /// `target`.
    pub(crate) async fn verify<Source>(
        &self,
        source: &Source,
        target: &VerificationTarget,
    ) -> Result<(), SourceError>
    where
        Source: BroadcastNodeProbe + Clone,
    {
        self.verify_schedule_and_capabilities(source, target)
            .await?;
        self.verify_served_tip(source, target).await
    }

    /// Verifies the schedule as [`Self::verify`] does, then checks the served
    /// tip through `source` alone, without joining a check another caller
    /// started.
    pub(crate) async fn verify_with_own_tip_check(
        &self,
        source: &impl BroadcastNodeProbe,
        target: &VerificationTarget,
    ) -> Result<(), SourceError> {
        self.verify_schedule_and_capabilities(source, target)
            .await?;
        verify_served_tip(source, target).await
    }

    /// Verifies that the node's best chain holds the served tip of `target`,
    /// sharing the call with concurrent callers checking the same block.
    pub(crate) async fn verify_served_tip<Source>(
        &self,
        source: &Source,
        target: &VerificationTarget,
    ) -> Result<(), SourceError>
    where
        Source: BroadcastNodeProbe + Clone,
    {
        if self.holds_block(source, target.fence.visible_tip()).await? {
            Ok(())
        } else {
            Err(served_tip_mismatch())
        }
    }

    /// Verifies that the node's best chain holds the served tip of `target`,
    /// charging one `check.kind` slot of `check.limit` inside the shared tip
    /// check.
    pub(crate) async fn verify_served_tip_admitted<Source>(
        &self,
        source: &Source,
        target: &VerificationTarget,
        check: AdmittedTipCheck<'_>,
    ) -> Result<(), crate::QueryError>
    where
        Source: BroadcastNodeProbe + Clone,
    {
        if self
            .holds_block_admitted(source, target.fence.visible_tip(), check)
            .await?
        {
            Ok(())
        } else {
            Err(crate::QueryError::Node(served_tip_mismatch()))
        }
    }

    /// Returns whether the node's best chain holds `block`, sharing one
    /// `getblockhash` call with concurrent callers asking about the same
    /// block. The answer is not kept after the call returns.
    pub(crate) async fn holds_block<Source>(
        &self,
        source: &Source,
        block: zinder_core::BlockId,
    ) -> Result<bool, SourceError>
    where
        Source: BroadcastNodeProbe + Clone,
    {
        self.tip_checks.holds_block(source, block).await
    }

    /// Returns whether the node's best chain holds `block`, charging one
    /// `check.kind` slot of `check.limit` inside the shared check.
    pub(crate) async fn holds_block_admitted<Source>(
        &self,
        source: &Source,
        block: zinder_core::BlockId,
        check: AdmittedTipCheck<'_>,
    ) -> Result<bool, crate::QueryError>
    where
        Source: BroadcastNodeProbe + Clone,
    {
        self.tip_checks
            .holds_block_admitted(source, block, check)
            .await
    }
}

/// Admission parameters for a foreground served-tip check.
#[derive(Clone, Copy)]
pub(crate) struct AdmittedTipCheck<'a> {
    pub(crate) limit: &'a crate::ForwardedReadLimit,
    pub(crate) kind: crate::ForwardedReadKind,
    pub(crate) deadline: tokio::time::Instant,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum TipCheckKey {
    Unadmitted(zinder_core::BlockId),
    Admitted(zinder_core::BlockId, crate::ForwardedReadKind),
}

#[derive(Clone)]
enum TipCheckError {
    Refused(crate::ForwardedReadRefused),
    Node(Arc<SourceError>),
}

impl TipCheckError {
    fn into_query_error(self) -> crate::QueryError {
        match self {
            Self::Refused(refused) => refused.into(),
            Self::Node(error) => {
                crate::QueryError::Node(crate::forwarded_read::owned_source_error(error))
            }
        }
    }

    fn into_source_error(self) -> SourceError {
        match self {
            Self::Refused(refused) => SourceError::NodeUnavailable {
                reason: refused.to_string(),
            },
            Self::Node(error) => crate::forwarded_read::owned_source_error(error),
        }
    }
}

type TipCheck = futures_util::future::BoxFuture<'static, Result<bool, TipCheckError>>;

/// Running checks by key, each with the number that tells it apart from a
/// later check of the same key, and the next such number.
type RunningTipChecks = (
    std::collections::HashMap<TipCheckKey, (u64, futures_util::future::WeakShared<TipCheck>)>,
    u64,
);

/// Served-tip checks in progress, by key.
#[derive(Default)]
struct TipChecks {
    running: Arc<parking_lot::Mutex<RunningTipChecks>>,
}

impl TipChecks {
    async fn holds_block<Source>(
        &self,
        source: &Source,
        block: zinder_core::BlockId,
    ) -> Result<bool, SourceError>
    where
        Source: BroadcastNodeProbe + Clone,
    {
        let source = source.clone();
        self.run(TipCheckKey::Unadmitted(block), move || async move {
            source
                .holds_block(block)
                .await
                .map_err(|error| TipCheckError::Node(Arc::new(error)))
        })
        .await
        .map_err(TipCheckError::into_source_error)
    }

    async fn holds_block_admitted<Source>(
        &self,
        source: &Source,
        block: zinder_core::BlockId,
        check: AdmittedTipCheck<'_>,
    ) -> Result<bool, crate::QueryError>
    where
        Source: BroadcastNodeProbe + Clone,
    {
        let source = source.clone();
        let limit = check.limit.clone();
        let kind = check.kind;
        let deadline = check.deadline;
        self.run(TipCheckKey::Admitted(block, kind), move || async move {
            limit
                .node_call(kind, deadline, source.holds_block(block))
                .await
                .map_err(TipCheckError::Refused)?
                .map_err(|error| TipCheckError::Node(Arc::new(error)))
        })
        .await
        .map_err(TipCheckError::into_query_error)
    }

    async fn run<Check, Started>(
        &self,
        key: TipCheckKey,
        start: Check,
    ) -> Result<bool, TipCheckError>
    where
        Check: FnOnce() -> Started,
        Started: Future<Output = Result<bool, TipCheckError>> + Send + 'static,
    {
        use futures_util::FutureExt as _;

        let check = {
            let mut running = self.running.lock();
            let joined = running.0.get(&key).and_then(|(_, weak)| weak.upgrade());
            let check = joined.unwrap_or_else(|| {
                let flight = running.1;
                running.1 += 1;
                let registry = Arc::clone(&self.running);
                let started = start();
                let check = async move {
                    let held = started.await;
                    // Leave the running set before answering, so a caller
                    // arriving after this answer makes its own call.
                    let mut running = registry.lock();
                    if running
                        .0
                        .get(&key)
                        .is_some_and(|(current, _)| *current == flight)
                    {
                        running.0.remove(&key);
                    }
                    drop(running);
                    held
                }
                .boxed()
                .shared();
                running.0.retain(|_, (_, weak)| weak.upgrade().is_some());
                if let Some(weak) = check.downgrade() {
                    running.0.insert(key, (flight, weak));
                }
                check
            });
            drop(running);
            check
        };
        check.await
    }

    #[cfg(test)]
    fn running(&self) -> usize {
        self.running
            .lock()
            .0
            .values()
            .filter(|(_, weak)| weak.upgrade().is_some())
            .count()
    }
}

#[cfg(test)]
mod tests;
