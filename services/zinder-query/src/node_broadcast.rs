//! Transaction broadcast through the ordered node list, with every endpoint
//! verified on first contact.

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
    BlockHeight, BlockId, BroadcastAccepted, BroadcastRejected, BroadcastRejectionReason,
    ConsensusBranchId, NetworkUpgradeActivations, RawTransactionBytes, TransactionBroadcastOutcome,
    TransactionId,
};
use zinder_source::{
    BroadcastNodeProbe, NodeCapability, NodeSet, NodeSetSwitch, NodeTarget, SourceError,
    SourceFailureClass, TransactionBroadcaster, ZebraJsonRpcSource,
};
use zinder_store::CanonicalActivationSchedule;

use crate::{UpstreamNodeCapabilities, WalletServingPairSlot, WalletServingReadPair};

/// First exclusion of an endpoint that answered verification with evidence
/// that it must not receive transactions. Each later exclusion doubles it.
const EXCLUSION_INITIAL_BACKOFF: Duration = Duration::from_secs(30);

/// Longest an endpoint stays excluded before it is verified again.
const EXCLUSION_MAX_BACKOFF: Duration = Duration::from_mins(5);

/// Longest an endpoint stays verified against an earlier basis.
///
/// It applies after the serving pair's basis changed and the endpoint could
/// not be verified against the new one, for example because it was one block
/// behind.
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
/// A passed verification lasts until the serving pair's schedule revision or
/// the network upgrade active at the height after its tip changes, and the
/// endpoint is then verified again. When that check is inconclusive, the
/// endpoint stays verified against the earlier basis for up to one minute
/// while it is checked again. An endpoint that lacks `sendrawtransaction` or
/// whose table disagrees is excluded, and verified again after a backoff that
/// starts at 30 seconds and doubles to 5 minutes. Clones share every
/// endpoint's verification and the preferred position.
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

    /// Returns whether an endpoint is verified only against an earlier basis
    /// because its last verification was inconclusive.
    pub(crate) fn awaits_reverification(&self) -> bool {
        self.inner
            .verifications
            .endpoints
            .iter()
            .any(EndpointVerification::awaits_reverification)
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
    /// Endpoints are verified concurrently. Concurrent calls share each
    /// endpoint's verification, and an endpoint whose verification still
    /// holds is not contacted.
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
        let target = VerificationTarget::capture(pair_slot);
        let verifications = &self.inner.verifications;
        let outcomes = join_all(verifications.preference_order().filter_map(|position| {
            let source = self.inner.nodes.endpoint(position)?;
            let verification = verifications.endpoints.get(position)?;
            Some(verification.verify(source, &target))
        }))
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
            let outcome = source.broadcast_transaction(raw_transaction.clone()).await;
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
                    return Ok(attempt.settle(answer, &raw_transaction, position));
                }
                Err(error)
                    if error.upstream_classification() == SourceFailureClass::NodeUnreachable =>
                {
                    attempt.pass_over(PassOver::NodeUnreachable);
                    attempt.unreachable = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        attempt.exhausted()
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
    BadConsensusBranch,
}

impl PassOver {
    const fn label(self) -> &'static str {
        match self {
            Self::NodeUnreachable => "node_unreachable",
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
    fn settle(
        &self,
        answer: TransactionBroadcastOutcome,
        raw_transaction: &RawTransactionBytes,
        position: usize,
    ) -> TransactionBroadcastOutcome {
        if self.unreachable.is_none() {
            return answer;
        }
        match answer {
            TransactionBroadcastOutcome::Duplicate(_) | TransactionBroadcastOutcome::Queued(_) => {
                submitted_transaction_id(raw_transaction).map_or(answer, |transaction_id| {
                    tracing::info!(
                        target: "zinder::wallet_serving",
                        event = "broadcast_reported_accepted_after_unreachable_endpoint",
                        position,
                        "node endpoint already knew a transaction an unreachable endpoint may have accepted; reporting it accepted"
                    );
                    TransactionBroadcastOutcome::Accepted(BroadcastAccepted { transaction_id })
                })
            }
            answer => answer,
        }
    }

    /// Ends a broadcast that no endpoint answered with anything other than a
    /// consensus-branch rejection. An unreachable endpoint may have accepted
    /// the transaction, so its failure takes precedence over a rejection.
    fn exhausted(self) -> Result<TransactionBroadcastOutcome, SourceError> {
        if let Some(error) = self.unreachable {
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
/// not parse as a transaction this build decodes.
fn submitted_transaction_id(raw_transaction: &RawTransactionBytes) -> Option<TransactionId> {
    raw_transaction
        .as_slice()
        .zcash_deserialize_into::<Transaction>()
        .ok()
        .map(|transaction| TransactionId::from_bytes(transaction.hash().0))
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "non-exhaustive broadcast outcomes from zinder-core are counted as unknown"
)]
fn record_attempt(position: usize, outcome: &Result<TransactionBroadcastOutcome, SourceError>) {
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
}

impl EndpointVerifications {
    /// Returns every position once, starting at the preferred one.
    fn preference_order(&self) -> impl Iterator<Item = usize> + use<> {
        let count = self.endpoints.len();
        let preferred = *self.preferred.borrow();
        (0..count).map(move |offset| (preferred + offset) % count)
    }

    fn is_verified(&self, position: usize) -> bool {
        self.endpoints
            .get(position)
            .is_some_and(EndpointVerification::is_verified)
    }

    pub(crate) fn verified_capabilities(&self) -> Option<UpstreamNodeCapabilities> {
        self.preference_order().find_map(|position| {
            self.endpoints
                .get(position)
                .and_then(EndpointVerification::verified_capabilities)
        })
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
        /// When a verification against a later basis was first
        /// inconclusive, or `None` while `basis` is the latest one checked.
        stale_since: Option<Instant>,
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

    fn is_verified(&self) -> bool {
        matches!(**self.state.load(), VerificationState::Verified { .. })
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

    fn verified_capabilities(&self) -> Option<UpstreamNodeCapabilities> {
        match &**self.state.load() {
            VerificationState::Verified { capabilities, .. } => Some(capabilities.clone()),
            VerificationState::Unchecked
            | VerificationState::Unverified
            | VerificationState::Excluded { .. } => None,
        }
    }

    /// Returns the outcome of a verification that still holds against
    /// `basis`, or `None` when the endpoint must be verified again.
    fn settled(&self, basis: VerificationBasis) -> Option<Result<(), SourceError>> {
        match &**self.state.load() {
            VerificationState::Verified {
                basis: verified, ..
            } if *verified == basis => Some(Ok(())),
            VerificationState::Excluded {
                refusal, retry_at, ..
            } if Instant::now() < *retry_at => Some(Err(refusal.error())),
            VerificationState::Unchecked
            | VerificationState::Unverified
            | VerificationState::Verified { .. }
            | VerificationState::Excluded { .. } => None,
        }
    }

    async fn verify(
        &self,
        source: &impl BroadcastNodeProbe,
        target: &VerificationTarget,
    ) -> Result<(), SourceError> {
        if let Some(settled) = self.settled(target.basis) {
            return settled;
        }
        let _attempt = self.attempt.lock().await;
        if let Some(settled) = self.settled(target.basis) {
            return settled;
        }
        let outcome = verify_node_endpoint(source, target).await;
        self.record(outcome, target.basis)
    }

    fn record(
        &self,
        outcome: Result<UpstreamNodeCapabilities, VerificationFailure>,
        basis: VerificationBasis,
    ) -> Result<(), SourceError> {
        let previous = self.state.load_full();
        let (next, verification) = match outcome {
            Ok(capabilities) => {
                if !matches!(*previous, VerificationState::Verified { .. }) {
                    tracing::info!(
                        target: "zinder::wallet_serving",
                        event = "wallet_node_verified",
                        position = self.position,
                        "node endpoint passed first-contact verification; transaction broadcast reaches it"
                    );
                }
                (
                    VerificationState::Verified {
                        capabilities,
                        basis,
                        stale_since: None,
                    },
                    Ok(()),
                )
            }
            Err(VerificationFailure::Refused(refusal)) => self.excluded(&previous, refusal),
            Err(VerificationFailure::Inconclusive(error)) => self.inconclusive(&previous, error),
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
    fn inconclusive(
        &self,
        previous: &VerificationState,
        error: SourceError,
    ) -> (VerificationState, Result<(), SourceError>) {
        if let VerificationState::Verified {
            capabilities,
            basis,
            stale_since,
        } = previous
        {
            let since = stale_since.unwrap_or_else(Instant::now);
            if since.elapsed() < STALE_VERIFICATION_GRACE {
                if stale_since.is_none() {
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
                    stale_since: Some(since),
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
        Self {
            schedule_revision: pair
                .canonical()
                .network_upgrade_schedule()
                .map(CanonicalActivationSchedule::revision),
            broadcast_branch: pair
                .network_upgrade_activations()
                .consensus_branch_id_at(broadcast_height(pair)),
        }
    }
}

/// Returns the height of the block a transaction broadcast now targets, the
/// one after the pair's visible tip.
fn broadcast_height(pair: &WalletServingReadPair) -> BlockHeight {
    let tip_height = pair.canonical_fence().visible_tip().height;
    tip_height.next().unwrap_or(tip_height)
}

/// The serving-pair evidence one verification checks endpoints against,
/// captured without retaining the pair across node calls.
struct VerificationTarget {
    basis: VerificationBasis,
    admitted_activations: NetworkUpgradeActivations,
    broadcast_height: BlockHeight,
    visible_tip: BlockId,
}

impl VerificationTarget {
    fn capture(pair_slot: &WalletServingPairSlot) -> Self {
        let pair = pair_slot.capture();
        Self {
            basis: VerificationBasis::of(&pair),
            admitted_activations: pair.network_upgrade_activations().clone(),
            broadcast_height: broadcast_height(&pair),
            visible_tip: pair.canonical_fence().visible_tip(),
        }
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
) -> Result<UpstreamNodeCapabilities, VerificationFailure> {
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
    if !node_activations.preserves_through(&target.admitted_activations, target.broadcast_height) {
        return Err(VerificationFailure::Refused(
            VerificationRefusal::ScheduleMismatch,
        ));
    }
    // A tip block replaced by a reorg the pair has not published yet also
    // fails here, so this refusal is retried on the next probe round.
    if !source
        .holds_block(target.visible_tip)
        .await
        .map_err(VerificationFailure::Inconclusive)?
    {
        return Err(VerificationFailure::Inconclusive(
            SourceError::SourceProtocolMismatch {
                reason: "node best chain holds another block at the served tip height",
            },
        ));
    }
    Ok(UpstreamNodeCapabilities::from_probed(capabilities))
}
