//! [`TransactionBroadcaster`] fakes for tests.
//!
//! [`TransactionBroadcaster`]: zinder_source::TransactionBroadcaster

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use zinder_core::{
    BlockId, BroadcastAccepted, NetworkUpgradeActivations, RawTransactionBytes,
    TransactionBroadcastOutcome, TransactionId,
};
use zinder_source::{
    BroadcastNodeProbe, NodeCapabilities, NodeCapability, SourceError, TransactionBroadcaster,
};

/// A configurable [`TransactionBroadcaster`] fake.
///
/// Construct with one of the named constructors below; the broadcaster
/// returns the same outcome for every call. Every call's raw transaction
/// bytes are captured automatically and can be inspected via
/// [`MockTransactionBroadcaster::captured_calls`].
///
/// As a [`BroadcastNodeProbe`], the fake advertises transaction broadcast,
/// reports the table set by
/// [`MockTransactionBroadcaster::with_node_activations`], and holds every
/// block unless built [`MockTransactionBroadcaster::on_another_chain`], so a
/// reader's first-contact verification passes against a store with that
/// table. Its capability probe yields to the runtime once before it answers,
/// so concurrent verifications interleave. Clones share the captured calls
/// and the probe answers.
#[derive(Clone, Debug)]
pub struct MockTransactionBroadcaster {
    outcome: BroadcastOutcome,
    captured_calls: Arc<Mutex<Vec<RawTransactionBytes>>>,
    probe: Arc<Mutex<MockNodeProbe>>,
}

#[derive(Debug, Default)]
struct MockNodeProbe {
    node_activations: Option<NetworkUpgradeActivations>,
    on_another_chain: bool,
    capability_probes: usize,
}

#[derive(Clone, Debug)]
enum BroadcastOutcome {
    Returned(TransactionBroadcastOutcome),
    SourceError(MockSourceError),
}

/// A reproducible [`SourceError`] shape used by [`MockTransactionBroadcaster`].
///
/// `SourceError` itself is `non_exhaustive` and not `Clone`, so the mock
/// stores a structural representation and rebuilds the error per call.
#[derive(Clone, Debug)]
enum MockSourceError {
    BroadcastDisabled,
    NodeUnavailable { reason: String },
}

impl MockSourceError {
    fn as_source_error(&self) -> SourceError {
        match self {
            Self::BroadcastDisabled => SourceError::TransactionBroadcastDisabled,
            Self::NodeUnavailable { reason } => SourceError::NodeUnavailable {
                reason: reason.clone(),
            },
        }
    }
}

impl MockTransactionBroadcaster {
    /// Returns a broadcaster that accepts every call and reports
    /// `transaction_id` as the node's accepted id.
    #[must_use]
    pub fn accepted(transaction_id: TransactionId) -> Self {
        Self::with_outcome(BroadcastOutcome::Returned(
            TransactionBroadcastOutcome::Accepted(BroadcastAccepted { transaction_id }),
        ))
    }

    /// Returns a broadcaster that returns the given
    /// [`TransactionBroadcastOutcome`] verbatim for every call.
    #[must_use]
    pub fn returning(broadcast_outcome: TransactionBroadcastOutcome) -> Self {
        Self::with_outcome(BroadcastOutcome::Returned(broadcast_outcome))
    }

    /// Returns a broadcaster that fails every call with
    /// [`SourceError::TransactionBroadcastDisabled`].
    #[must_use]
    pub fn broadcast_disabled() -> Self {
        Self::with_outcome(BroadcastOutcome::SourceError(
            MockSourceError::BroadcastDisabled,
        ))
    }

    /// Returns a broadcaster that fails every call with
    /// [`SourceError::NodeUnavailable`] using the supplied reason.
    #[must_use]
    pub fn node_unavailable(reason: impl Into<String>) -> Self {
        Self::with_outcome(BroadcastOutcome::SourceError(
            MockSourceError::NodeUnavailable {
                reason: reason.into(),
            },
        ))
    }

    /// Reports `activations` as the node's activation table to a reader's
    /// first-contact verification.
    #[must_use]
    pub fn with_node_activations(self, activations: NetworkUpgradeActivations) -> Self {
        self.report_node_activations(activations);
        self
    }

    /// Reports `activations` as the node's activation table from now on, for
    /// this broadcaster and every clone, as a node restarted with another
    /// table would.
    pub fn report_node_activations(&self, activations: NetworkUpgradeActivations) {
        self.probe.lock().node_activations = Some(activations);
    }

    /// Reports that the node's best chain holds none of the reader's blocks,
    /// as a node on another chain with the same activation table would.
    #[must_use]
    pub fn on_another_chain(self) -> Self {
        self.report_another_chain();
        self
    }

    /// Reports from now on, for this broadcaster and every clone, that the
    /// node's best chain holds none of the reader's blocks, as a node that
    /// has not received the reader's tip block yet would.
    pub fn report_another_chain(&self) {
        self.probe.lock().on_another_chain = true;
    }

    /// Returns how many times a reader probed this node's capabilities, which
    /// it does once per first-contact verification.
    #[must_use]
    pub fn capability_probe_count(&self) -> usize {
        self.probe.lock().capability_probes
    }

    /// Returns the raw-transaction byte arguments captured by every call to
    /// this broadcaster, in call order.
    #[must_use]
    pub fn captured_calls(&self) -> Vec<RawTransactionBytes> {
        self.captured_calls.lock().clone()
    }

    /// Returns the count of calls captured so far.
    #[must_use]
    pub fn call_count(&self) -> usize {
        self.captured_calls.lock().len()
    }

    fn with_outcome(outcome: BroadcastOutcome) -> Self {
        Self {
            outcome,
            captured_calls: Arc::new(Mutex::new(Vec::new())),
            probe: Arc::new(Mutex::new(MockNodeProbe::default())),
        }
    }
}

#[async_trait]
impl BroadcastNodeProbe for MockTransactionBroadcaster {
    async fn probe_capabilities(&self) -> Result<NodeCapabilities, SourceError> {
        self.probe.lock().capability_probes += 1;
        tokio::task::yield_now().await;
        NodeCapabilities::new([
            NodeCapability::JsonRpc,
            NodeCapability::OpenRpcDiscovery,
            NodeCapability::TipId,
            NodeCapability::TransactionBroadcast,
        ])
        .map_err(|_| SourceError::SourceProtocolMismatch {
            reason: "the mock node capability set lists a capability twice",
        })
    }

    async fn fetch_network_upgrade_activations(
        &self,
    ) -> Result<NetworkUpgradeActivations, SourceError> {
        self.probe
            .lock()
            .node_activations
            .clone()
            .ok_or_else(|| SourceError::NodeUnavailable {
                reason: "the mock node reports no network-upgrade activation table".to_owned(),
            })
    }

    async fn holds_block(&self, _block: BlockId) -> Result<bool, SourceError> {
        Ok(!self.probe.lock().on_another_chain)
    }
}

#[async_trait]
impl TransactionBroadcaster for MockTransactionBroadcaster {
    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, SourceError> {
        self.captured_calls.lock().push(raw_transaction);
        match &self.outcome {
            BroadcastOutcome::Returned(broadcast_outcome) => Ok(broadcast_outcome.clone()),
            BroadcastOutcome::SourceError(mock_source_error) => {
                Err(mock_source_error.as_source_error())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::MockTransactionBroadcaster;
    use zinder_core::{
        BroadcastAccepted, RawTransactionBytes, TransactionBroadcastOutcome, TransactionId,
    };
    use zinder_source::{SourceError, TransactionBroadcaster};

    #[tokio::test(flavor = "current_thread")]
    async fn accepted_returns_supplied_transaction_id() -> Result<(), Box<dyn Error>> {
        let broadcaster =
            MockTransactionBroadcaster::accepted(TransactionId::from_bytes([0x42; 32]));
        let outcome = broadcaster
            .broadcast_transaction(RawTransactionBytes::new(vec![1, 2, 3]))
            .await?;
        assert!(matches!(
            outcome,
            TransactionBroadcastOutcome::Accepted(BroadcastAccepted { transaction_id })
                if transaction_id == TransactionId::from_bytes([0x42; 32])
        ));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn captured_calls_records_every_call_in_order() -> Result<(), Box<dyn Error>> {
        let broadcaster =
            MockTransactionBroadcaster::accepted(TransactionId::from_bytes([0x42; 32]));
        broadcaster
            .broadcast_transaction(RawTransactionBytes::new(vec![1, 2, 3]))
            .await?;
        broadcaster
            .broadcast_transaction(RawTransactionBytes::new(vec![4, 5, 6]))
            .await?;

        let captured_calls = broadcaster.captured_calls();
        assert_eq!(captured_calls.len(), 2);
        assert_eq!(captured_calls[0].as_slice(), &[1, 2, 3]);
        assert_eq!(captured_calls[1].as_slice(), &[4, 5, 6]);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn broadcast_disabled_returns_typed_error() -> Result<(), Box<dyn Error>> {
        let broadcaster = MockTransactionBroadcaster::broadcast_disabled();
        let outcome = broadcaster
            .broadcast_transaction(RawTransactionBytes::new(vec![1, 2, 3]))
            .await;
        assert!(matches!(
            outcome,
            Err(SourceError::TransactionBroadcastDisabled)
        ));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn node_unavailable_propagates_reason() -> Result<(), Box<dyn Error>> {
        let broadcaster = MockTransactionBroadcaster::node_unavailable("planned outage");
        let outcome = broadcaster
            .broadcast_transaction(RawTransactionBytes::new(vec![1, 2, 3]))
            .await;
        match outcome {
            Err(SourceError::NodeUnavailable { reason }) => {
                assert_eq!(reason, "planned outage");
            }
            other => {
                return Err(format!("expected NodeUnavailable, got {other:?}").into());
            }
        }
        Ok(())
    }
}
