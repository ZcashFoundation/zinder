//! Immutable, exact-fence read pairs for wallet serving.

use std::{
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use thiserror::Error;
use tokio::sync::OnceCell;
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use zinder_core::{BlockId, ChainEpoch, Network, NetworkUpgradeActivations};
use zinder_store::{CanonicalActivationSchedule, CanonicalEventFence};
use zinder_wallet_projection::{
    WalletCanonicalSourceIdentity, WalletProjectionReadyEvidence, WalletProjectionSourcePosition,
};

use crate::{CanonicalReader, QueryError, WalletProjectionReader};

/// Typed reason why canonical and wallet readers cannot form one serving pair.
///
/// This error is intentionally distinct from [`QueryError`]. Runtime pair
/// publishers use it to classify replica lag, wallet projection lag, and
/// malformed admitted evidence before deciding readiness. Query construction
/// maps it to a fail-closed request error.
#[derive(Debug, Error)]
pub enum WalletServingAdmissionError {
    /// The independently admitted readers committed different networks.
    #[error("canonical and wallet readers have different admitted networks")]
    NetworkMismatch {
        /// Network committed by the canonical reader.
        canonical: Network,
        /// Network committed by the wallet reader.
        wallet: Network,
    },
    /// The visible canonical epoch disagreed with the canonical READY fence.
    #[error("canonical READY epoch does not match its admitted event fence")]
    CanonicalFenceMismatch,
    /// The canonical reader carries no writer-admitted network-upgrade
    /// schedule, so the pair has no activation table to serve.
    #[error("canonical reader carries no writer-admitted network-upgrade schedule")]
    NetworkUpgradeScheduleNotAdmitted,
    /// The wallet READY identity did not exactly match canonical state.
    #[error("wallet READY evidence does not match the canonical fence and settlement boundary")]
    WalletSourceMismatch {
        /// Identity reconstructed from canonical READY state.
        canonical: Box<WalletCanonicalSourceIdentity>,
        /// Identity committed by wallet READY evidence.
        wallet: Box<WalletCanonicalSourceIdentity>,
    },
}

/// What a canonical reader admitted with its fence, as pair admission
/// compares it with wallet READY evidence.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CanonicalAdmission<'reader> {
    /// Network the reader admitted.
    pub(crate) network: Network,
    /// Network-upgrade schedule the canonical writer admitted with the fence.
    pub(crate) network_upgrade_schedule: Option<&'reader CanonicalActivationSchedule>,
    /// Exact admitted canonical event fence.
    pub(crate) event_fence: CanonicalEventFence,
    /// Chain epoch visible at the fence.
    pub(crate) chain_epoch: ChainEpoch,
}

impl<'reader> CanonicalAdmission<'reader> {
    /// Returns what `reader` admitted with its fence.
    fn of(reader: &'reader dyn CanonicalReader) -> Self {
        Self {
            network: reader.network(),
            network_upgrade_schedule: reader.network_upgrade_schedule(),
            event_fence: reader.event_fence(),
            chain_epoch: reader.chain_epoch(),
        }
    }
}

/// One immutable canonical and wallet reader pair at the exact same source.
///
/// Pair construction validates network, epoch, event cursor, visible tip,
/// sequence digest, and settled tip, and requires the canonical writer's
/// admitted network-upgrade schedule, which is the pair's activation table.
/// Callers can safely hold an `Arc` to this pair for an entire request while
/// a different generation catches up.
///
/// A held pair keeps its secondary generation open. A read that spans many
/// bounded steps checks between them whether the publisher has revoked reads
/// on the pair, which it does once the pair has been released for longer than
/// the released-pair read limit, and then fails and drops the pair.
///
/// A pair whose read found one of its table files deleted by a primary's
/// compaction is marked. A marked pair answers no pin, and it stays the
/// current pair only until the publisher publishes its replacement.
pub struct WalletServingReadPair {
    canonical: Arc<dyn CanonicalReader>,
    wallet: Arc<dyn WalletProjectionReader>,
    canonical_fence: CanonicalEventFence,
    wallet_source: WalletCanonicalSourceIdentity,
    network_upgrade_activations: NetworkUpgradeActivations,
    chain_epoch: ChainEpoch,
    reads_revoked: CancellationToken,
    table_file_missing: AtomicBool,
    observed_for_epoch_pin: AtomicBool,
    tip_block_time: OnceCell<i64>,
}

impl WalletServingReadPair {
    /// Creates an immutable pair only when both readers prove one exact source.
    pub fn new(
        canonical: Arc<dyn CanonicalReader>,
        wallet: Arc<dyn WalletProjectionReader>,
    ) -> Result<Self, QueryError> {
        let chain_epoch = Self::validate_admission(
            CanonicalAdmission::of(canonical.as_ref()),
            wallet.network(),
            wallet.ready_evidence(),
        )
        .map_err(|error| pair_admission_query_error(&error))?;
        let network_upgrade_activations = canonical
            .network_upgrade_schedule()
            .map(|schedule| schedule.activations().clone())
            .ok_or_else(|| {
                pair_admission_query_error(
                    &WalletServingAdmissionError::NetworkUpgradeScheduleNotAdmitted,
                )
            })?;
        Ok(Self {
            canonical_fence: canonical.event_fence(),
            wallet_source: WalletCanonicalSourceIdentity::from_ready_evidence(
                wallet.ready_evidence(),
            ),
            network_upgrade_activations,
            chain_epoch,
            canonical,
            wallet,
            reads_revoked: CancellationToken::new(),
            table_file_missing: AtomicBool::new(false),
            observed_for_epoch_pin: AtomicBool::new(false),
            tip_block_time: OnceCell::new(),
        })
    }

    /// Validates whether a canonical reader admitted as `canonical` and a
    /// wallet reader of `wallet_network` with `wallet_evidence` represent one
    /// exact source, and returns the canonical epoch they agree on.
    ///
    /// Candidate-pair publishers use this before publication. It compares the
    /// admitted network, epoch, event cursor, visible tip, sequence digest,
    /// and settlement boundary without retaining either reader.
    pub(crate) fn validate_admission(
        canonical: CanonicalAdmission<'_>,
        wallet_network: Network,
        wallet_evidence: &WalletProjectionReadyEvidence,
    ) -> Result<ChainEpoch, WalletServingAdmissionError> {
        if canonical.network != wallet_network {
            return Err(WalletServingAdmissionError::NetworkMismatch {
                canonical: canonical.network,
                wallet: wallet_network,
            });
        }
        if canonical.network_upgrade_schedule.is_none() {
            return Err(WalletServingAdmissionError::NetworkUpgradeScheduleNotAdmitted);
        }
        let canonical_fence = canonical.event_fence;
        let canonical_epoch = canonical.chain_epoch;
        let canonical_visible_tip = BlockId::new(
            canonical_epoch.visible_tip_height,
            canonical_epoch.visible_tip_hash,
        );
        if canonical_fence.schema_version()
            != zinder_wallet_projection::REQUIRED_CANONICAL_STORE_SCHEMA_VERSION
            || canonical_epoch.network != canonical.network
            || canonical_epoch.id != canonical_fence.chain_epoch_id()
            || canonical_visible_tip != canonical_fence.visible_tip()
        {
            return Err(WalletServingAdmissionError::CanonicalFenceMismatch);
        }
        let canonical_source = WalletCanonicalSourceIdentity::new(
            WalletProjectionSourcePosition::new(
                canonical_fence.chain_epoch_id(),
                canonical_fence.visible_tip(),
                canonical_fence.chain_event_sequence(),
            ),
            canonical_fence.sequence_digest(),
            BlockId::new(
                canonical_epoch.settled_tip_height,
                canonical_epoch.settled_tip_hash,
            ),
        );
        let wallet_source = WalletCanonicalSourceIdentity::from_ready_evidence(wallet_evidence);
        if wallet_source != canonical_source {
            return Err(WalletServingAdmissionError::WalletSourceMismatch {
                canonical: Box::new(canonical_source),
                wallet: Box::new(wallet_source),
            });
        }
        Ok(canonical_epoch)
    }

    /// Returns the canonical reader frozen for this pair's entire lifetime.
    #[must_use]
    pub fn canonical(&self) -> &(dyn CanonicalReader + 'static) {
        self.canonical.as_ref()
    }

    /// Returns a cloned handle to the frozen canonical reader for detached tasks.
    #[must_use]
    pub(crate) fn canonical_arc(&self) -> Arc<dyn CanonicalReader> {
        Arc::clone(&self.canonical)
    }

    /// Returns the wallet reader frozen for this pair's entire lifetime.
    #[must_use]
    pub fn wallet(&self) -> &(dyn WalletProjectionReader + 'static) {
        self.wallet.as_ref()
    }

    /// Returns the exact canonical fence admitted for this pair.
    #[must_use]
    pub const fn canonical_fence(&self) -> CanonicalEventFence {
        self.canonical_fence
    }

    /// Returns the exact wallet source admitted for this pair.
    #[must_use]
    pub const fn wallet_source(&self) -> WalletCanonicalSourceIdentity {
        self.wallet_source
    }

    /// Returns the network-upgrade activation table the canonical writer
    /// admitted with this pair's fence.
    #[must_use]
    pub const fn network_upgrade_activations(&self) -> &NetworkUpgradeActivations {
        &self.network_upgrade_activations
    }

    /// Returns the canonical epoch this pair serves, read and validated
    /// against both readers when the pair was admitted.
    #[must_use]
    pub const fn chain_epoch(&self) -> ChainEpoch {
        self.chain_epoch
    }

    /// Tells the reads still holding this pair to stop, returning `true` the
    /// first time.
    pub(crate) fn revoke_reads(&self) -> bool {
        let first_revocation = !self.reads_revoked.is_cancelled();
        self.reads_revoked.cancel();
        first_revocation
    }

    /// Resolves once the publisher has revoked reads on this pair.
    pub(crate) fn reads_revoked(&self) -> WaitForCancellationFuture<'_> {
        self.reads_revoked.cancelled()
    }

    /// Reports whether the publisher has revoked reads on this pair.
    pub(crate) fn are_reads_revoked(&self) -> bool {
        self.reads_revoked.is_cancelled()
    }

    /// Marks this pair as missing a table file, returning `true` the first
    /// time.
    pub(crate) fn mark_table_file_missing(&self) -> bool {
        !self.table_file_missing.swap(true, Ordering::AcqRel)
    }

    /// Reports whether a read found one of this pair's table files deleted.
    pub(crate) fn has_missing_table_file(&self) -> bool {
        self.table_file_missing.load(Ordering::Acquire)
    }

    /// Marks this pair as one whose chain epoch a request may have exposed,
    /// so the slot keeps it readable for pins once it is replaced.
    ///
    /// Only the slot calls this, while it holds its transition lock.
    pub(crate) fn mark_observed_for_epoch_pin(&self) {
        self.observed_for_epoch_pin.store(true, Ordering::Release);
    }

    /// Reports whether a request may have exposed this pair's chain epoch.
    pub(crate) fn is_observed_for_epoch_pin(&self) -> bool {
        self.observed_for_epoch_pin.load(Ordering::Acquire)
    }

    /// Clears the observation mark, so a test can replay the first
    /// observation of a pair.
    #[cfg(test)]
    pub(crate) fn clear_observation_for_test(&self) {
        self.observed_for_epoch_pin.store(false, Ordering::Release);
    }

    /// Reads the block time of this pair's visible tip once and keeps it, so
    /// the frequent single-shot tip calls read a header once per pair.
    pub(crate) async fn visible_tip_block_time(&self) -> Result<i64, QueryError> {
        self.tip_block_time
            .get_or_try_init(|| async {
                self.canonical
                    .block_header_at(self.chain_epoch.visible_tip_height)
                    .await
                    .map_err(QueryError::canonical_read)?
                    .ok_or(QueryError::BlockNotInBestChain)
                    .map(|stored| stored.into_header().block_time)
            })
            .await
            .copied()
    }

    /// Returns the failure a revoked read ends with: its epoch is no longer
    /// served, so the caller restarts at a newer one.
    pub(crate) const fn revoked_read_error(&self) -> QueryError {
        QueryError::ChainEpochPinUnavailable {
            chain_epoch_id: self.canonical_fence.chain_epoch_id(),
        }
    }
}

impl fmt::Debug for WalletServingReadPair {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WalletServingReadPair")
            .field("canonical_fence", &self.canonical_fence)
            .field("wallet_source", &self.wallet_source)
            .finish_non_exhaustive()
    }
}

fn pair_admission_query_error(error: &WalletServingAdmissionError) -> QueryError {
    QueryError::WalletProjectionRead {
        source: Box::new(io::Error::other(error.to_string())),
    }
}
