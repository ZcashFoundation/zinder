//! Bound on the transaction bytes a process broadcasts at once.

use std::{
    future::Future,
    sync::{Arc, LazyLock},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::QueryError;

/// Counter of broadcasts refused because the byte budget was spent.
const BUDGET_REFUSALS_COUNTER: &str = "zinder_query_broadcast_budget_refusals_total";

/// Transaction bytes one process broadcasts at once, the default of
/// lightwalletd's `--max-concurrent-sendtx-mib`.
///
/// A broadcast copies its transaction several times on the way to the node:
/// the request, a copy per endpoint, hex, the JSON-RPC value, and the request
/// body, about six times its size, and holds them until the node answers or
/// the broadcast times out. The budget bounds that memory once the request
/// has left its receive slot.
pub const BROADCAST_BYTE_BUDGET: usize = 16 * 1024 * 1024;

const _: () = assert!(BROADCAST_BYTE_BUDGET >= zinder_core::MAX_RAW_TRANSACTION_BYTES);

/// The budget every query in this process shares.
static PROCESS_BUDGET: LazyLock<BroadcastByteBudget> =
    LazyLock::new(|| BroadcastByteBudget::new(BROADCAST_BYTE_BUDGET));

/// Transaction bytes the process may broadcast at once.
///
/// Clones share one budget. A broadcast that does not fit is refused at once
/// rather than queued, because a queued broadcast would keep its decoded
/// request in memory while it waits.
#[derive(Clone, Debug)]
pub(crate) struct BroadcastByteBudget {
    bytes: Arc<Semaphore>,
    budget_bytes: usize,
}

impl BroadcastByteBudget {
    /// The budget shared by every query in this process.
    pub(crate) fn process() -> Self {
        PROCESS_BUDGET.clone()
    }

    /// A budget of `budget_bytes`, separate from the process budget.
    pub(crate) fn new(budget_bytes: usize) -> Self {
        Self {
            bytes: Arc::new(Semaphore::new(budget_bytes.min(Semaphore::MAX_PERMITS))),
            budget_bytes,
        }
    }

    /// Reserves `transaction_bytes` of the budget until the returned
    /// reservation and every clone of it are dropped.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::BroadcastBudgetExhausted`] when the bytes do not
    /// fit, and counts the refusal in
    /// `zinder_query_broadcast_budget_refusals_total`.
    pub(crate) fn try_reserve(
        &self,
        transaction_bytes: usize,
    ) -> Result<BroadcastReservation, QueryError> {
        u32::try_from(transaction_bytes)
            .ok()
            .and_then(|permits| Arc::clone(&self.bytes).try_acquire_many_owned(permits).ok())
            .map(|permit| BroadcastReservation {
                _bytes: Arc::new(permit),
            })
            .ok_or_else(|| {
                metrics::counter!(BUDGET_REFUSALS_COUNTER).increment(1);
                QueryError::BroadcastBudgetExhausted {
                    budget_bytes: self.budget_bytes,
                }
            })
    }
}

/// Budget bytes held for one broadcast, returned when the last clone drops.
///
/// Work a broadcast detaches, such as hashing its transaction on the blocking
/// pool, keeps the transaction after the broadcast itself is cancelled. Such
/// work takes a clone with [`BroadcastReservation::current`] so the bytes
/// stay counted until it ends.
#[derive(Clone, Debug)]
pub(crate) struct BroadcastReservation {
    _bytes: Arc<OwnedSemaphorePermit>,
}

tokio::task_local! {
    static CURRENT_RESERVATION: BroadcastReservation;
}

impl BroadcastReservation {
    /// Runs `broadcast` while this reservation is its
    /// [`current`](Self::current) one, holding it until `broadcast` ends or
    /// is dropped.
    pub(crate) async fn hold_during<F: Future>(self, broadcast: F) -> F::Output {
        CURRENT_RESERVATION.scope(self, broadcast).await
    }

    /// The reservation of the broadcast being polled, if any.
    pub(crate) fn current() -> Option<Self> {
        CURRENT_RESERVATION.try_with(Self::clone).ok()
    }
}

/// Registers `# HELP` and `# TYPE` text for the broadcast budget metrics.
pub(crate) fn describe_metrics() {
    metrics::describe_counter!(
        BUDGET_REFUSALS_COUNTER,
        metrics::Unit::Count,
        "Transaction broadcasts refused with UNAVAILABLE because the process was already \
         broadcasting its budget of transaction bytes."
    );
}
