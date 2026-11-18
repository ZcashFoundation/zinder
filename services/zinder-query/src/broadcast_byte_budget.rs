//! Bound on the transaction bytes a process broadcasts at once.

use std::sync::{Arc, LazyLock};

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

    /// Reserves `transaction_bytes` of the budget until the returned permit
    /// is dropped.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::BroadcastBudgetExhausted`] when the bytes do not
    /// fit, and counts the refusal in
    /// `zinder_query_broadcast_budget_refusals_total`.
    pub(crate) fn try_reserve(
        &self,
        transaction_bytes: usize,
    ) -> Result<OwnedSemaphorePermit, QueryError> {
        u32::try_from(transaction_bytes)
            .ok()
            .and_then(|permits| Arc::clone(&self.bytes).try_acquire_many_owned(permits).ok())
            .ok_or_else(|| {
                metrics::counter!(BUDGET_REFUSALS_COUNTER).increment(1);
                QueryError::BroadcastBudgetExhausted {
                    budget_bytes: self.budget_bytes,
                }
            })
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
