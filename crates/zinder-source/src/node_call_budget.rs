//! Admission control for JSON-RPC calls one source sends to its node.
//!
//! jsonrpsee 0.26 does not expose hyper's connection-pool limit, so every
//! concurrent call can open another HTTP connection. Zebra answers HTTP 429
//! once its JSON-RPC server holds its connection limit (100 by default), so
//! the source admits calls through a shared semaphore sized by
//! `node.max_concurrent_requests`. Upstream-health probes take a separate
//! single slot, so a budget saturated by slow forwarded calls never fails the
//! readiness probe.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Semaphore, SemaphorePermit};

use crate::SourceError;

/// Which slot pool a JSON-RPC call is admitted through.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NodeCallLane {
    /// Every call except upstream-health probes.
    Shared,
    /// Upstream-health probes, which must not queue behind other calls.
    Health,
}

/// Concurrency budget shared by every clone of one source.
#[derive(Clone, Debug)]
pub(crate) struct NodeCallBudget {
    shared: Arc<Semaphore>,
    health: Arc<Semaphore>,
    max_concurrent_requests: NonZeroU32,
}

impl NodeCallBudget {
    pub(crate) fn new(max_concurrent_requests: NonZeroU32) -> Self {
        let shared_slots = usize::try_from(max_concurrent_requests.get())
            .unwrap_or(Semaphore::MAX_PERMITS)
            .min(Semaphore::MAX_PERMITS);
        Self {
            shared: Arc::new(Semaphore::new(shared_slots)),
            health: Arc::new(Semaphore::new(1)),
            max_concurrent_requests,
        }
    }

    /// Waits for a call slot on `lane`, for at most `wait_limit`.
    ///
    /// The returned permit frees the slot when dropped. Waiting out
    /// `wait_limit` fails with [`SourceError::NodeUnavailable`] without
    /// contacting the node.
    pub(crate) async fn admit(
        &self,
        lane: NodeCallLane,
        operation: &'static str,
        wait_limit: Duration,
    ) -> Result<SemaphorePermit<'_>, SourceError> {
        let slots = match lane {
            NodeCallLane::Shared => &self.shared,
            NodeCallLane::Health => &self.health,
        };
        let wait_started_at = Instant::now();
        match tokio::time::timeout(wait_limit, slots.acquire()).await {
            Ok(Ok(permit)) => {
                metrics::histogram!(
                    "zinder_node_request_admission_wait_seconds",
                    "source" => "zebra_json_rpc",
                    "method" => operation
                )
                .record(wait_started_at.elapsed());
                Ok(permit)
            }
            Ok(Err(_closed)) => Err(SourceError::NodeUnavailable {
                reason: format!("{operation} could not be admitted: node call slots are closed"),
            }),
            Err(_elapsed) => {
                metrics::counter!(
                    "zinder_node_request_admission_timeouts_total",
                    "source" => "zebra_json_rpc",
                    "method" => operation
                )
                .increment(1);
                Err(SourceError::NodeUnavailable {
                    reason: format!(
                        "{operation} waited {}ms for one of node.max_concurrent_requests={} \
                         JSON-RPC call slots",
                        wait_limit.as_millis(),
                        self.max_concurrent_requests,
                    ),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(slots: u32) -> Result<NonZeroU32, SourceError> {
        NonZeroU32::new(slots).ok_or(SourceError::SourceProtocolMismatch {
            reason: "test limit must be nonzero",
        })
    }

    #[tokio::test]
    async fn waiting_past_the_limit_fails_without_a_slot() -> Result<(), SourceError> {
        let budget = NodeCallBudget::new(limit(1)?);
        let _held = budget
            .admit(NodeCallLane::Shared, "getblock", Duration::from_secs(1))
            .await?;

        let refusal = budget
            .admit(NodeCallLane::Shared, "getblock", Duration::from_millis(10))
            .await
            .err();

        assert!(
            matches!(&refusal, Some(SourceError::NodeUnavailable { reason }) if reason.contains("node.max_concurrent_requests=1")),
            "{refusal:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn health_lane_is_admitted_while_shared_slots_are_held() -> Result<(), SourceError> {
        let budget = NodeCallBudget::new(limit(1)?);
        let _held = budget
            .admit(NodeCallLane::Shared, "getblock", Duration::from_secs(1))
            .await?;

        let _probe = budget
            .admit(
                NodeCallLane::Health,
                "getblockchaininfo",
                Duration::from_millis(10),
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_released_slot_admits_the_next_call() -> Result<(), SourceError> {
        let budget = NodeCallBudget::new(limit(1)?);
        let held = budget
            .admit(NodeCallLane::Shared, "getblock", Duration::from_secs(1))
            .await?;
        drop(held);

        let _next = budget
            .admit(NodeCallLane::Shared, "getblock", Duration::from_millis(10))
            .await?;
        Ok(())
    }
}
