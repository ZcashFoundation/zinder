//! The `grpc.health.v1.Health` service that public Zinder listeners serve.
//!
//! gRPC-aware load balancers and Kubernetes `grpc` probes ask the serving
//! listener whether it is healthy. The service answers from the runtime's
//! [`Readiness`], the state behind `/readyz`, so both agree: a listener is
//! `SERVING` exactly when `/readyz` reports ready, including the warning
//! causes that still permit traffic, and `NOT_SERVING` otherwise. A runtime
//! that is shutting down publishes `shutting_down` before its listener
//! drains, so both report it at the same moment.
//!
//! `Watch` streams wait on readiness publications instead of sampling it, so
//! an idle stream costs no work. Each stream holds one of
//! [`MAX_ACTIVE_HEALTH_WATCHES`] process-wide permits and keeps no part of
//! its request, so open streams cannot exhaust memory. Once the listener's
//! drain starts, open streams end, so they do not hold the drain open. A
//! stream on the listener's status first sends `NOT_SERVING` unless that was
//! its last message, and drops any recovery it had not yet sent; a `SERVING`
//! already being sent as the drain began can still precede it.

use std::{
    pin::Pin,
    sync::{Arc, LazyLock},
};

use futures_util::{Stream, stream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tonic_health::pb::{
    HealthCheckRequest, HealthCheckResponse,
    health_check_response::ServingStatus,
    health_server::{Health, HealthServer},
};

use crate::{Readiness, RetryPushback, readiness::TrafficAdmission};

/// Most `Watch` streams the process keeps open at once, across every
/// listener and connection. A `Watch` past the limit fails with
/// `RESOURCE_EXHAUSTED`; `Check` is never refused.
pub(crate) const MAX_ACTIVE_HEALTH_WATCHES: usize = 1024;

static ACTIVE_HEALTH_WATCHES: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_ACTIVE_HEALTH_WATCHES)));

/// Returns the `grpc.health.v1.Health` service for a public listener that
/// serves the service named `served_service`.
///
/// An empty service name asks about the whole listener, and
/// `served_service` asks about that service; both report the listener's
/// status. Any other name is unknown. Open `Watch` streams end once `drain`
/// is cancelled.
pub(crate) fn public_grpc_health(
    readiness: &Readiness,
    drain: &CancellationToken,
    served_service: &'static str,
) -> HealthServer<impl Health> {
    HealthServer::new(ReadinessHealth {
        readiness: readiness.clone(),
        drain: drain.clone(),
        served_service,
        watch_permits: Arc::clone(&ACTIVE_HEALTH_WATCHES),
    })
    .max_decoding_message_size(crate::MAX_DECODING_MESSAGE_BYTES)
}

#[derive(Clone, Debug)]
struct ReadinessHealth {
    readiness: Readiness,
    drain: CancellationToken,
    served_service: &'static str,
    watch_permits: Arc<Semaphore>,
}

/// What a health request asks about, decided once from its service name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HealthSubject {
    Listener,
    Unknown,
}

impl ReadinessHealth {
    fn subject(&self, service: &str) -> HealthSubject {
        if service.is_empty() || service == self.served_service {
            HealthSubject::Listener
        } else {
            HealthSubject::Unknown
        }
    }
}

#[tonic::async_trait]
impl Health for ReadinessHealth {
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        match self.subject(&request.get_ref().service) {
            HealthSubject::Listener => Ok(Response::new(response(listener_status(
                self.readiness.permits_traffic(),
                self.drain.is_cancelled(),
            )))),
            HealthSubject::Unknown => Err(Status::not_found("service not registered")),
        }
    }

    type WatchStream = WatchResponses;

    /// Reports the current status at once and again whenever it changes. A
    /// service name this listener does not serve reports `SERVICE_UNKNOWN`.
    async fn watch(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let subject = self.subject(&request.into_inner().service);
        let admission = self.readiness.subscribe_traffic_admission();
        let observed_changes = admission.borrow().changes;
        let state = WatchState {
            subject,
            admission,
            drain: self.drain.clone(),
            observed_changes,
            last_sent: None,
            unsent_changes: 0,
            _permit: Arc::clone(&self.watch_permits)
                .try_acquire_owned()
                .map_err(|_| {
                    RetryPushback::Backoff
                        .apply(Status::resource_exhausted("too many open health watches"))
                })?,
        };
        Ok(Response::new(Box::pin(stream::unfold(
            state,
            next_watch_response,
        ))))
    }
}

type WatchResponses = Pin<Box<dyn Stream<Item = Result<HealthCheckResponse, Status>> + Send>>;

struct WatchState {
    subject: HealthSubject,
    admission: watch::Receiver<TrafficAdmission>,
    drain: CancellationToken,
    /// The admission change count this stream has read.
    observed_changes: u64,
    last_sent: Option<ServingStatus>,
    /// Admission changes read but not sent yet, shortened by
    /// [`coalesced_changes`].
    unsent_changes: u64,
    _permit: OwnedSemaphorePermit,
}

impl WatchState {
    fn send(mut self, status: ServingStatus) -> (Result<HealthCheckResponse, Status>, Self) {
        self.last_sent = Some(status);
        (Ok(response(status)), self)
    }

    /// Returns the next status the stream owes its client, if any.
    ///
    /// Once the drain has started, the stream owes at most a final
    /// `NOT_SERVING`: changes it has not sent yet are dropped, so none of
    /// them can report `SERVING` while the listener closes.
    ///
    /// Before that, every admission change flips the status, so each owed
    /// message is the opposite of the last one sent, and the count of unsent
    /// changes says how many are owed.
    fn owed_status(&mut self, draining: bool) -> Option<ServingStatus> {
        match (self.subject, self.last_sent) {
            (HealthSubject::Unknown, last_sent) => {
                last_sent.is_none().then_some(ServingStatus::ServiceUnknown)
            }
            (HealthSubject::Listener, last_sent) if draining => {
                (last_sent != Some(ServingStatus::NotServing)).then_some(ServingStatus::NotServing)
            }
            (HealthSubject::Listener, None) => {
                let admission = *self.admission.borrow_and_update();
                self.observed_changes = admission.changes;
                Some(listener_status(admission.permits_traffic, draining))
            }
            (HealthSubject::Listener, Some(last_sent)) => {
                let admission = *self.admission.borrow_and_update();
                let read_changes = admission.changes.wrapping_sub(self.observed_changes);
                self.observed_changes = admission.changes;
                self.unsent_changes =
                    coalesced_changes(self.unsent_changes.saturating_add(read_changes));
                if self.unsent_changes == 0 {
                    return None;
                }
                self.unsent_changes -= 1;
                Some(opposite(last_sent))
            }
        }
    }
}

/// Shortens a run of unsent admission changes to at most three messages.
///
/// The shortened run keeps its parity, so it still ends at the current
/// status, and a run of two or more changes still sends both statuses after
/// the last one sent. A client therefore sees an outage that recovered, or a
/// recovery that failed again, however quickly it reversed.
const fn coalesced_changes(changes: u64) -> u64 {
    if changes <= 3 {
        changes
    } else {
        2 + changes % 2
    }
}

/// Waits for the next status change, and ends the stream once the drain has
/// started and its final status has been sent.
async fn next_watch_response(
    mut state: WatchState,
) -> Option<(Result<HealthCheckResponse, Status>, WatchState)> {
    loop {
        let draining = state.drain.is_cancelled();
        if let Some(status) = state.owed_status(draining) {
            return Some(state.send(status));
        }
        if draining {
            return None;
        }
        match state.subject {
            HealthSubject::Listener => tokio::select! {
                changed = state.admission.changed() => {
                    if changed.is_err() {
                        return None;
                    }
                }
                () = state.drain.cancelled() => {}
            },
            HealthSubject::Unknown => state.drain.cancelled().await,
        }
    }
}

const fn listener_status(permits_traffic: bool, draining: bool) -> ServingStatus {
    if permits_traffic && !draining {
        ServingStatus::Serving
    } else {
        ServingStatus::NotServing
    }
}

const fn opposite(status: ServingStatus) -> ServingStatus {
    match status {
        ServingStatus::Serving => ServingStatus::NotServing,
        ServingStatus::NotServing | ServingStatus::Unknown | ServingStatus::ServiceUnknown => {
            ServingStatus::Serving
        }
    }
}

fn response(status: ServingStatus) -> HealthCheckResponse {
    HealthCheckResponse {
        status: status.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use futures_util::{FutureExt as _, StreamExt as _};
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;
    use tonic::{Code, Request, Status};
    use tonic_health::pb::{
        HealthCheckRequest, health_check_response::ServingStatus, health_server::Health,
    };

    use super::{ReadinessHealth, WatchResponses};
    use crate::{Readiness, ReadinessCause, ReadinessState};

    const SERVED: &str = "test.v1.Served";
    const WAIT_LIMIT: Duration = Duration::from_secs(5);

    fn health(readiness: &Readiness, watch_permits: usize) -> ReadinessHealth {
        ReadinessHealth {
            readiness: readiness.clone(),
            drain: CancellationToken::new(),
            served_service: SERVED,
            watch_permits: Arc::new(Semaphore::new(watch_permits)),
        }
    }

    async fn watch(health: &ReadinessHealth, service: &str) -> Result<WatchResponses, Status> {
        Ok(health
            .watch(Request::new(HealthCheckRequest {
                service: service.to_owned(),
            }))
            .await?
            .into_inner())
    }

    async fn next_status(
        watch: &mut WatchResponses,
    ) -> Result<ServingStatus, Box<dyn std::error::Error>> {
        let message = tokio::time::timeout(WAIT_LIMIT, watch.next())
            .await?
            .ok_or("health stream ended")??;
        Ok(ServingStatus::try_from(message.status)?)
    }

    /// Returns every status the stream sends until it ends.
    async fn statuses_until_end(
        watch: &mut WatchResponses,
    ) -> Result<Vec<ServingStatus>, Box<dyn std::error::Error>> {
        let mut statuses = Vec::new();
        while let Some(message) = tokio::time::timeout(WAIT_LIMIT, watch.next()).await? {
            statuses.push(ServingStatus::try_from(message?.status)?);
        }
        Ok(statuses)
    }

    /// Whether the stream would wait for a readiness change before sending
    /// anything else.
    fn owes_nothing(watch: &mut WatchResponses) -> bool {
        watch.next().now_or_never().is_none()
    }

    fn refusing_traffic() -> ReadinessState {
        ReadinessState::syncing(Some(1), Some(1), Some(2))
    }

    fn admitting_traffic() -> ReadinessState {
        ReadinessState::ready(Some(1))
    }

    /// Open watches are bounded across the process, whatever names they ask
    /// about and however many connections carry them, and closing one frees
    /// its permit.
    #[tokio::test]
    async fn watches_past_the_process_limit_are_refused_until_one_closes()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = Readiness::new(admitting_traffic());
        let health = health(&readiness, 2);
        let listener_watch = watch(&health, "").await?;
        let unknown_watch = watch(&health, &"x".repeat(1 << 20)).await?;

        let refused = watch(&health, SERVED)
            .await
            .err()
            .map(|status| status.code());
        assert_eq!(refused, Some(Code::ResourceExhausted));

        drop(unknown_watch);
        let mut admitted = watch(&health, SERVED).await?;
        assert_eq!(next_status(&mut admitted).await?, ServingStatus::Serving);
        drop(listener_watch);
        Ok(())
    }

    /// A readiness change that reverses before the stream wakes still
    /// reaches the client, followed by the status readiness returned to.
    #[tokio::test]
    async fn watch_reports_a_change_that_reverses_before_the_stream_wakes()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = Readiness::new(ReadinessState::starting());
        let health = health(&readiness, 1);
        let mut watch = watch(&health, "").await?;
        assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);

        readiness.set(admitting_traffic());
        readiness.set(refusing_traffic());

        assert_eq!(next_status(&mut watch).await?, ServingStatus::Serving);
        assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);
        Ok(())
    }

    /// From `NOT_SERVING`, a recovery, an outage, and a second recovery that
    /// all land before the stream wakes still show the client the outage.
    #[tokio::test]
    async fn watch_reports_an_outage_between_recoveries_before_the_stream_wakes()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = Readiness::new(ReadinessState::starting());
        let health = health(&readiness, 1);
        let mut watch = watch(&health, "").await?;
        assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);

        readiness.set(admitting_traffic());
        readiness.set(refusing_traffic());
        readiness.set(admitting_traffic());

        assert_eq!(next_status(&mut watch).await?, ServingStatus::Serving);
        assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);
        assert_eq!(next_status(&mut watch).await?, ServingStatus::Serving);
        assert!(owes_nothing(&mut watch));
        Ok(())
    }

    /// A long run of changes before the stream wakes is shortened, but it
    /// still shows both statuses and ends at the current one.
    #[tokio::test]
    async fn watch_shortens_a_long_run_of_changes_without_hiding_either_status()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = Readiness::new(admitting_traffic());
        let health = health(&readiness, 1);
        let mut watch = watch(&health, "").await?;
        assert_eq!(next_status(&mut watch).await?, ServingStatus::Serving);

        for _ in 0..3 {
            readiness.set(refusing_traffic());
            readiness.set(admitting_traffic());
        }
        readiness.set(refusing_traffic());
        assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);
        assert_eq!(next_status(&mut watch).await?, ServingStatus::Serving);
        assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);
        assert!(owes_nothing(&mut watch));

        for _ in 0..3 {
            readiness.set(admitting_traffic());
            readiness.set(refusing_traffic());
        }
        assert_eq!(next_status(&mut watch).await?, ServingStatus::Serving);
        assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);
        assert!(owes_nothing(&mut watch));
        Ok(())
    }

    /// A recovery still owed when shutdown starts is never sent, so a stream
    /// cannot report `SERVING` once the drain has started.
    #[tokio::test]
    async fn a_drain_drops_an_owed_recovery() -> Result<(), Box<dyn std::error::Error>> {
        let readiness = Readiness::new(admitting_traffic());
        let health = health(&readiness, 1);
        let mut watch = watch(&health, "").await?;
        assert_eq!(next_status(&mut watch).await?, ServingStatus::Serving);

        readiness.set(refusing_traffic());
        readiness.set(admitting_traffic());
        assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);

        readiness.set(ReadinessState::not_ready(ReadinessCause::ShuttingDown));
        health.drain.cancel();

        assert_eq!(statuses_until_end(&mut watch).await?, []);
        Ok(())
    }

    /// A drain that starts while a stream still owes an outage and a
    /// recovery ends it with `NOT_SERVING` alone.
    #[tokio::test]
    async fn a_drain_ends_a_stream_owing_several_changes_with_not_serving()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = Readiness::new(admitting_traffic());
        let health = health(&readiness, 1);
        let mut watch = watch(&health, "").await?;
        assert_eq!(next_status(&mut watch).await?, ServingStatus::Serving);

        readiness.set(refusing_traffic());
        readiness.set(admitting_traffic());
        health.drain.cancel();

        assert_eq!(
            statuses_until_end(&mut watch).await?,
            [ServingStatus::NotServing]
        );
        Ok(())
    }
}
