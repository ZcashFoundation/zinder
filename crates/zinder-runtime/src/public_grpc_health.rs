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
//! drain starts, open streams report `NOT_SERVING` and end, so they do not
//! hold the drain open.

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

use crate::{Readiness, readiness::TrafficAdmission};

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
            queued: None,
            _permit: Arc::clone(&self.watch_permits)
                .try_acquire_owned()
                .map_err(|_| Status::resource_exhausted("too many open health watches"))?,
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
    /// The admission change count this stream has accounted for.
    observed_changes: u64,
    last_sent: Option<ServingStatus>,
    /// A status to send right after the one being sent.
    queued: Option<ServingStatus>,
    _permit: OwnedSemaphorePermit,
}

impl WatchState {
    fn send(mut self, status: ServingStatus) -> (Result<HealthCheckResponse, Status>, Self) {
        self.last_sent = Some(status);
        (Ok(response(status)), self)
    }

    /// Returns the next status the stream owes its client, if any.
    ///
    /// The admission has two values, so the change count says which values
    /// it passed through since the last message. When it changed and changed
    /// back before this stream woke, the stream sends the intermediate
    /// status and then the current one, so no change goes unreported.
    fn owed_status(&mut self, draining: bool) -> Option<ServingStatus> {
        match self.subject {
            HealthSubject::Unknown => self
                .last_sent
                .is_none()
                .then_some(ServingStatus::ServiceUnknown),
            HealthSubject::Listener => {
                let admission = *self.admission.borrow_and_update();
                let unobserved_changes = admission.changes.wrapping_sub(self.observed_changes);
                self.observed_changes = admission.changes;
                let status = listener_status(admission.permits_traffic, draining);
                if self.last_sent != Some(status) {
                    return Some(status);
                }
                if draining || unobserved_changes == 0 {
                    return None;
                }
                self.queued = Some(status);
                Some(opposite(status))
            }
        }
    }
}

/// Waits for the next status change, and ends the stream once the drain has
/// started and its status has been sent.
async fn next_watch_response(
    mut state: WatchState,
) -> Option<(Result<HealthCheckResponse, Status>, WatchState)> {
    loop {
        if let Some(status) = state.queued.take() {
            return Some(state.send(status));
        }
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

    use futures_util::StreamExt as _;
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;
    use tonic::{Code, Request, Status};
    use tonic_health::pb::{
        HealthCheckRequest, health_check_response::ServingStatus, health_server::Health,
    };

    use super::{ReadinessHealth, WatchResponses};
    use crate::{Readiness, ReadinessState};

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

    /// Open watches are bounded across the process, whatever names they ask
    /// about and however many connections carry them, and closing one frees
    /// its permit.
    #[tokio::test]
    async fn watches_past_the_process_limit_are_refused_until_one_closes()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness = Readiness::new(ReadinessState::ready(Some(1)));
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

        readiness.set(ReadinessState::ready(Some(1)));
        readiness.set(ReadinessState::syncing(Some(1), Some(1), Some(2)));

        assert_eq!(next_status(&mut watch).await?, ServingStatus::Serving);
        assert_eq!(next_status(&mut watch).await?, ServingStatus::NotServing);
        Ok(())
    }
}
