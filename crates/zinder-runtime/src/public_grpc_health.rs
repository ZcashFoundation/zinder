//! The `grpc.health.v1.Health` service that public Zinder listeners serve.
//!
//! gRPC-aware load balancers and Kubernetes `grpc` probes ask the serving
//! listener whether it is healthy. [`public_grpc_health`] answers from the
//! runtime's [`Readiness`], the state behind `/readyz`, so both agree: a
//! listener is `SERVING` exactly when `/readyz` reports ready, including the
//! warning causes that still permit traffic, and `NOT_SERVING` otherwise.
//!
//! Shutdown is part of that state. Once the shutdown token fires, every
//! answer is `NOT_SERVING` and open `Watch` streams report it and end, so a
//! balancer learns the listener is leaving before it stops accepting
//! connections and a draining server is not held open by health watchers.
//!
//! The service is registered without the [`crate::TrafficReadinessInterceptor`]
//! that gates the wallet services, so it keeps answering while those refuse
//! traffic.

use std::{pin::Pin, time::Duration};

use futures_util::{Stream, stream};
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, server::NamedService};
use tonic_health::pb::{
    HealthCheckRequest, HealthCheckResponse,
    health_check_response::ServingStatus,
    health_server::{Health, HealthServer},
};

use crate::Readiness;

/// How often an open `Watch` stream samples readiness for a change.
const WATCH_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Returns the `grpc.health.v1.Health` service for a public Zinder listener
/// that serves `served_service`.
///
/// An empty service name asks about the whole listener, and the name of
/// `served_service` asks about that service; both report the listener's
/// status. Any other name is unknown. `shutdown` is the token that also stops
/// the listener.
///
/// The service decodes request messages up to
/// [`crate::MAX_DECODING_MESSAGE_BYTES`], like every other public service.
#[must_use]
pub fn public_grpc_health<S: NamedService>(
    readiness: Readiness,
    shutdown: CancellationToken,
    _served_service: &S,
) -> HealthServer<impl Health> {
    HealthServer::new(ReadinessHealth {
        readiness,
        shutdown,
        served_service: S::NAME,
    })
    .max_decoding_message_size(crate::MAX_DECODING_MESSAGE_BYTES)
}

#[derive(Clone, Debug)]
struct ReadinessHealth {
    readiness: Readiness,
    shutdown: CancellationToken,
    served_service: &'static str,
}

impl ReadinessHealth {
    fn knows(&self, service: &str) -> bool {
        service.is_empty() || service == self.served_service
    }

    fn listener_status(&self) -> ServingStatus {
        if !self.shutdown.is_cancelled() && self.readiness.report().is_ready {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        }
    }

    fn status_of(&self, service: &str) -> ServingStatus {
        if self.knows(service) {
            self.listener_status()
        } else {
            ServingStatus::ServiceUnknown
        }
    }
}

struct WatchState {
    health: ReadinessHealth,
    service: String,
    last_sent: Option<ServingStatus>,
    poll: tokio::time::Interval,
}

type WatchResponses = Pin<Box<dyn Stream<Item = Result<HealthCheckResponse, Status>> + Send>>;

#[tonic::async_trait]
impl Health for ReadinessHealth {
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let service = &request.get_ref().service;
        if !self.knows(service) {
            return Err(Status::not_found("service not registered"));
        }
        Ok(Response::new(response(self.listener_status())))
    }

    type WatchStream = WatchResponses;

    /// Reports the current status at once and again whenever it changes. A
    /// service name this listener does not serve reports `SERVICE_UNKNOWN`.
    async fn watch(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let mut poll = interval(WATCH_POLL_INTERVAL);
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let state = WatchState {
            health: self.clone(),
            service: request.into_inner().service,
            last_sent: None,
            poll,
        };
        Ok(Response::new(Box::pin(stream::unfold(
            state,
            next_watch_response,
        ))))
    }
}

/// Waits for the next status change, and ends the stream once shutdown has
/// started and its status has been sent.
async fn next_watch_response(
    mut state: WatchState,
) -> Option<(Result<HealthCheckResponse, Status>, WatchState)> {
    loop {
        let shutting_down = state.health.shutdown.is_cancelled();
        let status = state.health.status_of(&state.service);
        if state.last_sent != Some(status) {
            state.last_sent = Some(status);
            return Some((Ok(response(status)), state));
        }
        if shutting_down {
            return None;
        }
        tokio::select! {
            _ = state.poll.tick() => {}
            () = state.health.shutdown.cancelled() => {}
        }
    }
}

fn response(status: ServingStatus) -> HealthCheckResponse {
    HealthCheckResponse {
        status: status.into(),
    }
}
