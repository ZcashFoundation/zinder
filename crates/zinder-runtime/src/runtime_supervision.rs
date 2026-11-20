//! Supervision of a runtime's public gRPC listener and the background tasks
//! it serves from.
//!
//! Every supervised task loops until the runtime stops, so a task that ends
//! earlier has failed, even when the supervisor first sees it end while the
//! runtime stops. A supervised runtime stops when shutdown is requested, when
//! its gRPC server stops, or when one of its tasks ends, and always in the
//! same order:
//!
//! 1. It reports `shutting_down` through the runtime's readiness hook, so
//!    `/readyz` and gRPC health report it before anything stops.
//! 2. It cancels [`SupervisedRuntime::stopping`], which stops the background
//!    tasks, the operational endpoint among them, and starts the gRPC
//!    server's graceful drain at the same time. Open health `Watch` streams
//!    end as the drain starts, so they never hold it open.
//! 3. It waits up to the drain timeout for the server to drain.
//! 4. It ends every call the listener still serves. Once it has, no handler
//!    future or response body of the listener remains, and each ended
//!    response sends its status before any more data.
//! 5. If the drain timed out, it waits up to [`GRPC_SERVER_CLOSE_TIMEOUT`]
//!    for the server to send those statuses and close its connections.
//! 6. It cancels [`SupervisedRuntime::serving_drained`], which lets the
//!    serving publishers release their generations.
//! 7. It waits for every task and reports each task that failed.
//!
//! The supervisor watches the tasks through every step, so they stop, and
//! their failures are logged, while the server drains.
//!
//! A shutdown request is a signal of its own. The server and the tasks never
//! observe it, so a request can never look like a server or task stopping on
//! its own.

use std::{convert::Infallible, future::Future, pin::Pin, sync::Arc, time::Duration};

use futures_util::{StreamExt as _, future::BoxFuture, stream::FuturesUnordered};
use thiserror::Error;
use tokio::task::{AbortHandle, JoinError, JoinHandle};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tonic::{body::Body, server::NamedService};
use tower::{
    Service,
    layer::util::{Identity, Stack},
};

use crate::{
    ClientAddressSource, DEFAULT_GRPC_SEND_STALL_TIMEOUT, GrpcCallTerminationLayer,
    GrpcReceiveLimitLayer, GrpcReceiveLimits, GrpcSendStallLayer, OpsEndpointHandle,
    OpsServerError, Readiness, RuntimeService, cancel_on_terminating_signal,
    public_grpc_server::{OpenGrpcCalls, supervised_grpc_layers, supervised_router},
};

/// Longest a supervised gRPC server may drain after the runtime starts
/// stopping before its open calls are ended.
///
/// A graceful drain waits for every open call, and a block-range stream to a
/// client that reads slowly stays open until its range ends. This bound keeps
/// such calls from holding the process past the single-volume entrypoint's
/// 20-second grace window, with room left for secondary teardown.
pub const GRPC_SERVER_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Longest a supervised gRPC server may take, once the calls it still served
/// at the drain deadline have ended, to send them their status and close its
/// connections.
///
/// A client that reads its stream needs only milliseconds. The bound is for a
/// client that has stopped reading, whose status cannot be delivered. With
/// [`GRPC_SERVER_DRAIN_TIMEOUT`], it stays within the single-volume
/// entrypoint's 20-second grace window.
pub const GRPC_SERVER_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

/// The tonic router type that [`SupervisedRuntime::public_grpc_router`]
/// returns.
pub type SupervisedGrpcRouter<L = Identity> = tonic::transport::server::Router<
    Stack<
        Stack<
            GrpcSendStallLayer,
            Stack<GrpcReceiveLimitLayer, Stack<GrpcCallTerminationLayer, Identity>>,
        >,
        L,
    >,
>;

/// A runtime's public gRPC listener and background tasks, supervised so that
/// they stop together.
///
/// Build the listener with [`Self::public_grpc_router`], serve it until
/// [`Self::grpc_server_shutdown`] resolves, give background tasks
/// [`Self::stopping`], and pass both to [`Self::supervise`].
#[derive(Debug)]
pub struct SupervisedRuntime {
    service: RuntimeService,
    shutdown_requested: CancellationToken,
    stopping: CancellationToken,
    serving_drained: CancellationToken,
    open_calls: Arc<OpenGrpcCalls>,
    grpc_drain_timeout: Duration,
}

impl SupervisedRuntime {
    /// Creates the supervision of `service`, with the default
    /// [`GRPC_SERVER_DRAIN_TIMEOUT`].
    #[must_use]
    pub fn new(service: RuntimeService) -> Self {
        Self {
            service,
            shutdown_requested: CancellationToken::new(),
            stopping: CancellationToken::new(),
            serving_drained: CancellationToken::new(),
            open_calls: Arc::new(OpenGrpcCalls::default()),
            grpc_drain_timeout: GRPC_SERVER_DRAIN_TIMEOUT,
        }
    }

    /// Replaces the gRPC drain timeout.
    ///
    /// Production runtimes keep [`GRPC_SERVER_DRAIN_TIMEOUT`]. This form
    /// exists so tests can reach the drain deadline quickly.
    #[must_use]
    pub const fn with_grpc_drain_timeout(mut self, grpc_drain_timeout: Duration) -> Self {
        self.grpc_drain_timeout = grpc_drain_timeout;
        self
    }

    /// Requests shutdown when the process receives `SIGINT` or `SIGTERM`.
    ///
    /// See [`cancel_on_terminating_signal`]; callers usually drop the handle.
    #[must_use = "drop the handle to detach the task or await it for symmetric shutdown"]
    pub fn request_shutdown_on_terminating_signal(&self) -> JoinHandle<()> {
        cancel_on_terminating_signal(self.shutdown_requested.clone())
    }

    /// A handle that requests this runtime's shutdown.
    #[must_use]
    pub fn shutdown_request(&self) -> ShutdownRequest {
        ShutdownRequest(self.shutdown_requested.clone())
    }

    /// The token cancelled once the runtime starts stopping.
    ///
    /// Background tasks run until it is cancelled. Only the supervisor
    /// cancels it, after it has reported `shutting_down`.
    #[must_use]
    pub fn stopping(&self) -> CancellationToken {
        self.stopping.clone()
    }

    /// The token cancelled once the gRPC server has drained and every call
    /// it still served has ended.
    ///
    /// Serving publishers hold their generations until it is cancelled.
    #[must_use]
    pub fn serving_drained(&self) -> CancellationToken {
        self.serving_drained.clone()
    }

    /// Assembles this runtime's public gRPC listener, which serves
    /// `application_service` and tells clients apart by `client_address`.
    ///
    /// The router serves, on the transport policy of
    /// [`crate::public_grpc_server`]:
    ///
    /// - `application_service`, behind a [`crate::TrafficReadinessInterceptor`]
    ///   over `readiness`;
    /// - gRPC reflection v1, plus v1alpha for `CompactTxStreamer`, describing
    ///   the application, health, and reflection services; and
    /// - `grpc.health.v1.Health`, `SERVING` exactly when `readiness` admits
    ///   traffic, as `/readyz` reports it.
    ///
    /// Reflection and health stay outside the traffic gate, so they answer
    /// while the runtime refuses application calls. Every call first passes
    /// through [`GrpcCallTerminationLayer`], so the supervisor can end the
    /// calls still open at the drain deadline.
    ///
    /// Serve the router until [`Self::grpc_server_shutdown`] resolves, and
    /// publish `shutting_down` to `readiness` from the hook
    /// [`Self::supervise`] runs first. Health then reports `NOT_SERVING`
    /// together with `/readyz` before the drain starts.
    ///
    /// # Errors
    ///
    /// Returns an error when reflection cannot describe
    /// `descriptor_set`.
    pub fn public_grpc_router<S, ResponseBody>(
        &self,
        application_service: S,
        descriptor_set: &'static [u8],
        client_address: ClientAddressSource,
        readiness: &Readiness,
    ) -> Result<SupervisedGrpcRouter, tonic_reflection::server::Error>
    where
        S: Service<
                http::Request<Body>,
                Response = http::Response<ResponseBody>,
                Error = Infallible,
            > + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        ResponseBody:
            http_body::Body<Data = bytes::Bytes, Error: Into<axum::BoxError>> + Send + 'static,
    {
        self.public_grpc_router_with_transport(
            (application_service, descriptor_set),
            client_address,
            readiness,
            std::convert::identity,
        )
    }

    /// Builds the same supervised router with an outer transport adapter.
    /// Pass the application service with its reflection descriptor set.
    /// The adapter translates incoming requests before the supervisor and receive
    /// limits inspect native gRPC frames.
    ///
    /// # Errors
    ///
    /// Returns an error when reflection cannot describe the application service.
    pub fn public_grpc_router_with_transport<S, L, ResponseBody>(
        &self,
        (application_service, descriptor_set): (S, &'static [u8]),
        client_address: ClientAddressSource,
        readiness: &Readiness,
        transport: impl FnOnce(tonic::transport::Server) -> tonic::transport::Server<L>,
    ) -> Result<SupervisedGrpcRouter<L>, tonic_reflection::server::Error>
    where
        L: Clone,
        S: Service<
                http::Request<Body>,
                Response = http::Response<ResponseBody>,
                Error = Infallible,
            > + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        ResponseBody:
            http_body::Body<Data = bytes::Bytes, Error: Into<axum::BoxError>> + Send + 'static,
    {
        let server = transport(crate::public_grpc_server::public_grpc_transport()).layer(
            supervised_grpc_layers(
                Arc::clone(&self.open_calls),
                GrpcReceiveLimits::default(),
                DEFAULT_GRPC_SEND_STALL_TIMEOUT,
                client_address,
            ),
        );
        supervised_router(
            server,
            application_service,
            descriptor_set,
            readiness,
            &self.stopping,
        )
    }

    /// The shutdown signal to serve the gRPC listener with, which resolves
    /// once the runtime starts stopping.
    pub fn grpc_server_shutdown(&self) -> WaitForCancellationFutureOwned {
        self.stopping.clone().cancelled_owned()
    }

    /// Runs `server` and `tasks` until shutdown is requested, the server
    /// stops, or a task ends, then stops the runtime in the order the module
    /// documentation lists.
    ///
    /// `publish_shutting_down` runs first, so no request is admitted while
    /// the runtime stops.
    ///
    /// # Exit status
    ///
    /// A requested shutdown returns `Ok` only when the gRPC server reported no
    /// error and every task stopped cleanly once the runtime started
    /// stopping. Otherwise, and whenever the server or a task stopped first,
    /// it returns an error. Every task that failed, or that had ended before
    /// the runtime started stopping, is logged by name. The error names the
    /// task or server that stopped the runtime, or, for a requested shutdown,
    /// the first such task.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeSupervisionError`] as described above.
    pub async fn supervise<Server>(
        self,
        server: Server,
        tasks: SupervisedTasks,
        publish_shutting_down: impl FnOnce(),
    ) -> Result<(), RuntimeSupervisionError>
    where
        Server: Future<Output = Result<(), tonic::transport::Error>>,
    {
        let service = self.service.binary_name();
        let mut running = tasks.start(&self.stopping);
        let mut server = std::pin::pin!(server);
        let exit = tokio::select! {
            biased;
            () = self.shutdown_requested.cancelled() => RuntimeExit::ShutdownRequested,
            server_outcome = &mut server => RuntimeExit::GrpcServer(server_outcome),
            Some(task_exit) = running.exits.next() => RuntimeExit::Task(task_exit),
        };

        running.record_tasks_already_ended();
        publish_shutting_down();
        self.stopping.cancel();
        if let RuntimeExit::Task(task_exit) = &exit {
            log_task_exit(service, task_exit.task, task_exit.outcome.as_ref().err());
        }

        let server_stopped = matches!(exit, RuntimeExit::GrpcServer(_));
        let (server_outcome, first_stop_failure) = tokio::join!(
            self.stop_serving(server.as_mut(), server_stopped),
            running.wait_for_stop(service),
        );

        match exit {
            RuntimeExit::ShutdownRequested => {
                if let Err(source) = &server_outcome {
                    log_server_drain_failure(service, source);
                }
                if let Some(failure) = first_stop_failure {
                    return Err(failure);
                }
                server_outcome.map_err(|source| RuntimeSupervisionError::GrpcServer { source })
            }
            RuntimeExit::GrpcServer(Ok(())) => Err(RuntimeSupervisionError::GrpcServerStopped),
            RuntimeExit::GrpcServer(Err(source)) => {
                Err(RuntimeSupervisionError::GrpcServer { source })
            }
            RuntimeExit::Task(TaskExit { task, outcome, .. }) => {
                if let Err(source) = &server_outcome {
                    log_server_drain_failure(service, source);
                }
                Err(match outcome {
                    Ok(()) => RuntimeSupervisionError::TaskStopped { task },
                    Err(source) => RuntimeSupervisionError::TaskFailed { task, source },
                })
            }
        }
    }

    /// Drains the gRPC server, ends every call it still serves, and then
    /// releases the serving generations.
    ///
    /// A server that has already stopped is not polled again.
    async fn stop_serving<Server>(
        &self,
        mut server: Pin<&mut Server>,
        server_stopped: bool,
    ) -> Result<(), tonic::transport::Error>
    where
        Server: Future<Output = Result<(), tonic::transport::Error>>,
    {
        let drained = if server_stopped {
            Some(Ok(()))
        } else {
            self.drain_grpc_server(server.as_mut()).await
        };
        let ended_calls = self.open_calls.terminate();
        if ended_calls > 0 {
            tracing::warn!(
                target: "zinder::runtime",
                event = "runtime_grpc_calls_ended",
                service = self.service.binary_name(),
                ended_calls,
                "ended gRPC calls still open after the drain deadline"
            );
        }
        let server_outcome = match drained {
            Some(server_outcome) => server_outcome,
            None => self.close_grpc_server(server).await,
        };
        self.serving_drained.cancel();
        server_outcome
    }

    /// Waits for the gRPC server's graceful drain, or returns `None` once
    /// the drain timeout passes.
    async fn drain_grpc_server<Server>(
        &self,
        server: Pin<&mut Server>,
    ) -> Option<Result<(), tonic::transport::Error>>
    where
        Server: Future<Output = Result<(), tonic::transport::Error>>,
    {
        if let Ok(server_outcome) = tokio::time::timeout(self.grpc_drain_timeout, server).await {
            return Some(server_outcome);
        }
        tracing::warn!(
            target: "zinder::runtime",
            event = "runtime_grpc_drain_timed_out",
            service = self.service.binary_name(),
            timeout_seconds = self.grpc_drain_timeout.as_secs_f64(),
            "gRPC server did not drain before its shutdown deadline; ending its open calls"
        );
        None
    }

    /// Waits for the gRPC server to send the calls it ended their status and
    /// close its connections, giving up after [`GRPC_SERVER_CLOSE_TIMEOUT`].
    async fn close_grpc_server<Server>(
        &self,
        server: Pin<&mut Server>,
    ) -> Result<(), tonic::transport::Error>
    where
        Server: Future<Output = Result<(), tonic::transport::Error>>,
    {
        if let Ok(server_outcome) = tokio::time::timeout(GRPC_SERVER_CLOSE_TIMEOUT, server).await {
            return server_outcome;
        }
        tracing::warn!(
            target: "zinder::runtime",
            event = "runtime_grpc_close_timed_out",
            service = self.service.binary_name(),
            timeout_seconds = GRPC_SERVER_CLOSE_TIMEOUT.as_secs_f64(),
            "gRPC server did not close its connections after its open calls ended; their clients may not receive the calls' status"
        );
        Ok(())
    }
}

/// Requests a [`SupervisedRuntime`]'s shutdown.
#[derive(Clone, Debug)]
pub struct ShutdownRequest(CancellationToken);

impl ShutdownRequest {
    /// Requests shutdown. Requesting it again has no further effect.
    pub fn request(&self) {
        self.0.cancel();
    }
}

/// How a supervised task ended.
struct TaskExit {
    /// The task's position in its [`SupervisedTasks`].
    index: usize,
    task: &'static str,
    outcome: Result<(), RuntimeTaskFailure>,
}

enum RuntimeExit {
    ShutdownRequested,
    GrpcServer(Result<(), tonic::transport::Error>),
    Task(TaskExit),
}

fn log_task_exit(service: &'static str, task: &'static str, failure: Option<&RuntimeTaskFailure>) {
    if let Some(error) = failure {
        tracing::error!(
            target: "zinder::runtime",
            event = "runtime_task_failed",
            service,
            task,
            error = %error,
            "runtime task failed"
        );
    } else {
        tracing::error!(
            target: "zinder::runtime",
            event = "runtime_task_stopped",
            service,
            task,
            "runtime task stopped before runtime shutdown"
        );
    }
}

fn log_server_drain_failure(service: &'static str, error: &tonic::transport::Error) {
    tracing::error!(
        target: "zinder::runtime",
        event = "runtime_grpc_drain_failed",
        service,
        error = %error,
        "gRPC server failed while draining"
    );
}

/// The background tasks a [`SupervisedRuntime`] watches, each under the name
/// its failure reports.
#[derive(Default)]
pub struct SupervisedTasks {
    tasks: Vec<SupervisedTask>,
}

impl std::fmt::Debug for SupervisedTasks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_list()
            .entries(self.tasks.iter().map(|task| task.name))
            .finish()
    }
}

struct SupervisedTask {
    name: &'static str,
    /// Tells whether the task has ended, without waiting for it.
    ended: Option<AbortHandle>,
    run: TaskRun,
}

enum TaskRun {
    Spawned(BoxFuture<'static, Result<(), RuntimeTaskFailure>>),
    Operations(OpsEndpointHandle),
}

impl SupervisedTasks {
    /// An empty task set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a spawned task that runs until [`SupervisedRuntime::stopping`]
    /// is cancelled.
    #[must_use]
    pub fn with(self, task: &'static str, handle: JoinHandle<()>) -> Self {
        let ended = handle.abort_handle();
        self.push(
            task,
            Some(ended),
            TaskRun::Spawned(Box::pin(async move {
                handle.await.map_err(RuntimeTaskFailure::Join)
            })),
        )
    }

    /// Adds a spawned task that runs until [`SupervisedRuntime::stopping`]
    /// is cancelled and returns an error when it fails, such as a serving
    /// publisher whose teardown can fail after the runtime starts stopping.
    #[must_use]
    pub fn with_fallible<E>(self, task: &'static str, handle: JoinHandle<Result<(), E>>) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let ended = handle.abort_handle();
        self.push(
            task,
            Some(ended),
            TaskRun::Spawned(Box::pin(async move {
                match handle.await {
                    Ok(outcome) => {
                        outcome.map_err(|error| RuntimeTaskFailure::Failed(Box::new(error)))
                    }
                    Err(join_error) => Err(RuntimeTaskFailure::Join(join_error)),
                }
            })),
        )
    }

    /// Adds the operational endpoint, which the runtime closes once it
    /// starts stopping.
    #[must_use]
    pub fn with_operations_endpoint(self, operations: Option<OpsEndpointHandle>) -> Self {
        match operations {
            Some(handle) => {
                let ended = handle.server_task();
                self.push("operations endpoint", ended, TaskRun::Operations(handle))
            }
            None => self,
        }
    }

    fn push(mut self, name: &'static str, ended: Option<AbortHandle>, run: TaskRun) -> Self {
        self.tasks.push(SupervisedTask { name, ended, run });
        self
    }

    fn start(self, stopping: &CancellationToken) -> RunningTasks {
        let mut running = RunningTasks {
            exits: FuturesUnordered::new(),
            ended: Vec::with_capacity(self.tasks.len()),
            ended_before_stopping: Vec::new(),
        };
        for (index, SupervisedTask { name, ended, run }) in self.tasks.into_iter().enumerate() {
            let outcome = match run {
                TaskRun::Spawned(outcome) => outcome,
                TaskRun::Operations(handle) => {
                    Box::pin(watch_operations_endpoint(handle, stopping.clone()))
                }
            };
            running.ended.push(ended);
            running.exits.push(Box::pin(async move {
                TaskExit {
                    index,
                    task: name,
                    outcome: outcome.await,
                }
            }));
        }
        running
    }
}

/// The supervised tasks of a running runtime.
struct RunningTasks {
    exits: FuturesUnordered<BoxFuture<'static, TaskExit>>,
    /// Tells, for each task by position, whether it has ended.
    ended: Vec<Option<AbortHandle>>,
    /// Whether each task, by position, had ended before the runtime started
    /// stopping.
    ended_before_stopping: Vec<bool>,
}

impl RunningTasks {
    /// Records which tasks have already ended, before the runtime starts
    /// stopping. Each of them stopped on its own, however late its exit is
    /// observed.
    fn record_tasks_already_ended(&mut self) {
        self.ended_before_stopping = self
            .ended
            .iter()
            .map(|ended| ended.as_ref().is_some_and(AbortHandle::is_finished))
            .collect();
    }

    /// Waits for every task still running, logging each one that failed or
    /// had ended before the runtime started stopping, and returns the first.
    async fn wait_for_stop(&mut self, service: &'static str) -> Option<RuntimeSupervisionError> {
        let mut first_failure = None;
        while let Some(TaskExit {
            index,
            task,
            outcome,
        }) = self.exits.next().await
        {
            let failure = match outcome {
                Err(source) => {
                    log_task_exit(service, task, Some(&source));
                    RuntimeSupervisionError::TaskFailed { task, source }
                }
                Ok(()) if self.ended_before_stopping.get(index) == Some(&true) => {
                    log_task_exit(service, task, None);
                    RuntimeSupervisionError::TaskStopped { task }
                }
                Ok(()) => continue,
            };
            first_failure.get_or_insert(failure);
        }
        first_failure
    }
}

async fn watch_operations_endpoint(
    mut handle: OpsEndpointHandle,
    stopping: CancellationToken,
) -> Result<(), RuntimeTaskFailure> {
    tokio::select! {
        exited = handle.wait() => exited,
        () = stopping.cancelled() => handle.shutdown().await,
    }
    .map_err(RuntimeTaskFailure::Operations)
}

/// Why a supervised task failed.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RuntimeTaskFailure {
    /// The task panicked or was aborted.
    #[error(transparent)]
    Join(JoinError),

    /// The operational endpoint failed.
    #[error(transparent)]
    Operations(OpsServerError),

    /// The task returned an error.
    #[error(transparent)]
    Failed(Box<dyn std::error::Error + Send + Sync>),
}

/// Why a supervised runtime stopped with an error.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RuntimeSupervisionError {
    /// A task ended before the runtime started stopping.
    #[error("{task} stopped before runtime shutdown")]
    TaskStopped {
        /// The task's name.
        task: &'static str,
    },

    /// A task failed.
    #[error("{task} failed: {source}")]
    TaskFailed {
        /// The task's name.
        task: &'static str,
        /// Why it failed.
        #[source]
        source: RuntimeTaskFailure,
    },

    /// The gRPC server stopped before the runtime started stopping.
    #[error("gRPC server stopped before runtime shutdown")]
    GrpcServerStopped,

    /// The gRPC server failed.
    #[error("gRPC server failed: {source}")]
    GrpcServer {
        /// The transport failure.
        #[source]
        source: tonic::transport::Error,
    },
}

#[cfg(test)]
mod tests {
    use std::{
        future,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;
    use zinder_testkit::LogCapture;

    use super::{
        GRPC_SERVER_CLOSE_TIMEOUT, GRPC_SERVER_DRAIN_TIMEOUT, RuntimeSupervisionError,
        RuntimeTaskFailure, SupervisedRuntime, SupervisedTasks,
    };
    use crate::{OpsEndpointHandle, OpsServerError, RuntimeService};

    fn until_stopping(runtime: &SupervisedRuntime) -> JoinHandle<()> {
        let stopping = runtime.stopping();
        tokio::spawn(async move { stopping.cancelled().await })
    }

    fn aborted_task() -> JoinHandle<()> {
        let handle = tokio::spawn(future::pending::<()>());
        handle.abort();
        handle
    }

    /// Spawns a task that ends at once, and returns it once it has ended.
    async fn ended_task() -> JoinHandle<()> {
        let handle = tokio::spawn(async {});
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
        handle
    }

    /// An operational endpoint that serves until `cancel` is cancelled, and
    /// then reports through `closed` that it has closed.
    fn operations_endpoint(closed: CancellationToken) -> OpsEndpointHandle {
        let cancel = CancellationToken::new();
        let server = tokio::spawn({
            let cancel = cancel.clone();
            async move {
                cancel.cancelled().await;
                closed.cancel();
                Ok(())
            }
        });
        OpsEndpointHandle::for_task(cancel, server)
    }

    #[derive(Debug, thiserror::Error)]
    #[error("teardown failed")]
    struct TeardownFailed;

    /// What a supervised run observed once the supervisor returned.
    struct RuntimeRun {
        outcome: Result<(), RuntimeSupervisionError>,
        published_shutting_down: bool,
        server_stopped: bool,
        server_drained_before_release: bool,
    }

    /// Supervises a server that drains as soon as the runtime starts
    /// stopping, with the tasks `make_tasks` builds.
    async fn run_supervised(
        runtime: SupervisedRuntime,
        make_tasks: impl FnOnce(&SupervisedRuntime) -> SupervisedTasks,
    ) -> RuntimeRun {
        let tasks = make_tasks(&runtime);
        let serving_drained = runtime.serving_drained();
        let server_stopped = Arc::new(AtomicBool::new(false));
        let server_drained_before_release = Arc::new(AtomicBool::new(false));
        let server = {
            let shutdown = runtime.grpc_server_shutdown();
            let server_stopped = Arc::clone(&server_stopped);
            let server_drained_before_release = Arc::clone(&server_drained_before_release);
            async move {
                shutdown.await;
                server_drained_before_release
                    .store(!serving_drained.is_cancelled(), Ordering::SeqCst);
                server_stopped.store(true, Ordering::SeqCst);
                Ok(())
            }
        };
        let published_shutting_down = Arc::new(AtomicBool::new(false));
        let outcome = runtime
            .supervise(server, tasks, || {
                published_shutting_down.store(true, Ordering::SeqCst);
            })
            .await;
        RuntimeRun {
            outcome,
            published_shutting_down: published_shutting_down.load(Ordering::SeqCst),
            server_stopped: server_stopped.load(Ordering::SeqCst),
            server_drained_before_release: server_drained_before_release.load(Ordering::SeqCst),
        }
    }

    #[tokio::test]
    async fn a_task_that_stops_early_stops_the_runtime_and_is_named() {
        let run = run_supervised(SupervisedRuntime::new(RuntimeService::Query), |runtime| {
            SupervisedTasks::new()
                .with("first task", until_stopping(runtime))
                .with("ended task", tokio::spawn(async {}))
        })
        .await;

        assert!(matches!(
            run.outcome,
            Err(RuntimeSupervisionError::TaskStopped { task: "ended task" })
        ));
        assert!(run.published_shutting_down);
        assert!(run.server_stopped, "the server must stop");
        assert!(
            run.server_drained_before_release,
            "serving generations must outlive the server drain"
        );
    }

    #[tokio::test]
    async fn a_task_that_fails_to_join_is_reported_by_name() {
        let run = run_supervised(SupervisedRuntime::new(RuntimeService::Query), |runtime| {
            SupervisedTasks::new()
                .with("first task", until_stopping(runtime))
                .with("aborted task", aborted_task())
        })
        .await;

        assert!(matches!(
            run.outcome,
            Err(RuntimeSupervisionError::TaskFailed {
                task: "aborted task",
                ..
            })
        ));
        assert!(run.published_shutting_down);
        assert!(run.server_stopped);
    }

    #[tokio::test]
    async fn requested_shutdown_stops_every_task_cleanly() -> Result<(), RuntimeSupervisionError> {
        let runtime = SupervisedRuntime::new(RuntimeService::Query);
        runtime.shutdown_request().request();
        let run = run_supervised(runtime, |runtime| {
            SupervisedTasks::new()
                .with("first task", until_stopping(runtime))
                .with("second task", until_stopping(runtime))
        })
        .await;

        run.outcome?;
        assert!(run.published_shutting_down);
        assert!(run.server_stopped);
        assert!(run.server_drained_before_release);
        Ok(())
    }

    /// Load balancers read `/readyz` and gRPC health, so the runtime reports
    /// `shutting_down` before the listener drains or any task stops.
    #[tokio::test]
    async fn shutting_down_is_published_before_the_runtime_starts_stopping()
    -> Result<(), RuntimeSupervisionError> {
        let runtime = SupervisedRuntime::new(RuntimeService::Query);
        let stopping = runtime.stopping();
        let tasks = SupervisedTasks::new().with("task", until_stopping(&runtime));
        let shutdown = runtime.grpc_server_shutdown();
        let server = async move {
            shutdown.await;
            Ok(())
        };
        runtime.shutdown_request().request();
        let mut stopping_when_published = None;

        runtime
            .supervise(server, tasks, || {
                stopping_when_published = Some(stopping.is_cancelled());
            })
            .await?;

        assert_eq!(stopping_when_published, Some(false));
        Ok(())
    }

    /// A task that ended cleanly just before a shutdown request is seen
    /// first while the runtime stops. It still stopped on its own.
    #[tokio::test]
    async fn a_task_that_ended_before_a_requested_shutdown_is_reported_by_name() {
        let capture = LogCapture::install_for_target("zinder::runtime");
        let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd);
        let ended = ended_task().await;
        runtime.shutdown_request().request();

        let run = run_supervised(runtime, |runtime| {
            SupervisedTasks::new()
                .with("running task", until_stopping(runtime))
                .with("ended task", ended)
        })
        .await;

        assert!(
            matches!(
                run.outcome,
                Err(RuntimeSupervisionError::TaskStopped { task: "ended task" })
            ),
            "expected the runtime to fail naming the ended task, got {:?}",
            run.outcome
        );
        assert!(capture.events().iter().any(|event| {
            event.field("event") == Some("runtime_task_stopped")
                && event.field("task") == Some("ended task")
        }));
    }

    /// A shutdown request that lands after the supervisor checked for one,
    /// while the server is being polled, used to stop an idle server first
    /// and so look like the server stopping on its own.
    #[tokio::test]
    async fn a_shutdown_request_seen_while_the_server_runs_exits_cleanly() {
        let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd);
        let shutdown = runtime.grpc_server_shutdown();
        let request = runtime.shutdown_request();
        let server = async move {
            request.request();
            shutdown.await;
            Ok(())
        };

        let outcome = runtime
            .supervise(server, SupervisedTasks::new(), || {})
            .await;

        assert!(outcome.is_ok(), "expected a clean exit, got {outcome:?}");
    }

    #[tokio::test]
    async fn a_server_that_stops_on_its_own_is_a_runtime_failure() {
        let runtime = SupervisedRuntime::new(RuntimeService::Query);
        let tasks = SupervisedTasks::new().with("first task", until_stopping(&runtime));
        let published_shutting_down = AtomicBool::new(false);

        let outcome = runtime
            .supervise(async { Ok(()) }, tasks, || {
                published_shutting_down.store(true, Ordering::SeqCst);
            })
            .await;

        assert!(matches!(
            outcome,
            Err(RuntimeSupervisionError::GrpcServerStopped)
        ));
        assert!(published_shutting_down.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn requested_shutdown_reports_every_task_that_fails_while_stopping() {
        let capture = LogCapture::install_for_target("zinder::runtime");
        let runtime = SupervisedRuntime::new(RuntimeService::Query);
        runtime.shutdown_request().request();

        let run = run_supervised(runtime, |runtime| {
            SupervisedTasks::new()
                .with("clean task", until_stopping(runtime))
                .with("first aborted task", aborted_task())
                .with("second aborted task", aborted_task())
        })
        .await;

        assert!(matches!(
            run.outcome,
            Err(RuntimeSupervisionError::TaskFailed {
                task: "first aborted task" | "second aborted task",
                ..
            })
        ));
        let failed_tasks: Vec<String> = capture
            .events()
            .iter()
            .filter(|event| event.field("event") == Some("runtime_task_failed"))
            .filter_map(|event| event.field("task").map(str::to_owned))
            .collect();
        assert_eq!(failed_tasks.len(), 2, "logged failures: {failed_tasks:?}");
        assert!(failed_tasks.iter().any(|task| task == "first aborted task"));
        assert!(
            failed_tasks
                .iter()
                .any(|task| task == "second aborted task")
        );
    }

    /// A publisher's teardown runs after the runtime starts stopping, so its
    /// failure must still fail a requested shutdown.
    #[tokio::test]
    async fn a_fallible_task_that_fails_while_stopping_fails_a_requested_shutdown() {
        let runtime = SupervisedRuntime::new(RuntimeService::Query);
        runtime.shutdown_request().request();

        let run = run_supervised(runtime, |runtime| {
            let serving_drained = runtime.serving_drained();
            SupervisedTasks::new().with_fallible(
                "serving-pair publisher",
                tokio::spawn(async move {
                    serving_drained.cancelled().await;
                    Err::<(), _>(TeardownFailed)
                }),
            )
        })
        .await;

        assert!(
            matches!(
                &run.outcome,
                Err(RuntimeSupervisionError::TaskFailed {
                    task: "serving-pair publisher",
                    source: RuntimeTaskFailure::Failed(_),
                })
            ),
            "expected the publisher's teardown failure, got {:?}",
            run.outcome
        );
    }

    #[tokio::test]
    async fn an_operations_endpoint_that_fails_stops_the_runtime_and_is_named() {
        let run = run_supervised(SupervisedRuntime::new(RuntimeService::Query), |runtime| {
            let failed = tokio::spawn(async {
                Err(OpsServerError::Transport {
                    source: std::io::Error::other("listener failed"),
                })
            });
            SupervisedTasks::new()
                .with("first task", until_stopping(runtime))
                .with_operations_endpoint(Some(OpsEndpointHandle::for_task(
                    CancellationToken::new(),
                    failed,
                )))
        })
        .await;

        assert!(
            matches!(
                &run.outcome,
                Err(RuntimeSupervisionError::TaskFailed {
                    task: "operations endpoint",
                    source: RuntimeTaskFailure::Operations(_),
                })
            ),
            "expected the operations endpoint to be named, got {:?}",
            run.outcome
        );
        assert!(run.published_shutting_down);
        assert!(run.server_stopped);
    }

    /// The operational endpoint closes while the gRPC server drains, not
    /// after. The server here drains only once the endpoint has closed.
    #[tokio::test(start_paused = true)]
    async fn the_operations_endpoint_closes_while_the_grpc_server_drains() {
        let runtime = SupervisedRuntime::new(RuntimeService::Query);
        let operations_closed = CancellationToken::new();
        let tasks = SupervisedTasks::new()
            .with_operations_endpoint(Some(operations_endpoint(operations_closed.clone())));
        let shutdown = runtime.grpc_server_shutdown();
        let server = async move {
            shutdown.await;
            operations_closed.cancelled().await;
            Ok(())
        };
        runtime.shutdown_request().request();
        let started_at = tokio::time::Instant::now();

        let outcome = runtime.supervise(server, tasks, || {}).await;

        assert!(outcome.is_ok(), "expected a clean exit, got {outcome:?}");
        assert!(
            started_at.elapsed() < GRPC_SERVER_DRAIN_TIMEOUT,
            "the server drained only at its deadline, after {:?}",
            started_at.elapsed()
        );
    }

    /// A server whose open calls never finish stops holding the process once
    /// the drain and close bounds pass, and the serving generations are
    /// released.
    #[tokio::test(start_paused = true)]
    async fn requested_shutdown_gives_up_on_a_server_that_does_not_drain() {
        let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd);
        let serving_drained: CancellationToken = runtime.serving_drained();
        runtime.shutdown_request().request();
        let started_at = tokio::time::Instant::now();

        let outcome = runtime
            .supervise(future::pending(), SupervisedTasks::new(), || {})
            .await;

        assert!(outcome.is_ok());
        assert_eq!(
            started_at.elapsed(),
            GRPC_SERVER_DRAIN_TIMEOUT + GRPC_SERVER_CLOSE_TIMEOUT
        );
        assert!(serving_drained.is_cancelled());
    }
}
