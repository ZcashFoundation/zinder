//! Supervision of a runtime's public gRPC listener and the background tasks
//! it serves from.
//!
//! Every supervised task loops until the runtime stops, so a task that ends
//! earlier has failed. A supervised runtime stops when shutdown is requested,
//! when its gRPC server stops, or when one of its tasks ends, and always in
//! the same order:
//!
//! 1. It reports `shutting_down` through the runtime's readiness hook.
//! 2. It cancels [`SupervisedRuntime::stopping`], which stops the background
//!    tasks and starts the gRPC server's graceful drain.
//! 3. It waits up to the drain timeout for the server to drain.
//! 4. It ends every call the listener still serves, so no handler future or
//!    response body remains.
//! 5. It cancels [`SupervisedRuntime::serving_drained`], which lets the
//!    serving publishers release their generations.
//! 6. It waits for every task and reports each task that failed.
//!
//! A shutdown request is a signal of its own. The server and the tasks never
//! observe it, so a request can never look like a server or task stopping on
//! its own.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use futures_util::{StreamExt as _, future::BoxFuture, stream::FuturesUnordered};
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tower::layer::util::{Identity, Stack};

use crate::{
    GrpcReceiveLimitLayer, GrpcSendStallLayer, OpsEndpointHandle, OpsServerError, RuntimeService,
    cancel_on_terminating_signal,
    public_grpc_server::{GrpcCallTerminationLayer, OpenGrpcCalls},
    public_grpc_server_with,
};

/// Longest a supervised gRPC server may drain after the runtime starts
/// stopping before its open calls are ended.
///
/// A graceful drain waits for every open call, and a block-range stream to a
/// client that reads slowly stays open until its range ends. This bound keeps
/// such calls from holding the process past the single-volume entrypoint's
/// 20-second grace window, with room left for secondary teardown.
pub const GRPC_SERVER_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// The tonic server type that [`SupervisedRuntime::public_grpc_server`]
/// returns.
pub type SupervisedGrpcServer = tonic::transport::Server<
    Stack<
        GrpcCallTerminationLayer,
        Stack<GrpcSendStallLayer, Stack<GrpcReceiveLimitLayer, Identity>>,
    >,
>;

/// A runtime's public gRPC listener and background tasks, supervised so that
/// they stop together.
///
/// Build the listener with [`Self::public_grpc_server`], serve it until
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

    /// The public gRPC server builder for this runtime's listener.
    ///
    /// It applies the policy of [`crate::public_grpc_server`], and lets the
    /// supervisor end every call still open once the drain deadline passes.
    #[must_use]
    pub fn public_grpc_server(&self) -> SupervisedGrpcServer {
        public_grpc_server_with(
            crate::GrpcReceiveLimits::default(),
            crate::DEFAULT_GRPC_SEND_STALL_TIMEOUT,
        )
        .layer(GrpcCallTerminationLayer::new(Arc::clone(&self.open_calls)))
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
    /// error and every task stopped cleanly. Otherwise, and whenever the
    /// server or a task stopped first, it returns an error. Every task that
    /// failed is logged by name; the error names the task or server that
    /// stopped the runtime, or, for a requested shutdown, the first task that
    /// failed while stopping.
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
            Some(task_exit) = running.next() => RuntimeExit::Task(task_exit),
        };

        publish_shutting_down();
        self.stopping.cancel();
        if let RuntimeExit::Task((task, outcome)) = &exit {
            log_task_exit(service, task, outcome.as_ref().err());
        }

        let server_outcome = match exit {
            RuntimeExit::GrpcServer(_) => Ok(()),
            RuntimeExit::ShutdownRequested | RuntimeExit::Task(_) => {
                self.drain_grpc_server(server.as_mut()).await
            }
        };
        let ended_calls = self.open_calls.terminate();
        if ended_calls > 0 {
            tracing::warn!(
                target: "zinder::runtime",
                event = "runtime_grpc_calls_ended",
                service,
                ended_calls,
                "ended gRPC calls still open after the drain deadline"
            );
        }
        self.serving_drained.cancel();

        let mut first_stop_failure = None;
        while let Some((task, outcome)) = running.next().await {
            if let Err(failure) = outcome {
                log_task_exit(service, task, Some(&failure));
                first_stop_failure.get_or_insert(RuntimeSupervisionError::TaskFailed {
                    task,
                    source: failure,
                });
            }
        }

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
            RuntimeExit::Task((task, outcome)) => {
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

    /// Waits for the gRPC server's graceful drain, giving up on it after the
    /// drain timeout.
    async fn drain_grpc_server<Server>(
        &self,
        server: Pin<&mut Server>,
    ) -> Result<(), tonic::transport::Error>
    where
        Server: Future<Output = Result<(), tonic::transport::Error>>,
    {
        if let Ok(server_outcome) = tokio::time::timeout(self.grpc_drain_timeout, server).await {
            return server_outcome;
        }
        tracing::warn!(
            target: "zinder::runtime",
            event = "runtime_grpc_drain_timed_out",
            service = self.service.binary_name(),
            timeout_seconds = self.grpc_drain_timeout.as_secs_f64(),
            "gRPC server did not drain before its shutdown deadline; ending its open calls"
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

type TaskExit = (&'static str, Result<(), RuntimeTaskFailure>);

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
    tasks: Vec<(&'static str, SupervisedTask)>,
}

impl std::fmt::Debug for SupervisedTasks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_list()
            .entries(self.tasks.iter().map(|(task, _)| task))
            .finish()
    }
}

enum SupervisedTask {
    Spawned(JoinHandle<()>),
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
    pub fn with(mut self, task: &'static str, handle: JoinHandle<()>) -> Self {
        self.tasks.push((task, SupervisedTask::Spawned(handle)));
        self
    }

    /// Adds the operational endpoint, which the runtime closes once it
    /// starts stopping.
    #[must_use]
    pub fn with_operations_endpoint(mut self, operations: Option<OpsEndpointHandle>) -> Self {
        if let Some(handle) = operations {
            self.tasks
                .push(("operations endpoint", SupervisedTask::Operations(handle)));
        }
        self
    }

    fn start(self, stopping: &CancellationToken) -> FuturesUnordered<BoxFuture<'static, TaskExit>> {
        self.tasks
            .into_iter()
            .map(|(task, supervised)| -> BoxFuture<'static, TaskExit> {
                match supervised {
                    SupervisedTask::Spawned(handle) => {
                        Box::pin(
                            async move { (task, handle.await.map_err(RuntimeTaskFailure::Join)) },
                        )
                    }
                    SupervisedTask::Operations(handle) => {
                        let stopping = stopping.clone();
                        Box::pin(async move {
                            (
                                task,
                                watch_operations_endpoint(handle, stopping)
                                    .await
                                    .map_err(RuntimeTaskFailure::Operations),
                            )
                        })
                    }
                }
            })
            .collect()
    }
}

async fn watch_operations_endpoint(
    mut handle: OpsEndpointHandle,
    stopping: CancellationToken,
) -> Result<(), OpsServerError> {
    tokio::select! {
        exited = handle.wait() => exited,
        () = stopping.cancelled() => handle.shutdown().await,
    }
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
        GRPC_SERVER_DRAIN_TIMEOUT, RuntimeSupervisionError, SupervisedRuntime, SupervisedTasks,
    };
    use crate::RuntimeService;

    fn until_stopping(runtime: &SupervisedRuntime) -> JoinHandle<()> {
        let stopping = runtime.stopping();
        tokio::spawn(async move { stopping.cancelled().await })
    }

    fn aborted_task() -> JoinHandle<()> {
        let handle = tokio::spawn(future::pending::<()>());
        handle.abort();
        handle
    }

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

    /// A server whose open calls never finish stops holding the process once
    /// the drain bound passes, and the serving generations are released.
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
        assert_eq!(started_at.elapsed(), GRPC_SERVER_DRAIN_TIMEOUT);
        assert!(serving_drained.is_cancelled());
    }
}
