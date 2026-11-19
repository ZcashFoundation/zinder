//! Supervision of the compatibility runtime's gRPC server and background tasks.
//!
//! Every background task loops until cancellation, so any exit before shutdown
//! is a fault. The supervisor drains readiness, stops the server, and returns
//! an error that names the task, so the platform restarts the process instead
//! of leaving it serving on that task's last state.

use std::{future::Future, pin::Pin, time::Duration};

use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::config::LightwalletdConfigError;

/// Longest the gRPC server may drain after shutdown starts before open calls
/// are abandoned.
///
/// A graceful drain waits for every open call, and a block-range stream to a
/// client that reads slowly stays open until its range ends. This bound keeps
/// such calls from holding the process past the wallet-serving supervisor's
/// 20-second grace window, with room left for secondary teardown.
pub(crate) const GRPC_SERVER_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

type TaskOutcome = (&'static str, Result<(), JoinError>);

/// Background tasks the supervisor watches, each under the name an
/// unexpected exit reports.
#[derive(Default)]
pub(crate) struct SupervisedTasks {
    tasks: JoinSet<TaskOutcome>,
}

impl SupervisedTasks {
    fn with(mut self, task: &'static str, handle: JoinHandle<()>) -> Self {
        self.tasks.spawn(async move { (task, handle.await) });
        self
    }
}

/// The background tasks of wallet serving.
pub(crate) struct WalletServingTasks {
    pub(crate) serving_pair_publisher: JoinHandle<()>,
    pub(crate) node_readiness_probe: JoinHandle<()>,
    pub(crate) ingest_control_readiness_probe: JoinHandle<()>,
    pub(crate) tip_change_publisher: JoinHandle<()>,
    pub(crate) node_tip_witness: JoinHandle<()>,
}

impl WalletServingTasks {
    pub(crate) fn supervised(self) -> SupervisedTasks {
        SupervisedTasks::default()
            .with("serving-pair publisher", self.serving_pair_publisher)
            .with("node-readiness probe", self.node_readiness_probe)
            .with(
                "ingest-control readiness probe",
                self.ingest_control_readiness_probe,
            )
            .with("tip-change publisher", self.tip_change_publisher)
            .with("node-tip witness", self.node_tip_witness)
    }
}

/// The background tasks of compact-blocks serving.
pub(crate) struct CompactBlocksServingTasks {
    pub(crate) compact_block_publisher: JoinHandle<()>,
    pub(crate) node_tip_witness: JoinHandle<()>,
}

impl CompactBlocksServingTasks {
    pub(crate) fn supervised(self) -> SupervisedTasks {
        SupervisedTasks::default()
            .with("compact-block publisher", self.compact_block_publisher)
            .with("node-tip witness", self.node_tip_witness)
    }
}

enum RuntimeExit {
    ShutdownRequested,
    GrpcServer(Result<(), tonic::transport::Error>),
    Task(TaskOutcome),
}

/// Runs `server` until shutdown is requested, the server stops, or a
/// supervised task exits.
///
/// Readiness drains first, so no request is admitted while the runtime stops.
/// `serving_runtime_drained` is cancelled only after the server drains, so
/// retired serving generations are not torn down under live readers.
pub(crate) async fn supervise_compat_runtime<Server>(
    server: Server,
    cancel: &CancellationToken,
    publish_shutting_down: impl FnOnce(),
    serving_runtime_drained: &CancellationToken,
    mut tasks: SupervisedTasks,
) -> Result<(), LightwalletdConfigError>
where
    Server: Future<Output = Result<(), tonic::transport::Error>>,
{
    let mut server = std::pin::pin!(server);
    let exit = tokio::select! {
        biased;
        () = cancel.cancelled() => RuntimeExit::ShutdownRequested,
        server_outcome = &mut server => RuntimeExit::GrpcServer(server_outcome),
        Some(task_outcome) = tasks.tasks.join_next() => {
            RuntimeExit::Task(task_outcome.unwrap_or_else(|source| ("task supervisor", Err(source))))
        }
    };

    publish_shutting_down();
    cancel.cancel();

    match exit {
        RuntimeExit::ShutdownRequested => {
            let server_result = drain_grpc_server(server.as_mut()).await;
            serving_runtime_drained.cancel();
            let mut task_failure = None;
            while let Some(joined) = tasks.tasks.join_next().await {
                let (task, outcome) =
                    joined.unwrap_or_else(|source| ("task supervisor", Err(source)));
                if let Err(source) = outcome {
                    task_failure
                        .get_or_insert(LightwalletdConfigError::RuntimeTaskJoin { task, source });
                }
            }
            if let Some(error) = task_failure {
                return Err(error);
            }
            server_result.map_err(LightwalletdConfigError::Transport)
        }
        RuntimeExit::GrpcServer(server_result) => {
            serving_runtime_drained.cancel();
            drain_tasks_after_runtime_failure(&mut tasks).await;
            server_result.map_err(LightwalletdConfigError::Transport)?;
            Err(LightwalletdConfigError::GrpcServerStopped)
        }
        RuntimeExit::Task((task, outcome)) => {
            let task_error = match outcome {
                Ok(()) => LightwalletdConfigError::RuntimeTaskStopped { task },
                Err(source) => LightwalletdConfigError::RuntimeTaskJoin { task, source },
            };
            if let Err(error) = drain_grpc_server(server.as_mut()).await {
                tracing::warn!(
                    target: "zinder::compat_lightwalletd",
                    event = "compat_server_drain_failed",
                    error = %error,
                    "lightwalletd-compatible gRPC server failed while draining after a runtime task failure"
                );
            }
            serving_runtime_drained.cancel();
            drain_tasks_after_runtime_failure(&mut tasks).await;
            Err(task_error)
        }
    }
}

/// Waits for the gRPC server's graceful drain, abandoning calls that are
/// still open after [`GRPC_SERVER_DRAIN_TIMEOUT`].
async fn drain_grpc_server<Server>(server: Pin<&mut Server>) -> Result<(), tonic::transport::Error>
where
    Server: Future<Output = Result<(), tonic::transport::Error>>,
{
    if let Ok(server_outcome) = tokio::time::timeout(GRPC_SERVER_DRAIN_TIMEOUT, server).await {
        return server_outcome;
    }
    tracing::warn!(
        target: "zinder::compat_lightwalletd",
        event = "compat_server_drain_timed_out",
        timeout_seconds = GRPC_SERVER_DRAIN_TIMEOUT.as_secs(),
        "lightwalletd-compatible gRPC server did not drain before its shutdown deadline; abandoning open calls"
    );
    Ok(())
}

async fn drain_tasks_after_runtime_failure(tasks: &mut SupervisedTasks) {
    while let Some(joined) = tasks.tasks.join_next().await {
        let (task, outcome) = joined.unwrap_or_else(|source| ("task supervisor", Err(source)));
        if let Err(error) = outcome {
            tracing::warn!(
                target: "zinder::compat_lightwalletd",
                event = "compat_runtime_task_drain_failed",
                task,
                error = %error,
                "compatibility runtime task failed while draining after another runtime failure"
            );
        }
    }
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

    use zinder_query::WalletServingReadiness;
    use zinder_runtime::{Readiness, ReadinessCause, ReadinessState};

    use super::*;

    fn wait_for_cancel(cancel: &CancellationToken) -> JoinHandle<()> {
        let cancel = cancel.clone();
        tokio::spawn(async move { cancel.cancelled().await })
    }

    /// Spawns the task `task_name` as one that has already ended, or as a
    /// task that runs until cancellation.
    fn task_for(
        task_name: &str,
        ended_task: &str,
        ending: impl FnOnce() -> JoinHandle<()>,
        cancel: &CancellationToken,
    ) -> JoinHandle<()> {
        if task_name == ended_task {
            ending()
        } else {
            wait_for_cancel(cancel)
        }
    }

    fn wallet_tasks(
        cancel: &CancellationToken,
        ended_task: &str,
        ending: impl Fn() -> JoinHandle<()>,
    ) -> SupervisedTasks {
        WalletServingTasks {
            serving_pair_publisher: task_for("serving-pair publisher", ended_task, &ending, cancel),
            node_readiness_probe: task_for("node-readiness probe", ended_task, &ending, cancel),
            ingest_control_readiness_probe: task_for(
                "ingest-control readiness probe",
                ended_task,
                &ending,
                cancel,
            ),
            tip_change_publisher: task_for("tip-change publisher", ended_task, &ending, cancel),
            node_tip_witness: task_for("node-tip witness", ended_task, &ending, cancel),
        }
        .supervised()
    }

    fn compact_blocks_tasks(
        cancel: &CancellationToken,
        ended_task: &str,
        ending: impl Fn() -> JoinHandle<()>,
    ) -> SupervisedTasks {
        CompactBlocksServingTasks {
            compact_block_publisher: task_for(
                "compact-block publisher",
                ended_task,
                &ending,
                cancel,
            ),
            node_tip_witness: task_for("node-tip witness", ended_task, &ending, cancel),
        }
        .supervised()
    }

    const WALLET_TASK_NAMES: [&str; 5] = [
        "serving-pair publisher",
        "node-readiness probe",
        "ingest-control readiness probe",
        "tip-change publisher",
        "node-tip witness",
    ];
    const COMPACT_BLOCKS_TASK_NAMES: [&str; 2] = ["compact-block publisher", "node-tip witness"];

    /// What a supervised run observed once the supervisor returned.
    struct RuntimeRun {
        outcome: Result<(), LightwalletdConfigError>,
        readiness: Readiness,
        server_stopped: bool,
        drained_after_server: bool,
    }

    async fn run_supervised(
        make_tasks: impl FnOnce(&CancellationToken) -> SupervisedTasks,
        shutdown_requested: bool,
    ) -> RuntimeRun {
        let readiness = Readiness::new(ReadinessState::ready(Some(1)));
        let serving_readiness = WalletServingReadiness::pair_only(readiness.clone());
        let cancel = CancellationToken::new();
        let serving_runtime_drained = CancellationToken::new();
        let tasks = make_tasks(&cancel);
        let server_stopped = Arc::new(AtomicBool::new(false));
        let drained_after_server = Arc::new(AtomicBool::new(false));
        let server = {
            let cancel = cancel.clone();
            let serving_runtime_drained = serving_runtime_drained.clone();
            let server_stopped = Arc::clone(&server_stopped);
            let drained_after_server = Arc::clone(&drained_after_server);
            async move {
                cancel.cancelled().await;
                drained_after_server
                    .store(!serving_runtime_drained.is_cancelled(), Ordering::SeqCst);
                server_stopped.store(true, Ordering::SeqCst);
                Ok(())
            }
        };
        if shutdown_requested {
            cancel.cancel();
        }

        let outcome = supervise_compat_runtime(
            server,
            &cancel,
            || serving_readiness.publish_shutting_down(),
            &serving_runtime_drained,
            tasks,
        )
        .await;

        RuntimeRun {
            outcome,
            readiness,
            server_stopped: server_stopped.load(Ordering::SeqCst),
            drained_after_server: drained_after_server.load(Ordering::SeqCst),
        }
    }

    fn assert_drained_with_error_naming(run: &RuntimeRun, expected_task: &str) {
        assert!(
            matches!(
                &run.outcome,
                Err(LightwalletdConfigError::RuntimeTaskStopped { task })
                    if *task == expected_task
            ),
            "expected the runtime to fail naming {expected_task}"
        );
        assert!(matches!(
            run.readiness.report().cause,
            ReadinessCause::ShuttingDown
        ));
        assert!(run.server_stopped, "the server must stop");
        assert!(
            run.drained_after_server,
            "serving generations must outlive the server drain"
        );
    }

    #[tokio::test]
    async fn a_wallet_task_that_exits_early_drains_the_runtime() {
        for ended_task in WALLET_TASK_NAMES {
            let run = run_supervised(
                |cancel| wallet_tasks(cancel, ended_task, || tokio::spawn(async {})),
                false,
            )
            .await;

            assert_drained_with_error_naming(&run, ended_task);
        }
    }

    #[tokio::test]
    async fn a_compact_blocks_task_that_exits_early_drains_the_runtime() {
        for ended_task in COMPACT_BLOCKS_TASK_NAMES {
            let run = run_supervised(
                |cancel| compact_blocks_tasks(cancel, ended_task, || tokio::spawn(async {})),
                false,
            )
            .await;

            assert_drained_with_error_naming(&run, ended_task);
        }
    }

    #[tokio::test]
    async fn a_task_that_fails_to_join_is_reported_by_name() {
        for ended_task in WALLET_TASK_NAMES {
            let run = run_supervised(
                |cancel| {
                    wallet_tasks(cancel, ended_task, || {
                        let handle = tokio::spawn(future::pending::<()>());
                        handle.abort();
                        handle
                    })
                },
                false,
            )
            .await;

            assert!(
                matches!(
                    &run.outcome,
                    Err(LightwalletdConfigError::RuntimeTaskJoin { task, .. })
                        if *task == ended_task
                ),
                "expected a join failure naming {ended_task}"
            );
            assert!(matches!(
                run.readiness.report().cause,
                ReadinessCause::ShuttingDown
            ));
            assert!(run.server_stopped);
        }
    }

    #[tokio::test]
    async fn requested_shutdown_drains_every_task_cleanly() -> Result<(), LightwalletdConfigError> {
        let wallet_run = run_supervised(
            |cancel| wallet_tasks(cancel, "", || tokio::spawn(async {})),
            true,
        )
        .await;
        let compact_run = run_supervised(
            |cancel| compact_blocks_tasks(cancel, "", || tokio::spawn(async {})),
            true,
        )
        .await;

        for run in [wallet_run, compact_run] {
            run.outcome?;
            assert!(matches!(
                run.readiness.report().cause,
                ReadinessCause::ShuttingDown
            ));
            assert!(run.server_stopped);
            assert!(run.drained_after_server);
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_server_that_stops_on_its_own_is_a_runtime_failure() {
        let readiness = Readiness::new(ReadinessState::ready(Some(1)));
        let serving_readiness = WalletServingReadiness::pair_only(readiness.clone());
        let cancel = CancellationToken::new();
        let tasks = wallet_tasks(&cancel, "", || tokio::spawn(async {}));

        let outcome = supervise_compat_runtime(
            async { Ok(()) },
            &cancel,
            || serving_readiness.publish_shutting_down(),
            &CancellationToken::new(),
            tasks,
        )
        .await;

        assert!(matches!(
            outcome,
            Err(LightwalletdConfigError::GrpcServerStopped)
        ));
        assert!(matches!(
            readiness.report().cause,
            ReadinessCause::ShuttingDown
        ));
    }

    /// A server whose open calls never finish stops holding the process once
    /// the drain bound passes.
    #[tokio::test(start_paused = true)]
    async fn requested_shutdown_abandons_a_server_that_does_not_drain() {
        let readiness = Readiness::new(ReadinessState::ready(Some(1)));
        let serving_readiness = WalletServingReadiness::pair_only(readiness.clone());
        let cancel = CancellationToken::new();
        let tasks = wallet_tasks(&cancel, "", || tokio::spawn(async {}));
        cancel.cancel();
        let started_at = tokio::time::Instant::now();

        let outcome = supervise_compat_runtime(
            future::pending(),
            &cancel,
            || serving_readiness.publish_shutting_down(),
            &CancellationToken::new(),
            tasks,
        )
        .await;

        assert!(outcome.is_ok());
        assert_eq!(started_at.elapsed(), GRPC_SERVER_DRAIN_TIMEOUT);
    }
}
