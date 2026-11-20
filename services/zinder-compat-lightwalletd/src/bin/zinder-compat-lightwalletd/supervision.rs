//! The background tasks each compatibility serving mode supervises next to
//! its gRPC server.

use tokio::task::JoinHandle;
use zinder_query::WalletServingPairTeardownError;
use zinder_runtime::{OpsEndpointHandle, SupervisedTasks};

use crate::compact_block_serving::CompactBlockServingError;

/// The background tasks of wallet serving.
pub(crate) struct WalletServingTasks {
    pub(crate) serving_pair_publisher: JoinHandle<Result<(), WalletServingPairTeardownError>>,
    pub(crate) node_readiness_probe: JoinHandle<()>,
    pub(crate) ingest_control_readiness_probe: JoinHandle<()>,
    pub(crate) tip_change_publisher: JoinHandle<()>,
    pub(crate) node_tip_witness: JoinHandle<()>,
    pub(crate) operations: Option<OpsEndpointHandle>,
}

impl WalletServingTasks {
    pub(crate) fn supervised(self) -> SupervisedTasks {
        SupervisedTasks::new()
            .with_fallible("serving-pair publisher", self.serving_pair_publisher)
            .with("node-readiness probe", self.node_readiness_probe)
            .with(
                "ingest-control readiness probe",
                self.ingest_control_readiness_probe,
            )
            .with("tip-change publisher", self.tip_change_publisher)
            .with("node-tip witness", self.node_tip_witness)
            .with_operations_endpoint(self.operations)
    }
}

/// The background tasks of compact-blocks serving.
pub(crate) struct CompactBlocksServingTasks {
    pub(crate) compact_block_publisher: JoinHandle<Result<(), CompactBlockServingError>>,
    pub(crate) node_tip_witness: JoinHandle<()>,
    pub(crate) operations: Option<OpsEndpointHandle>,
}

impl CompactBlocksServingTasks {
    pub(crate) fn supervised(self) -> SupervisedTasks {
        SupervisedTasks::new()
            .with_fallible("compact-block publisher", self.compact_block_publisher)
            .with("node-tip witness", self.node_tip_witness)
            .with_operations_endpoint(self.operations)
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use parking_lot::Mutex;
    use zinder_query::WalletServingReadiness;
    use zinder_runtime::{
        Readiness, ReadinessCause, RuntimeService, RuntimeSupervisionError, SupervisedRuntime,
        spawn_ops_endpoint_for,
    };

    use super::*;
    use crate::compact_block_serving::CompactServingReadiness;

    /// Spawns `task_name` as a task that has already ended when it is
    /// `ended_task`, or as one that runs until the runtime stops.
    fn task_for(task_name: &str, ended_task: &str, runtime: &SupervisedRuntime) -> JoinHandle<()> {
        if task_name == ended_task {
            tokio::spawn(async {})
        } else {
            let stopping = runtime.stopping();
            tokio::spawn(async move { stopping.cancelled().await })
        }
    }

    /// [`task_for`] for a publisher, whose task returns its teardown outcome.
    fn publisher_for<E: Send + 'static>(
        task_name: &str,
        ended_task: &str,
        runtime: &SupervisedRuntime,
    ) -> JoinHandle<Result<(), E>> {
        if task_name == ended_task {
            tokio::spawn(async { Ok(()) })
        } else {
            let stopping = runtime.stopping();
            tokio::spawn(async move {
                stopping.cancelled().await;
                Ok(())
            })
        }
    }

    /// Ends `ended_task` early and checks how the runtime stops.
    ///
    /// Readiness reports `shutting_down` before the server drains and after
    /// the runtime stops, the server stops, and the runtime fails naming the
    /// task.
    async fn run_with_ended_task(
        ended_task: &str,
        readiness: &Readiness,
        publish_shutting_down: impl FnOnce(),
        make_tasks: impl FnOnce(&SupervisedRuntime) -> SupervisedTasks,
    ) {
        let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd);
        let tasks = make_tasks(&runtime);
        let readiness_at_drain = Arc::new(Mutex::new(None));
        let server = {
            let shutdown = runtime.grpc_server_shutdown();
            let readiness = readiness.clone();
            let readiness_at_drain = Arc::clone(&readiness_at_drain);
            async move {
                shutdown.await;
                *readiness_at_drain.lock() = Some(readiness.report());
                Ok(())
            }
        };

        let outcome = runtime
            .supervise(server, tasks, publish_shutting_down)
            .await;

        assert!(
            matches!(
                &outcome,
                Err(RuntimeSupervisionError::TaskStopped { task }) if *task == ended_task
            ),
            "expected the runtime to fail naming {ended_task}, got {outcome:?}"
        );
        let at_drain = readiness_at_drain.lock().take();
        assert!(
            at_drain
                .is_some_and(|report| !report.is_ready
                    && matches!(report.cause, ReadinessCause::ShuttingDown)),
            "readiness must report shutting_down once {ended_task} ends and before the server drains"
        );
        let after_stop = readiness.report();
        assert!(
            !after_stop.is_ready && matches!(after_stop.cause, ReadinessCause::ShuttingDown),
            "readiness must still report shutting_down after the runtime stops, got {:?}",
            after_stop.cause
        );
    }

    #[tokio::test]
    async fn every_wallet_serving_task_is_supervised_by_name() {
        for ended_task in [
            "serving-pair publisher",
            "node-readiness probe",
            "ingest-control readiness probe",
            "tip-change publisher",
            "node-tip witness",
        ] {
            let readiness = Readiness::default();
            let serving_readiness =
                WalletServingReadiness::with_node_and_ingest_control(readiness.clone());
            run_with_ended_task(
                ended_task,
                &readiness,
                || serving_readiness.publish_shutting_down(),
                |runtime| {
                    WalletServingTasks {
                        serving_pair_publisher: publisher_for(
                            "serving-pair publisher",
                            ended_task,
                            runtime,
                        ),
                        node_readiness_probe: task_for("node-readiness probe", ended_task, runtime),
                        ingest_control_readiness_probe: task_for(
                            "ingest-control readiness probe",
                            ended_task,
                            runtime,
                        ),
                        tip_change_publisher: task_for("tip-change publisher", ended_task, runtime),
                        node_tip_witness: task_for("node-tip witness", ended_task, runtime),
                        operations: None,
                    }
                    .supervised()
                },
            )
            .await;
        }
    }

    #[tokio::test]
    async fn every_compact_blocks_serving_task_is_supervised_by_name() {
        for ended_task in ["compact-block publisher", "node-tip witness"] {
            let serving_readiness = CompactServingReadiness::new(Readiness::default());
            let readiness = serving_readiness.runtime();
            run_with_ended_task(
                ended_task,
                &readiness,
                || serving_readiness.publish_shutting_down(),
                |runtime| {
                    CompactBlocksServingTasks {
                        compact_block_publisher: publisher_for(
                            "compact-block publisher",
                            ended_task,
                            runtime,
                        ),
                        node_tip_witness: task_for("node-tip witness", ended_task, runtime),
                        operations: None,
                    }
                    .supervised()
                },
            )
            .await;
        }
    }

    /// Both modes watch their operational endpoint with the other tasks, so
    /// an endpoint that fails stops the runtime instead of leaving wallet
    /// traffic served without `/readyz`.
    #[tokio::test]
    async fn both_serving_modes_supervise_their_operations_endpoint()
    -> Result<(), Box<dyn std::error::Error>> {
        let loopback: SocketAddr = "127.0.0.1:0".parse()?;
        let operations_endpoint = || {
            spawn_ops_endpoint_for(
                RuntimeService::CompatLightwalletd,
                Some(loopback),
                "test",
                "zcash-regtest",
                Readiness::default(),
                Arc::from([]),
            )
        };
        let wallet_tasks = WalletServingTasks {
            serving_pair_publisher: tokio::spawn(async { Ok(()) }),
            node_readiness_probe: tokio::spawn(async {}),
            ingest_control_readiness_probe: tokio::spawn(async {}),
            tip_change_publisher: tokio::spawn(async {}),
            node_tip_witness: tokio::spawn(async {}),
            operations: operations_endpoint().await?,
        }
        .supervised();
        let compact_blocks_tasks = CompactBlocksServingTasks {
            compact_block_publisher: tokio::spawn(async { Ok(()) }),
            node_tip_witness: tokio::spawn(async {}),
            operations: operations_endpoint().await?,
        }
        .supervised();

        for tasks in [wallet_tasks, compact_blocks_tasks] {
            let names = format!("{tasks:?}");
            assert!(
                names.contains("\"operations endpoint\""),
                "the operations endpoint is not supervised: {names}"
            );
        }
        Ok(())
    }
}
