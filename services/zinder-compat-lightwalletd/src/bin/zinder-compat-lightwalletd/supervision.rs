//! The background tasks each compatibility serving mode supervises next to
//! its gRPC server.

use tokio::task::JoinHandle;
use zinder_runtime::SupervisedTasks;

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
        SupervisedTasks::new()
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
        SupervisedTasks::new()
            .with("compact-block publisher", self.compact_block_publisher)
            .with("node-tip witness", self.node_tip_witness)
    }
}

#[cfg(test)]
mod tests {
    use zinder_runtime::{RuntimeService, RuntimeSupervisionError, SupervisedRuntime};

    use super::*;

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

    async fn run_with_ended_task(
        ended_task: &str,
        make_tasks: impl FnOnce(&SupervisedRuntime) -> SupervisedTasks,
    ) {
        let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd);
        let tasks = make_tasks(&runtime);
        let server = {
            let shutdown = runtime.grpc_server_shutdown();
            async move {
                shutdown.await;
                Ok(())
            }
        };
        let outcome = runtime.supervise(server, tasks, || {}).await;
        assert!(
            matches!(
                &outcome,
                Err(RuntimeSupervisionError::TaskStopped { task }) if *task == ended_task
            ),
            "expected the runtime to fail naming {ended_task}, got {outcome:?}"
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
            run_with_ended_task(ended_task, |runtime| {
                WalletServingTasks {
                    serving_pair_publisher: task_for("serving-pair publisher", ended_task, runtime),
                    node_readiness_probe: task_for("node-readiness probe", ended_task, runtime),
                    ingest_control_readiness_probe: task_for(
                        "ingest-control readiness probe",
                        ended_task,
                        runtime,
                    ),
                    tip_change_publisher: task_for("tip-change publisher", ended_task, runtime),
                    node_tip_witness: task_for("node-tip witness", ended_task, runtime),
                }
                .supervised()
            })
            .await;
        }
    }

    #[tokio::test]
    async fn every_compact_blocks_serving_task_is_supervised_by_name() {
        for ended_task in ["compact-block publisher", "node-tip witness"] {
            run_with_ended_task(ended_task, |runtime| {
                CompactBlocksServingTasks {
                    compact_block_publisher: task_for(
                        "compact-block publisher",
                        ended_task,
                        runtime,
                    ),
                    node_tip_witness: task_for("node-tip witness", ended_task, runtime),
                }
                .supervised()
            })
            .await;
        }
    }
}
