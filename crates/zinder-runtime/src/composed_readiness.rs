//! Conjunctive composition of independently owned readiness inputs.
//!
//! A runtime whose readiness depends on more than one owner (a storage or
//! serving-pair admission plus probes of its dependencies) publishes each
//! owner's observation as a separate input here. One pure precedence function
//! projects the inputs onto the runtime's [`Readiness`], so no writer can erase
//! another writer's failure. Each runtime still decides which cause each of its
//! inputs reports; this module only composes them.

use std::{collections::BTreeMap, sync::Arc};

use parking_lot::Mutex;

use crate::readiness::{Readiness, ReadinessCause, ReadinessState};

/// Named readiness input composed beside a runtime's admission state.
///
/// Declaration order is the precedence among failing inputs: when several
/// inputs refuse traffic, the earliest declared one supplies the cause.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ReadinessInput {
    /// Liveness and upstream synchronization of the admitted node source.
    NodeSource,
    /// Health of the admitted authenticated ingest-control channel.
    IngestControl,
    /// Outcome of the latest materialized-view secondary catch-up.
    MaterializedViewCatchup,
}

/// Readiness owner that projects an admission state and named inputs onto one
/// [`Readiness`] handle.
///
/// The admission state is the runtime's storage or serving-pair readiness and
/// carries the reported heights. Precedence is fixed: shutdown first, then an
/// input reporting a canonical writer's `reorg_window_exceeded` halt, then a
/// refusing admission state, then refusing inputs in [`ReadinessInput`]
/// declaration order. Otherwise the admission state, including a warning that
/// still admits traffic, is the projection.
#[derive(Clone, Debug)]
pub struct ComposedReadiness {
    runtime: Readiness,
    composition: Arc<Mutex<Composition>>,
}

#[derive(Clone, Debug)]
struct Composition {
    is_shutting_down: bool,
    admission: ReadinessState,
    inputs: BTreeMap<ReadinessInput, ReadinessCause>,
}

impl ComposedReadiness {
    /// Starts with the admission state and every required input in `starting`.
    ///
    /// An input omitted from `required_inputs` does not gate readiness until
    /// its first publication.
    #[must_use]
    pub fn new(runtime: Readiness, required_inputs: &[ReadinessInput]) -> Self {
        let composed = Self {
            runtime,
            composition: Arc::new(Mutex::new(Composition {
                is_shutting_down: false,
                admission: ReadinessState::starting(),
                inputs: required_inputs
                    .iter()
                    .map(|input| (*input, ReadinessCause::Starting))
                    .collect(),
            })),
        };
        composed.publish_with(|_| {});
        composed
    }

    /// Returns the projected handle served by `/readyz` and gRPC traffic gates.
    #[must_use]
    pub fn runtime_readiness(&self) -> Readiness {
        self.runtime.clone()
    }

    /// Replaces the admission state.
    pub fn publish_admission(&self, admission: ReadinessState) {
        self.publish_with(|composition| composition.admission = admission);
    }

    /// Replaces one input's latest cause.
    pub fn publish_input(&self, input: ReadinessInput, cause: ReadinessCause) {
        self.publish_with(|composition| {
            composition.inputs.insert(input, cause);
        });
    }

    /// Irreversibly drains readiness before graceful task and server shutdown.
    pub fn publish_shutting_down(&self) {
        self.publish_with(|composition| composition.is_shutting_down = true);
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the composition lock must cover runtime publication so a stale projection cannot overtake a concurrent failure"
    )]
    fn publish_with(&self, update: impl FnOnce(&mut Composition)) {
        let mut composition = self.composition.lock();
        update(&mut composition);
        self.runtime.set(project(&composition));
    }
}

fn project(composition: &Composition) -> ReadinessState {
    let mut projected = composition.admission.clone();
    let overriding_cause = if composition.is_shutting_down {
        Some(ReadinessCause::ShuttingDown)
    } else if let Some(halt) = composition
        .inputs
        .values()
        .find(|cause| cause.reorg_window_exceeded_detail().is_some())
    {
        // A parked canonical writer is the root cause of the admission
        // failures that follow it, so its typed halt wins over them.
        Some(halt.clone())
    } else if !composition.admission.cause.permits_traffic() {
        None
    } else {
        composition
            .inputs
            .values()
            .find(|cause| !cause.permits_traffic())
            .cloned()
    };
    if let Some(cause) = overriding_cause {
        projected.cause = cause;
        projected.target_height = None;
    }
    projected
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use super::*;
    use crate::readiness::NodeUnavailableDetail;

    fn node_unavailable() -> ReadinessCause {
        ReadinessCause::NodeUnavailable(NodeUnavailableDetail::first_iteration(
            "node_unreachable",
            "test outage",
        ))
    }

    const fn writer_halt() -> ReadinessCause {
        ReadinessCause::ReorgWindowExceeded {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        }
    }

    const fn stale_pair() -> ReadinessCause {
        ReadinessCause::ServingPairStale {
            lag_chain_epochs: 2,
            staleness_seconds: 5,
        }
    }

    /// One precedence case: name, shutdown flag, admission cause, inputs, expected cause.
    type PrecedenceCase = (
        &'static str,
        bool,
        ReadinessCause,
        Vec<(ReadinessInput, ReadinessCause)>,
        ReadinessCause,
    );

    #[allow(
        clippy::too_many_lines,
        reason = "the precedence table reads as one auditable contract"
    )]
    fn precedence_cases() -> Vec<PrecedenceCase> {
        use ReadinessCause::{
            IngestControlUnavailable as IngestDown, Ready, SchemaMismatch, ShuttingDown, Starting,
            StorageUnavailable,
        };
        use ReadinessInput::{IngestControl as Ingest, MaterializedViewCatchup as Catchup};
        let node = ReadinessInput::NodeSource;
        vec![
            (
                "all ready",
                false,
                Ready,
                vec![(node, Ready), (Ingest, Ready)],
                Ready,
            ),
            ("no inputs", false, Ready, vec![], Ready),
            (
                "shutdown beats ready",
                true,
                Ready,
                vec![(node, Ready)],
                ShuttingDown,
            ),
            (
                "shutdown beats admission",
                true,
                SchemaMismatch,
                vec![],
                ShuttingDown,
            ),
            (
                "admission beats inputs",
                false,
                SchemaMismatch,
                vec![(Ingest, IngestDown)],
                SchemaMismatch,
            ),
            (
                "starting admission beats inputs",
                false,
                Starting,
                vec![(node, node_unavailable())],
                Starting,
            ),
            (
                "failing input beats ready",
                false,
                Ready,
                vec![(node, Ready), (Ingest, IngestDown)],
                IngestDown,
            ),
            (
                "failing input beats warning",
                false,
                stale_pair(),
                vec![(node, node_unavailable())],
                node_unavailable(),
            ),
            (
                "earliest declared input wins",
                false,
                Ready,
                vec![
                    (Catchup, StorageUnavailable),
                    (Ingest, IngestDown),
                    (node, Starting),
                ],
                Starting,
            ),
            (
                "later input fails alone",
                false,
                Ready,
                vec![(node, Ready), (Catchup, SchemaMismatch)],
                SchemaMismatch,
            ),
            (
                "warning survives ready inputs",
                false,
                stale_pair(),
                vec![(node, Ready)],
                stale_pair(),
            ),
            (
                "input writer halt beats refusing admission",
                false,
                ReadinessCause::WriterStatusUnavailable,
                vec![(node, node_unavailable()), (Ingest, writer_halt())],
                writer_halt(),
            ),
            (
                "shutdown beats input writer halt",
                true,
                SchemaMismatch,
                vec![(Ingest, writer_halt())],
                ShuttingDown,
            ),
        ]
    }

    #[test]
    fn projection_applies_fixed_precedence() {
        for (name, is_shutting_down, admission_cause, inputs, expected) in precedence_cases() {
            let mut admission = ReadinessState::ready_with_target(Some(10), Some(12));
            admission.cause = admission_cause;
            let composition = Composition {
                is_shutting_down,
                admission,
                inputs: inputs.into_iter().collect(),
            };
            assert_eq!(project(&composition).cause, expected, "case: {name}");
        }
    }

    #[test]
    fn projection_keeps_admission_heights_and_clears_target_on_override() {
        let ready_admission = ReadinessState::ready_with_target(Some(10), Some(12));
        let mut composition = Composition {
            is_shutting_down: false,
            admission: ready_admission.clone(),
            inputs: BTreeMap::from([(ReadinessInput::NodeSource, ReadinessCause::Ready)]),
        };
        assert_eq!(project(&composition), ready_admission);

        composition
            .inputs
            .insert(ReadinessInput::NodeSource, node_unavailable());
        let projected = project(&composition);
        assert_eq!(projected.current_height, Some(10));
        assert_eq!(projected.target_height, None);

        composition.admission = ReadinessState::syncing(Some(2), Some(10), Some(12));
        assert_eq!(project(&composition), composition.admission);

        composition.is_shutting_down = true;
        let projected = project(&composition);
        assert_eq!(projected.cause, ReadinessCause::ShuttingDown);
        assert_eq!(projected.current_height, Some(10));
        assert_eq!(projected.target_height, None);
    }

    #[test]
    fn required_inputs_gate_until_published_and_shutdown_is_irreversible() {
        let runtime = Readiness::default();
        let composed = ComposedReadiness::new(runtime.clone(), &[ReadinessInput::NodeSource]);
        composed.publish_admission(ReadinessState::ready(Some(5)));
        assert_eq!(runtime.report().cause, ReadinessCause::Starting);

        composed.publish_input(ReadinessInput::NodeSource, ReadinessCause::Ready);
        assert!(runtime.report().is_ready);

        composed.publish_input(
            ReadinessInput::MaterializedViewCatchup,
            ReadinessCause::StorageUnavailable,
        );
        assert_eq!(runtime.report().cause, ReadinessCause::StorageUnavailable);
        composed.publish_input(
            ReadinessInput::MaterializedViewCatchup,
            ReadinessCause::Ready,
        );
        assert!(runtime.report().is_ready);

        composed.publish_shutting_down();
        composed.publish_admission(ReadinessState::ready(Some(6)));
        composed.publish_input(ReadinessInput::NodeSource, ReadinessCause::Ready);
        assert_eq!(runtime.report().cause, ReadinessCause::ShuttingDown);
    }

    #[test]
    fn concurrent_recovery_cannot_erase_another_input_failure() {
        const ITERATIONS: usize = 1_024;

        let runtime = Readiness::default();
        let composed = ComposedReadiness::new(
            runtime.clone(),
            &[ReadinessInput::NodeSource, ReadinessInput::IngestControl],
        );
        composed.publish_admission(ReadinessState::ready(Some(42)));
        let iteration_start = Barrier::new(3);
        let iteration_complete = Barrier::new(3);

        std::thread::scope(|scope| {
            scope.spawn(|| {
                for _ in 0..ITERATIONS {
                    iteration_start.wait();
                    composed.publish_input(ReadinessInput::NodeSource, ReadinessCause::Ready);
                    iteration_complete.wait();
                }
            });
            scope.spawn(|| {
                for _ in 0..ITERATIONS {
                    iteration_start.wait();
                    composed.publish_input(
                        ReadinessInput::IngestControl,
                        ReadinessCause::IngestControlUnavailable,
                    );
                    iteration_complete.wait();
                }
            });

            for iteration in 0..ITERATIONS {
                composed.publish_input(ReadinessInput::NodeSource, node_unavailable());
                composed.publish_input(ReadinessInput::IngestControl, ReadinessCause::Ready);
                iteration_start.wait();
                iteration_complete.wait();
                assert_eq!(
                    runtime.report().cause,
                    ReadinessCause::IngestControlUnavailable,
                    "concurrent node recovery erased ingest failure at iteration {iteration}"
                );
            }
        });
    }
}
