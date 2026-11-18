//! Composition of independently owned readiness inputs.
//!
//! A runtime whose readiness depends on more than one owner (a storage,
//! serving-pair, or canonical-writer admission plus probes of its
//! dependencies) publishes each
//! owner's observation as a separate input here. One pure precedence function
//! projects the inputs onto the runtime's [`Readiness`], so no writer can erase
//! another writer's failure. Each runtime still decides which cause each of its
//! inputs reports, and whether an input gates traffic or only reports a
//! dependency the runtime serves without; this module only composes them.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use parking_lot::Mutex;
use serde::Serialize;

use crate::readiness::{Readiness, ReadinessCause, ReadinessDependency, ReadinessState};

/// Named readiness input composed beside a runtime's admission state.
///
/// Declaration order is the precedence among failing inputs: when several
/// inputs refuse traffic, the earliest declared one supplies the cause. It is
/// also the order of a report's degraded dependencies.
///
/// Wire shape: the snake-case [`Self::wire_label`] on `/readyz` JSON, the
/// proto report, and the `input` label of
/// `zinder_readiness_dependency_state`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ReadinessInput {
    /// Liveness and upstream synchronization of the admitted node source.
    NodeSource,
    /// Health of the admitted authenticated ingest-control channel.
    IngestControl,
    /// Outcome of the latest materialized-view secondary catch-up.
    MaterializedViewCatchup,
}

impl ReadinessInput {
    /// Stable snake-case label used on `/readyz` JSON, the proto report, and
    /// metric label sets.
    #[must_use]
    pub const fn wire_label(self) -> &'static str {
        match self {
            Self::NodeSource => "node_source",
            Self::IngestControl => "ingest_control",
            Self::MaterializedViewCatchup => "materialized_view_catchup",
        }
    }
}

/// Readiness owner that projects an admission state and named inputs onto one
/// [`Readiness`] handle.
///
/// The admission state is the runtime's storage, serving-pair, or
/// canonical-writer readiness and carries the reported heights. An input is
/// either required, so its refusing cause closes the traffic gate, or a
/// dependency, declared with [`Self::with_dependency_inputs`], whose refusing
/// cause is reported as degraded while the runtime keeps serving.
///
/// Precedence is fixed: shutdown first, then any input, required or
/// dependency, reporting a canonical writer's `reorg_window_exceeded` halt,
/// then a refusing admission state, then refusing required inputs in
/// [`ReadinessInput`] declaration order. Otherwise the admission state,
/// including a warning that still admits traffic, is the projection.
#[derive(Clone, Debug)]
pub struct ComposedReadiness {
    runtime: Readiness,
    composition: Arc<Mutex<Composition>>,
}

#[derive(Clone, Debug)]
struct Composition {
    is_shutting_down: bool,
    is_parked: bool,
    admission: ReadinessState,
    inputs: BTreeMap<ReadinessInput, ReadinessCause>,
    dependency_inputs: BTreeSet<ReadinessInput>,
}

impl ComposedReadiness {
    /// Starts with the admission state and every required input in `starting`.
    ///
    /// An input omitted from `required_inputs` does not gate readiness until
    /// its first publication.
    #[must_use]
    pub fn new(runtime: Readiness, required_inputs: &[ReadinessInput]) -> Self {
        Self::compose(runtime, required_inputs, BTreeSet::new())
    }

    /// Starts with the admission state and every dependency input in
    /// `starting`.
    ///
    /// A dependency input reports something the runtime can serve without,
    /// such as the node behind a few node-backed methods. Its refusing cause
    /// never closes the traffic gate: the report lists it under
    /// [`ReadinessReport::degraded`](crate::ReadinessReport::degraded)
    /// instead. A canonical writer's `reorg_window_exceeded` halt is the
    /// exception, because it is the cause of the admission failures that
    /// follow it, and it refuses traffic from any input.
    #[must_use]
    pub fn with_dependency_inputs(
        runtime: Readiness,
        dependency_inputs: &[ReadinessInput],
    ) -> Self {
        Self::compose(
            runtime,
            dependency_inputs,
            dependency_inputs.iter().copied().collect(),
        )
    }

    fn compose(
        runtime: Readiness,
        starting_inputs: &[ReadinessInput],
        dependency_inputs: BTreeSet<ReadinessInput>,
    ) -> Self {
        let composed = Self {
            runtime,
            composition: Arc::new(Mutex::new(Composition {
                is_shutting_down: false,
                is_parked: false,
                admission: ReadinessState::starting(),
                inputs: starting_inputs
                    .iter()
                    .map(|input| (*input, ReadinessCause::Starting))
                    .collect(),
                dependency_inputs,
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

    /// Replaces the admission state with the rule [`Readiness::set`] applies.
    ///
    /// The admission keeps its last ingest phase when `admission` carries
    /// none, and keeps its observed target across a warning that does not
    /// observe one.
    ///
    /// A [parked](Self::park_admission) admission ignores the write.
    pub fn publish_admission(&self, admission: ReadinessState) {
        self.publish_with(|composition| {
            if !composition.is_parked {
                composition.admission = admission.replacing(&composition.admission);
            }
        });
    }

    /// Replaces the admission state with a terminal state that later admission
    /// writes cannot replace until the process restarts.
    ///
    /// A canonical writer parked on a reorg deeper than its window keeps
    /// serving its control plane while other workers still run; none of them
    /// may replace the halt with a recoverable cause, and the ingest phase it
    /// was parked in stays fixed. Inputs keep recording their causes, but the
    /// parked admission refuses traffic and so supplies the projection. Only
    /// shutdown replaces it, and a shutdown already in progress keeps its cause.
    pub fn park_admission(&self, admission: ReadinessState) {
        self.publish_with(|composition| {
            let phase = admission.phase.or(composition.admission.phase);
            composition.admission = admission;
            composition.admission.phase = phase;
            composition.is_parked = true;
        });
    }

    /// Atomically mutates the admission state.
    ///
    /// Use when an admission write depends on fields it does not change, such
    /// as a traffic-admitting warning that a second admission owner raises
    /// and the primary owner must keep.
    ///
    /// A [parked](Self::park_admission) admission ignores the update.
    pub fn update_admission(&self, update: impl FnOnce(&mut ReadinessState)) {
        self.publish_with(|composition| {
            if !composition.is_parked {
                update(&mut composition.admission);
            }
        });
    }

    /// Returns one input's latest published cause, whether or not it currently
    /// supplies the projected cause.
    ///
    /// A refusing admission state or shutdown hides every required input from
    /// [`Self::runtime_readiness`]; this accessor still reports the input, so
    /// a status surface can expose a dependency observation independently of
    /// the admission state.
    #[must_use]
    pub fn input_cause(&self, input: ReadinessInput) -> Option<ReadinessCause> {
        self.composition.lock().inputs.get(&input).cloned()
    }

    /// Returns the target height of the admission state.
    ///
    /// A refusing required input clears the target from
    /// [`Self::runtime_readiness`]; this accessor still reports the one the
    /// admission state last observed, such as the node tip a canonical
    /// writer follows while its node-health probe reports not ready.
    #[must_use]
    pub fn admission_target_height(&self) -> Option<u32> {
        self.composition.lock().admission.target_height
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
        let projected = project(&composition);
        self.runtime
            .publish_composition(projected, dependencies(&composition));
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
            .iter()
            .find(|(input, cause)| {
                !composition.dependency_inputs.contains(input) && !cause.permits_traffic()
            })
            .map(|(_, cause)| cause.clone())
    };
    if let Some(cause) = overriding_cause {
        projected.cause = cause;
        projected.target_height = None;
    }
    projected
}

/// Returns every dependency input's latest cause in declaration order.
fn dependencies(composition: &Composition) -> Vec<ReadinessDependency> {
    composition
        .inputs
        .iter()
        .filter(|(input, _)| composition.dependency_inputs.contains(input))
        .map(|(input, cause)| ReadinessDependency {
            input: *input,
            cause: cause.clone(),
        })
        .collect()
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
                is_parked: false,
                admission,
                inputs: inputs.into_iter().collect(),
                dependency_inputs: BTreeSet::new(),
            };
            assert_eq!(project(&composition).cause, expected, "case: {name}");
        }
    }

    /// One dependency case: name, shutdown flag, admission cause, inputs,
    /// expected cause, expected degraded inputs. Node source and ingest
    /// control are dependencies; materialized-view catch-up stays required.
    type DependencyCase = (
        &'static str,
        bool,
        ReadinessCause,
        Vec<(ReadinessInput, ReadinessCause)>,
        ReadinessCause,
        Vec<ReadinessInput>,
    );

    #[allow(
        clippy::too_many_lines,
        reason = "the dependency precedence table reads as one auditable contract"
    )]
    fn dependency_cases() -> Vec<DependencyCase> {
        use ReadinessCause::{
            IngestControlUnavailable as IngestDown, Ready, SchemaMismatch, ShuttingDown, Starting,
            StorageUnavailable,
        };
        use ReadinessInput::{IngestControl as Ingest, MaterializedViewCatchup as Catchup};
        let node = ReadinessInput::NodeSource;
        vec![
            (
                "healthy dependencies are not degraded",
                false,
                Ready,
                vec![(node, Ready), (Ingest, Ready)],
                Ready,
                vec![],
            ),
            (
                "failing dependency leaves ready admitting",
                false,
                Ready,
                vec![(node, node_unavailable()), (Ingest, Ready)],
                Ready,
                vec![node],
            ),
            (
                "failing dependency leaves a warning admitting",
                false,
                stale_pair(),
                vec![(node, Starting), (Ingest, IngestDown)],
                stale_pair(),
                vec![node, Ingest],
            ),
            (
                "refusing admission keeps dependencies reported",
                false,
                SchemaMismatch,
                vec![(node, node_unavailable())],
                SchemaMismatch,
                vec![node],
            ),
            (
                "dependency writer halt still refuses",
                false,
                Ready,
                vec![(node, Ready), (Ingest, writer_halt())],
                writer_halt(),
                vec![],
            ),
            (
                "dependency writer halt beats refusing admission",
                false,
                ReadinessCause::WriterStatusUnavailable,
                vec![(node, node_unavailable()), (Ingest, writer_halt())],
                writer_halt(),
                vec![node],
            ),
            (
                "required input gates beside dependencies",
                false,
                Ready,
                vec![(node, node_unavailable()), (Catchup, StorageUnavailable)],
                StorageUnavailable,
                vec![node],
            ),
            (
                "shutdown keeps dependencies reported",
                true,
                Ready,
                vec![(node, Starting)],
                ShuttingDown,
                vec![node],
            ),
        ]
    }

    #[test]
    fn dependencies_report_degradation_without_refusing_traffic() {
        for (name, is_shutting_down, admission_cause, inputs, expected, expected_degraded) in
            dependency_cases()
        {
            let mut admission = ReadinessState::ready(Some(10));
            admission.cause = admission_cause;
            let composition = Composition {
                is_shutting_down,
                is_parked: false,
                admission,
                inputs: inputs.into_iter().collect(),
                dependency_inputs: BTreeSet::from([
                    ReadinessInput::NodeSource,
                    ReadinessInput::IngestControl,
                ]),
            };
            assert_eq!(project(&composition).cause, expected, "case: {name}");
            let degraded = dependencies(&composition)
                .into_iter()
                .filter(ReadinessDependency::is_degraded)
                .map(|dependency| dependency.input)
                .collect::<Vec<_>>();
            assert_eq!(degraded, expected_degraded, "case: {name}");
        }
    }

    #[test]
    fn dependency_inputs_start_degraded_and_never_close_the_traffic_gate()
    -> Result<(), tonic::Status> {
        use tonic::service::Interceptor as _;

        let runtime = Readiness::default();
        let composed = ComposedReadiness::with_dependency_inputs(
            runtime.clone(),
            &[ReadinessInput::NodeSource, ReadinessInput::IngestControl],
        );
        let mut traffic_gate = crate::TrafficReadinessInterceptor::new(runtime.clone());
        let starting = runtime.report();
        assert_eq!(starting.cause, ReadinessCause::Starting);
        assert_eq!(starting.degraded.len(), 2);

        composed.publish_admission(ReadinessState::ready(Some(5)));
        let report = runtime.report();
        assert!(report.is_ready);
        assert_eq!(
            report.degraded,
            vec![
                ReadinessDependency {
                    input: ReadinessInput::NodeSource,
                    cause: ReadinessCause::Starting,
                },
                ReadinessDependency {
                    input: ReadinessInput::IngestControl,
                    cause: ReadinessCause::Starting,
                },
            ]
        );
        traffic_gate.call(tonic::Request::new(()))?;

        composed.publish_input(ReadinessInput::NodeSource, node_unavailable());
        composed.publish_input(
            ReadinessInput::IngestControl,
            ReadinessCause::IngestControlUnavailable,
        );
        let report = runtime.report();
        assert!(report.is_ready);
        assert_eq!(report.current_height, Some(5));
        assert_eq!(report.target_height, Some(5));
        assert_eq!(report.degraded.len(), 2);
        traffic_gate.call(tonic::Request::new(()))?;

        composed.publish_input(ReadinessInput::NodeSource, ReadinessCause::Ready);
        assert_eq!(
            runtime.report().degraded,
            vec![ReadinessDependency {
                input: ReadinessInput::IngestControl,
                cause: ReadinessCause::IngestControlUnavailable,
            }]
        );
        composed.publish_input(ReadinessInput::IngestControl, ReadinessCause::Ready);
        assert!(runtime.report().degraded.is_empty());
        assert_eq!(
            composed.input_cause(ReadinessInput::IngestControl),
            Some(ReadinessCause::Ready)
        );

        composed.publish_input(ReadinessInput::IngestControl, writer_halt());
        let halted = runtime.report();
        assert!(!halted.is_ready);
        assert_eq!(halted.cause, writer_halt());
        assert!(halted.degraded.is_empty());
        Ok(())
    }

    #[test]
    fn projection_keeps_admission_heights_and_clears_target_on_override() {
        let ready_admission = ReadinessState::ready_with_target(Some(10), Some(12));
        let mut composition = Composition {
            is_shutting_down: false,
            is_parked: false,
            admission: ready_admission.clone(),
            inputs: BTreeMap::from([(ReadinessInput::NodeSource, ReadinessCause::Ready)]),
            dependency_inputs: BTreeSet::new(),
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
    fn input_cause_reports_an_input_hidden_by_a_refusing_admission() {
        let runtime = Readiness::default();
        let composed = ComposedReadiness::new(runtime.clone(), &[ReadinessInput::IngestControl]);
        assert_eq!(
            composed.input_cause(ReadinessInput::IngestControl),
            Some(ReadinessCause::Starting)
        );
        assert_eq!(composed.input_cause(ReadinessInput::NodeSource), None);

        composed.publish_admission(ReadinessState::syncing(Some(2), Some(10), Some(12)));
        composed.publish_input(ReadinessInput::NodeSource, node_unavailable());
        assert!(matches!(
            runtime.report().cause,
            ReadinessCause::Syncing { .. }
        ));
        assert_eq!(
            composed.input_cause(ReadinessInput::NodeSource),
            Some(node_unavailable())
        );
    }

    #[test]
    fn admission_writes_keep_phase_and_warning_target_through_input_overrides() {
        let runtime = Readiness::default();
        let composed = ComposedReadiness::new(runtime.clone(), &[]);
        composed.publish_admission(
            ReadinessState::ready_with_target(Some(90), Some(100))
                .with_phase(crate::IngestPhase::FollowingTip),
        );
        composed.update_admission(|admission| {
            admission.cause = ReadinessCause::CursorAtRisk {
                oldest_retained_age_hours: 25,
                retention_hours: 168,
            };
        });
        composed.publish_input(ReadinessInput::NodeSource, node_unavailable());
        assert_eq!(runtime.report().target_height, None);

        composed.publish_input(ReadinessInput::NodeSource, ReadinessCause::Ready);
        let report = runtime.report();
        assert!(matches!(report.cause, ReadinessCause::CursorAtRisk { .. }));
        assert_eq!(report.target_height, Some(100));
        assert_eq!(report.phase, Some(crate::IngestPhase::FollowingTip));

        composed.publish_admission(ReadinessState::syncing(Some(3), Some(97), Some(100)));
        let report = runtime.report();
        assert!(matches!(report.cause, ReadinessCause::Syncing { .. }));
        assert_eq!(report.phase, Some(crate::IngestPhase::FollowingTip));
    }

    #[test]
    fn parked_admission_keeps_its_halt_and_phase_until_shutdown() {
        let runtime = Readiness::default();
        let composed = ComposedReadiness::new(runtime.clone(), &[]);
        composed.publish_admission(ReadinessState::ready(Some(10)));
        composed.park_admission(
            ReadinessState::reorg_window_exceeded(1_001, 1_000, Some(1_000), Some(10))
                .with_phase(crate::IngestPhase::FollowingTip),
        );

        composed.publish_admission(ReadinessState::ready(Some(11)));
        composed.update_admission(|admission| {
            admission.cause = ReadinessCause::Ready;
            admission.phase = Some(crate::IngestPhase::BulkCatchup);
        });
        composed.publish_input(ReadinessInput::NodeSource, node_unavailable());
        composed.publish_input(ReadinessInput::NodeSource, ReadinessCause::Ready);
        let report = runtime.report();
        assert!(!report.is_ready);
        assert!(matches!(
            report.cause,
            ReadinessCause::ReorgWindowExceeded {
                depth: 1_001,
                configured: 1_000,
                ..
            }
        ));
        assert_eq!(report.current_height, Some(10));
        assert_eq!(report.phase, Some(crate::IngestPhase::FollowingTip));

        composed.publish_shutting_down();
        assert_eq!(runtime.report().cause, ReadinessCause::ShuttingDown);
    }

    #[test]
    fn parking_during_shutdown_keeps_the_shutdown_cause() {
        let runtime = Readiness::default();
        let composed = ComposedReadiness::new(runtime.clone(), &[]);
        composed.publish_admission(ReadinessState::ready(Some(10)));
        composed.publish_shutting_down();
        composed.park_admission(ReadinessState::reorg_window_exceeded(
            1_001,
            1_000,
            Some(1_000),
            Some(10),
        ));
        assert_eq!(runtime.report().cause, ReadinessCause::ShuttingDown);
        composed.publish_admission(ReadinessState::ready(Some(11)));
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

    #[test]
    fn concurrent_recovery_cannot_erase_another_dependency_failure() {
        const ITERATIONS: usize = 1_024;

        let runtime = Readiness::default();
        let composed = ComposedReadiness::with_dependency_inputs(
            runtime.clone(),
            &[ReadinessInput::NodeSource, ReadinessInput::IngestControl],
        );
        composed.publish_admission(ReadinessState::ready(Some(42)));
        let iteration_start = Barrier::new(3);
        let iteration_complete = Barrier::new(3);
        let ingest_down = ReadinessDependency {
            input: ReadinessInput::IngestControl,
            cause: ReadinessCause::IngestControlUnavailable,
        };

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
                let report = runtime.report();
                assert!(report.is_ready);
                assert_eq!(
                    report.degraded,
                    vec![ingest_down.clone()],
                    "concurrent node recovery erased ingest degradation at iteration {iteration}"
                );
            }
        });
    }
}
