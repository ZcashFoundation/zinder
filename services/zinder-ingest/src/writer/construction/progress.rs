//! Fresh canonical construction progress gauges and `/readyz` lag updates.

use std::time::{Duration, Instant};

use zinder_core::BlockHeight;
use zinder_runtime::{ComposedReadiness, IngestPhase, ReadinessState};
use zinder_store::CanonicalStoreBuildPlan;

const CONSTRUCTION_LOADED_HEIGHT_METRIC: &str = "zinder_ingest_construction_loaded_height";
const CONSTRUCTION_TARGET_HEIGHT_METRIC: &str = "zinder_ingest_construction_target_height";
const PROGRESS_INTERVAL_BLOCKS: u32 = 256;
const PROGRESS_INTERVAL_DURATION: Duration = Duration::from_millis(250);

/// Registers `# HELP` and `# TYPE` text for the fresh construction progress gauges.
fn describe_metrics() {
    metrics::describe_gauge!(
        CONSTRUCTION_LOADED_HEIGHT_METRIC,
        metrics::Unit::Count,
        "Highest block height staged into the canonical store builder during fresh construction."
    );
    metrics::describe_gauge!(
        CONSTRUCTION_TARGET_HEIGHT_METRIC,
        metrics::Unit::Count,
        "Fixed target tip height of the fresh canonical store construction."
    );
}

/// Height range spanned by a single fresh canonical build.
#[derive(Clone, Copy, Debug)]
struct ProgressBounds {
    initial_height: BlockHeight,
    target_height: BlockHeight,
}

/// Minimum block or elapsed-time interval between throttled progress updates.
#[derive(Clone, Copy, Debug)]
struct ProgressCadence {
    max_blocks: u32,
    max_elapsed: Duration,
}

/// Publishes fresh canonical construction progress to Prometheus gauges and `/readyz`.
pub(crate) struct ConstructionProgress {
    target_height: u32,
    loaded_height: u32,
    last_published_height: u32,
    last_published_at: Instant,
    cadence: ProgressCadence,
    loaded_height_gauge: metrics::Gauge,
    readiness: Option<ComposedReadiness>,
}

impl ConstructionProgress {
    /// Initializes construction gauges and publishes the initial `bulk_catchup` lag.
    pub(crate) fn start(
        build_plan: &CanonicalStoreBuildPlan,
        readiness: Option<ComposedReadiness>,
    ) -> Self {
        Self::start_at(
            ProgressBounds {
                initial_height: build_plan.history_predecessor().block_id.height,
                target_height: build_plan.build_tip().height,
            },
            readiness,
            ProgressCadence {
                max_blocks: PROGRESS_INTERVAL_BLOCKS,
                max_elapsed: PROGRESS_INTERVAL_DURATION,
            },
            Instant::now(),
        )
    }

    fn start_at(
        bounds: ProgressBounds,
        readiness: Option<ComposedReadiness>,
        cadence: ProgressCadence,
        now: Instant,
    ) -> Self {
        describe_metrics();
        let target_height = bounds.target_height.value();
        let loaded_height = bounds.initial_height.value().min(target_height);
        metrics::gauge!(CONSTRUCTION_TARGET_HEIGHT_METRIC).set(f64::from(target_height));
        let loaded_height_gauge = metrics::gauge!(CONSTRUCTION_LOADED_HEIGHT_METRIC);
        let mut progress = Self {
            target_height,
            loaded_height,
            last_published_height: loaded_height,
            last_published_at: now,
            cadence,
            loaded_height_gauge,
            readiness,
        };
        progress.publish(now);
        progress
    }

    /// Advances construction progress after `height` has been staged into the builder.
    pub(crate) fn record_loaded(&mut self, height: BlockHeight) {
        self.record_loaded_at(height, Instant::now());
    }

    fn record_loaded_at(&mut self, height: BlockHeight, now: Instant) {
        let loaded = height.value().min(self.target_height);
        if loaded <= self.loaded_height {
            return;
        }
        self.loaded_height = loaded;
        let blocks_since_publish = loaded.saturating_sub(self.last_published_height);
        let elapsed_since_publish = now.saturating_duration_since(self.last_published_at);
        if loaded >= self.target_height
            || blocks_since_publish >= self.cadence.max_blocks
            || elapsed_since_publish >= self.cadence.max_elapsed
        {
            self.publish(now);
        }
    }

    /// Publishes any throttled height progress before the loader waits idle on upstream.
    pub(crate) fn flush_pending(&mut self) {
        self.flush_pending_at(Instant::now());
    }

    fn flush_pending_at(&mut self, now: Instant) {
        if self.loaded_height > self.last_published_height {
            self.publish(now);
        }
    }

    /// Ensures the gauges and readiness report reflect `loaded_height == target_height`.
    pub(crate) fn finish(&mut self) {
        self.record_loaded(BlockHeight::new(self.target_height));
        self.flush_pending();
    }

    fn publish(&mut self, now: Instant) {
        self.loaded_height_gauge.set(f64::from(self.loaded_height));
        if let Some(readiness) = &self.readiness {
            let lag_blocks = u64::from(self.target_height.saturating_sub(self.loaded_height));
            readiness.publish_admission(
                ReadinessState::syncing(
                    Some(lag_blocks),
                    Some(self.loaded_height),
                    Some(self.target_height),
                )
                .with_phase(IngestPhase::BulkCatchup),
            );
        }
        self.last_published_height = self.loaded_height;
        self.last_published_at = now;
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};

    use metrics_exporter_prometheus::PrometheusBuilder;
    use zinder_core::BlockHeight;
    use zinder_runtime::{ComposedReadiness, IngestPhase, Readiness, ReadinessCause};

    use super::{
        CONSTRUCTION_LOADED_HEIGHT_METRIC, CONSTRUCTION_TARGET_HEIGHT_METRIC, ConstructionProgress,
        PROGRESS_INTERVAL_BLOCKS, PROGRESS_INTERVAL_DURATION, ProgressBounds, ProgressCadence,
    };

    const TEST_CADENCE: ProgressCadence = ProgressCadence {
        max_blocks: PROGRESS_INTERVAL_BLOCKS,
        max_elapsed: PROGRESS_INTERVAL_DURATION,
    };

    fn gauge_sample(rendered: &str, name: &str) -> Result<f64, Box<dyn Error>> {
        let prefix = format!("{name} ");
        let raw_sample = rendered
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .ok_or_else(|| format!("missing gauge sample for {name} in:\n{rendered}"))?;
        Ok(raw_sample.parse()?)
    }

    fn assert_gauge_metadata(rendered: &str, name: &str) {
        assert!(
            rendered
                .lines()
                .any(|line| line.starts_with(&format!("# HELP {name} "))),
            "missing HELP for {name} in:\n{rendered}"
        );
        assert!(
            rendered
                .lines()
                .any(|line| line == format!("# TYPE {name} gauge")),
            "missing TYPE gauge for {name} in:\n{rendered}"
        );
    }

    #[test]
    fn genesis_initialization_publishes_zero_loaded_and_full_target() -> Result<(), Box<dyn Error>>
    {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        let now = std::time::Instant::now();

        let _progress = metrics::with_local_recorder(&recorder, || {
            ConstructionProgress::start_at(
                ProgressBounds {
                    initial_height: BlockHeight::new(0),
                    target_height: BlockHeight::new(1_000),
                },
                Some(readiness.clone()),
                TEST_CADENCE,
                now,
            )
        });

        let rendered = handle.render();
        assert_gauge_metadata(&rendered, CONSTRUCTION_LOADED_HEIGHT_METRIC);
        assert_gauge_metadata(&rendered, CONSTRUCTION_TARGET_HEIGHT_METRIC);
        assert_eq!(
            gauge_sample(&rendered, CONSTRUCTION_LOADED_HEIGHT_METRIC)?,
            0.0
        );
        assert_eq!(
            gauge_sample(&rendered, CONSTRUCTION_TARGET_HEIGHT_METRIC)?,
            1_000.0
        );

        let report = readiness.runtime_readiness().report();
        assert!(!report.is_ready);
        assert_eq!(report.phase, Some(IngestPhase::BulkCatchup));
        assert_eq!(report.current_height, Some(0));
        assert_eq!(report.target_height, Some(1_000));
        assert_eq!(
            report.cause,
            ReadinessCause::Syncing {
                lag_blocks: Some(1_000),
                wait_reason: None,
            }
        );
        Ok(())
    }

    #[test]
    fn checkpointed_initialization_starts_at_checkpoint_height() -> Result<(), Box<dyn Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        let now = std::time::Instant::now();

        let _progress = metrics::with_local_recorder(&recorder, || {
            ConstructionProgress::start_at(
                ProgressBounds {
                    initial_height: BlockHeight::new(280_000),
                    target_height: BlockHeight::new(300_000),
                },
                Some(readiness.clone()),
                TEST_CADENCE,
                now,
            )
        });

        let rendered = handle.render();
        assert_eq!(
            gauge_sample(&rendered, CONSTRUCTION_LOADED_HEIGHT_METRIC)?,
            280_000.0
        );
        assert_eq!(
            gauge_sample(&rendered, CONSTRUCTION_TARGET_HEIGHT_METRIC)?,
            300_000.0
        );

        let report = readiness.runtime_readiness().report();
        assert_eq!(report.current_height, Some(280_000));
        assert_eq!(report.target_height, Some(300_000));
        assert_eq!(
            report.cause,
            ReadinessCause::Syncing {
                lag_blocks: Some(20_000),
                wait_reason: None,
            }
        );
        Ok(())
    }

    #[test]
    fn block_interval_throttles_until_stride_or_target() -> Result<(), Box<dyn Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        let t0 = std::time::Instant::now();

        metrics::with_local_recorder(&recorder, || {
            let mut progress = ConstructionProgress::start_at(
                ProgressBounds {
                    initial_height: BlockHeight::new(0),
                    target_height: BlockHeight::new(300),
                },
                Some(readiness.clone()),
                TEST_CADENCE,
                t0,
            );

            for height in 1_u32..=255 {
                progress.record_loaded_at(BlockHeight::new(height), t0);
            }
            assert_eq!(
                readiness.runtime_readiness().report().current_height,
                Some(0)
            );

            progress.record_loaded_at(BlockHeight::new(256), t0);
            let mid_report = readiness.runtime_readiness().report();
            assert_eq!(mid_report.current_height, Some(256));
            assert_eq!(
                mid_report.cause,
                ReadinessCause::Syncing {
                    lag_blocks: Some(44),
                    wait_reason: None,
                }
            );

            progress.record_loaded_at(BlockHeight::new(300), t0);
        });

        let rendered = handle.render();
        assert_eq!(
            gauge_sample(&rendered, CONSTRUCTION_LOADED_HEIGHT_METRIC)?,
            300.0
        );
        let final_report = readiness.runtime_readiness().report();
        assert_eq!(final_report.current_height, Some(300));
        assert_eq!(
            final_report.cause,
            ReadinessCause::Syncing {
                lag_blocks: Some(0),
                wait_reason: None,
            }
        );
        Ok(())
    }

    #[test]
    fn elapsed_interval_and_idle_flush_publish_unreported_progress() -> Result<(), Box<dyn Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        let t0 = std::time::Instant::now();

        metrics::with_local_recorder(&recorder, || {
            let mut progress = ConstructionProgress::start_at(
                ProgressBounds {
                    initial_height: BlockHeight::new(0),
                    target_height: BlockHeight::new(1_000),
                },
                Some(readiness.clone()),
                TEST_CADENCE,
                t0,
            );

            let before_interval = t0 + Duration::from_millis(249);
            progress.record_loaded_at(BlockHeight::new(10), before_interval);
            assert_eq!(
                readiness.runtime_readiness().report().current_height,
                Some(0)
            );

            let at_interval = t0 + Duration::from_millis(250);
            progress.record_loaded_at(BlockHeight::new(11), at_interval);
            assert_eq!(
                readiness.runtime_readiness().report().current_height,
                Some(11)
            );

            progress.record_loaded_at(BlockHeight::new(15), at_interval);
            assert_eq!(
                readiness.runtime_readiness().report().current_height,
                Some(11)
            );
            progress.flush_pending_at(at_interval);
            assert_eq!(
                readiness.runtime_readiness().report().current_height,
                Some(15)
            );
            // Calling flush again without new blocks is a no-op.
            progress.flush_pending_at(at_interval);
            assert_eq!(
                readiness.runtime_readiness().report().current_height,
                Some(15)
            );
        });

        let rendered = handle.render();
        assert_eq!(
            gauge_sample(&rendered, CONSTRUCTION_LOADED_HEIGHT_METRIC)?,
            15.0
        );
        Ok(())
    }

    #[test]
    fn progress_stays_monotonic_clamps_to_target_and_works_without_readiness()
    -> Result<(), Box<dyn Error>> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let t0 = std::time::Instant::now();

        metrics::with_local_recorder(&recorder, || {
            let mut progress = ConstructionProgress::start_at(
                ProgressBounds {
                    initial_height: BlockHeight::new(0),
                    target_height: BlockHeight::new(20),
                },
                None,
                ProgressCadence {
                    max_blocks: 1,
                    max_elapsed: Duration::from_secs(1),
                },
                t0,
            );
            progress.record_loaded_at(BlockHeight::new(10), t0);
            progress.record_loaded_at(BlockHeight::new(5), t0);
            progress.record_loaded_at(BlockHeight::new(10), t0);
            assert_eq!(progress.loaded_height, 10);

            progress.record_loaded_at(BlockHeight::new(25), t0);
            assert_eq!(progress.loaded_height, 20);
            progress.finish();
            assert_eq!(progress.loaded_height, 20);
        });

        let rendered = handle.render();
        assert_eq!(
            gauge_sample(&rendered, CONSTRUCTION_LOADED_HEIGHT_METRIC)?,
            20.0
        );
        assert_eq!(
            gauge_sample(&rendered, CONSTRUCTION_TARGET_HEIGHT_METRIC)?,
            20.0
        );
        Ok(())
    }
}
