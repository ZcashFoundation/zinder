//! Prometheus metrics recorder used by Zinder operational endpoints.

use std::{io, sync::OnceLock, thread, time::Duration};

use metrics::{Unit, describe_gauge};
use metrics_exporter_prometheus::{
    BuildError, Matcher, PrometheusBuilder, PrometheusHandle, PrometheusRecorder,
};
use parking_lot::Mutex;
use thiserror::Error;

use crate::{
    BUILD_GIT_COMMIT, OpsServer, public_grpc_server::RECEIVE_SLOTS_HELD_AT_ADMISSION_METRIC,
    sections::RuntimeService,
};

static PROMETHEUS_HANDLE: OnceLock<MetricsHandle> = OnceLock::new();
static PROMETHEUS_INSTALL_LOCK: Mutex<()> = Mutex::new(());

/// How often the installed recorder drains buffered histogram samples.
///
/// The recorder buffers each histogram sample until a render or an upkeep
/// moves it into its buckets or summary, so without upkeep a runtime whose
/// `/metrics` nobody scrapes keeps every sample it records. Five seconds, the
/// exporter's own default, bounds that buffer to the samples of one interval.
const UPKEEP_INTERVAL: Duration = Duration::from_secs(5);

/// Name of the thread that runs recorder upkeep. Linux keeps at most 15 bytes
/// of a thread name.
const UPKEEP_THREAD_NAME: &str = "metrics-upkeep";

/// Histogram bucket boundaries (in seconds) applied to every metric whose name
/// ends in `_duration_seconds`.
///
/// The 3x-spaced sequence covers 1µs to 10s with enough resolution for
/// `histogram_quantile()` to compute meaningful p50/p95/p99 across the full
/// observed range: store reads (~10µs), node RPCs (~1ms), commit pipeline
/// (~100ms), backups (seconds). Reconfiguring after data has been recorded
/// invalidates pre-existing series, so changes here are sticky.
const DURATION_SECONDS_BUCKETS: &[f64] = &[
    1e-6, 3e-6, 1e-5, 3e-5, 1e-4, 3e-4, 1e-3, 3e-3, 1e-2, 3e-2, 1e-1, 3e-1, 1.0, 3.0, 10.0,
];

/// Histogram bucket boundaries (in seconds) for the startup-phase
/// duration histogram.
///
/// Startup phases span ~10 ms for config loading to several minutes for
/// cold WAL replay, so the buckets cover 0.01 s to 600 s. The upper
/// bucket alerts an SRE that a startup phase exceeded ten minutes, which
/// is the shape `open_storage` takes during the bulk-catchup OOM trap
/// described in
/// [the resource-tuning runbook](../../../docs/runbooks/bulk-catchup-resource-tuning.md).
const STARTUP_PHASE_DURATION_SECONDS_BUCKETS: &[f64] = &[
    0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 60.0, 180.0, 300.0, 600.0,
];

/// Histogram bucket boundaries for the receive slots a public gRPC pool held
/// once it admitted a request.
///
/// They step up to the default 32-slot pool, so the last bucket holds only
/// the requests that left the pool full, by taking its last free slot or
/// another client's slot.
const RECEIVE_SLOTS_HELD_BUCKETS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 24.0, 31.0, 32.0];

/// Handle used by `/metrics` to render the process-wide Prometheus snapshot.
#[derive(Clone, Debug)]
pub struct MetricsHandle {
    prometheus: PrometheusHandle,
}

impl MetricsHandle {
    /// Renders the current metric snapshot in Prometheus text format.
    #[must_use]
    pub fn render(&self) -> String {
        self.prometheus.render()
    }
}

/// Error returned when the process-wide metrics recorder cannot be installed.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MetricsInstallError {
    /// Failed to install the Prometheus recorder as the global metrics recorder.
    #[error("failed to install metrics recorder: {source}")]
    Install {
        /// Underlying recorder installation error.
        #[source]
        source: BuildError,
    },
    /// Failed to start the thread that drains buffered histogram samples.
    #[error("failed to start metrics upkeep: {source}")]
    Upkeep {
        /// Underlying thread spawn error.
        #[source]
        source: io::Error,
    },
}

/// Installs the global Prometheus recorder and records this service's build
/// identity.
///
/// The recorder is process-wide because the `metrics` facade has one global
/// recorder. Calling this more than once through Zinder runtime returns the
/// already-installed handle and records another `zinder_build_info` label set.
/// The first install also starts a thread that drains buffered histogram
/// samples every five seconds, so memory stays bounded while nothing scrapes
/// `/metrics`.
pub fn install_metrics_recorder(server: &OpsServer) -> Result<MetricsHandle, MetricsInstallError> {
    install_metrics_recorder_with_identity(
        server.service_name,
        server.service_version,
        server.network_name,
    )
}

/// Installs the global Prometheus recorder for a known runtime service and
/// records its build identity.
///
/// Use this before startup work that emits metrics but precedes construction
/// of an [`OpsServer`]. Later operational-endpoint startup reuses the same
/// recorder through [`install_metrics_recorder`].
pub fn install_metrics_recorder_for_service(
    service: RuntimeService,
    service_version: &'static str,
    network_name: &'static str,
) -> Result<MetricsHandle, MetricsInstallError> {
    install_metrics_recorder_with_identity(service.binary_name(), service_version, network_name)
}

fn install_metrics_recorder_with_identity(
    service_name: &'static str,
    service_version: &'static str,
    network_name: &'static str,
) -> Result<MetricsHandle, MetricsInstallError> {
    let handle = if let Some(handle) = PROMETHEUS_HANDLE.get() {
        handle.clone()
    } else {
        let _install_guard = PROMETHEUS_INSTALL_LOCK.lock();
        if let Some(handle) = PROMETHEUS_HANDLE.get() {
            return Ok(record_build_info(
                handle.clone(),
                service_name,
                service_version,
                network_name,
            ));
        }
        let prometheus = install_global_recorder(
            |recorder| metrics::set_global_recorder(recorder).map_err(BuildError::from),
            |prometheus| drain_on_schedule(prometheus, UPKEEP_INTERVAL),
        )?;
        let installed = MetricsHandle { prometheus };
        let _ = PROMETHEUS_HANDLE.set(installed.clone());
        installed
    };

    Ok(record_build_info(
        handle,
        service_name,
        service_version,
        network_name,
    ))
}

/// Builds the recorder, makes it global through `set_global`, and only then
/// starts its upkeep through `start_upkeep`, so a recorder that never becomes
/// the global one leaves no upkeep thread behind.
fn install_global_recorder(
    set_global: impl FnOnce(PrometheusRecorder) -> Result<(), BuildError>,
    start_upkeep: impl FnOnce(PrometheusHandle) -> io::Result<()>,
) -> Result<PrometheusHandle, MetricsInstallError> {
    let recorder = prometheus_builder()
        .map_err(|source| MetricsInstallError::Install { source })?
        .build_recorder();
    let prometheus = recorder.handle();
    set_global(recorder).map_err(|source| MetricsInstallError::Install { source })?;
    start_upkeep(prometheus.clone()).map_err(|source| MetricsInstallError::Upkeep { source })?;
    Ok(prometheus)
}

/// Moves the histogram samples that `prometheus` buffers into their buckets
/// or summaries every `interval`.
fn drain_on_schedule(prometheus: PrometheusHandle, interval: Duration) -> io::Result<()> {
    spawn_upkeep(interval, move || prometheus.run_upkeep())
}

/// Runs `upkeep` every `interval` on a thread of its own until the process
/// exits, since the global recorder it maintains lives that long.
fn spawn_upkeep(interval: Duration, upkeep: impl Fn() + Send + 'static) -> io::Result<()> {
    thread::Builder::new()
        .name(UPKEEP_THREAD_NAME.to_owned())
        .spawn(move || {
            loop {
                thread::sleep(interval);
                upkeep();
            }
        })
        .map(drop)
}

/// The Prometheus exporter every Zinder runtime installs, with its histogram
/// buckets.
fn prometheus_builder() -> Result<PrometheusBuilder, BuildError> {
    PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("zinder_startup_phase_duration_seconds".to_owned()),
            STARTUP_PHASE_DURATION_SECONDS_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Suffix("_duration_seconds".to_owned()),
            DURATION_SECONDS_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full(RECEIVE_SLOTS_HELD_AT_ADMISSION_METRIC.to_owned()),
            RECEIVE_SLOTS_HELD_BUCKETS,
        )
}

fn record_build_info(
    handle: MetricsHandle,
    service_name: &'static str,
    service_version: &'static str,
    network_name: &'static str,
) -> MetricsHandle {
    describe_gauge!(
        "zinder_build_info",
        Unit::Count,
        "Build-time identity for this Zinder service."
    );
    metrics::gauge!(
        "zinder_build_info",
        "service" => service_name,
        "version" => service_version,
        "git_commit" => BUILD_GIT_COMMIT,
        "network" => network_name
    )
    .set(1.0);

    handle
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, sync::mpsc, thread, time::Duration};

    use metrics::SetRecorderError;
    use metrics_exporter_prometheus::{PrometheusHandle, PrometheusRecorder};

    use super::{
        MetricsInstallError, RECEIVE_SLOTS_HELD_AT_ADMISSION_METRIC, drain_on_schedule,
        install_global_recorder, prometheus_builder, spawn_upkeep,
    };

    /// A histogram no bucket matcher names, so it renders as a summary.
    const UPKEEP_PROBE_METRIC: &str = "zinder_test_upkeep_probe";

    /// Records 64 samples of 1, waits for `pause`, records one sample of 100,
    /// and returns the smallest sample the rendered summary still counts.
    ///
    /// The recorder buffers samples in fixed-size blocks that 64 samples fill
    /// exactly, so the sample of 100 starts a block of its own, and a drain
    /// takes the newest block first. The summary starts its window at the
    /// first sample it takes in and drops any older one. Drained together,
    /// the samples of 1 arrive after the sample of 100 and are dropped.
    /// Drained before it, they all count.
    fn smallest_counted_sample(
        recorder: &PrometheusRecorder,
        prometheus: &PrometheusHandle,
        pause: Duration,
    ) -> Option<f64> {
        metrics::with_local_recorder(recorder, || {
            let histogram = metrics::histogram!(UPKEEP_PROBE_METRIC);
            for _ in 0..64 {
                histogram.record(1.0);
            }
            thread::sleep(pause);
            histogram.record(100.0);
        });
        let quantile_line = format!("{UPKEEP_PROBE_METRIC}{{quantile=\"0\"}} ");
        prometheus
            .render()
            .lines()
            .find_map(|line| line.strip_prefix(&quantile_line))
            .and_then(|smallest| smallest.parse().ok())
    }

    #[test]
    fn scheduled_upkeep_drains_buffered_samples() -> Result<(), Box<dyn std::error::Error>> {
        // Unless one render drains the samples of 1 only after the sample of
        // 100, the probe cannot tell an upkeep that drains from one that does
        // nothing.
        let unattended = prometheus_builder()?.build_recorder();
        let unattended_handle = unattended.handle();
        let smallest = smallest_counted_sample(&unattended, &unattended_handle, Duration::ZERO);
        assert!(
            smallest.is_some_and(|smallest| smallest > 50.0),
            "{smallest:?}"
        );

        let recorder = prometheus_builder()?.build_recorder();
        let prometheus = recorder.handle();
        drain_on_schedule(prometheus.clone(), Duration::from_millis(2))?;
        let smallest = smallest_counted_sample(&recorder, &prometheus, Duration::from_millis(500));
        assert!(
            smallest.is_some_and(|smallest| smallest < 50.0),
            "{smallest:?}"
        );
        Ok(())
    }

    #[test]
    fn a_recorder_that_does_not_become_global_starts_no_upkeep() {
        let upkeep_started = Cell::new(false);
        let installed = install_global_recorder(
            |recorder| Err(SetRecorderError(recorder).into()),
            |_| {
                upkeep_started.set(true);
                Ok(())
            },
        );

        assert!(matches!(
            installed,
            Err(MetricsInstallError::Install { .. })
        ));
        assert!(!upkeep_started.get());
    }

    #[test]
    fn upkeep_runs_again_after_every_interval() -> Result<(), Box<dyn std::error::Error>> {
        let (runs, observed) = mpsc::channel();
        spawn_upkeep(Duration::from_millis(5), move || {
            // The receiver stops listening once the test has seen enough.
            let _ = runs.send(());
        })?;

        for _ in 0..3 {
            observed.recv_timeout(Duration::from_secs(10))?;
        }
        Ok(())
    }

    #[test]
    fn receive_slot_occupancy_exports_buckets_that_single_out_a_full_pool()
    -> Result<(), super::BuildError> {
        let recorder = prometheus_builder()?.build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            for held in [1.0, 3.0, 32.0] {
                metrics::histogram!(RECEIVE_SLOTS_HELD_AT_ADMISSION_METRIC, "pool" => "request")
                    .record(held);
            }
        });

        let exposition = handle.render();
        for expected in [
            "zinder_grpc_request_receive_slots_held_at_admission_bucket{pool=\"request\",le=\"1\"} 1",
            "zinder_grpc_request_receive_slots_held_at_admission_bucket{pool=\"request\",le=\"4\"} 2",
            "zinder_grpc_request_receive_slots_held_at_admission_bucket{pool=\"request\",le=\"31\"} 2",
            "zinder_grpc_request_receive_slots_held_at_admission_bucket{pool=\"request\",le=\"32\"} 3",
        ] {
            assert!(exposition.contains(expected), "{expected}\n{exposition}");
        }
        Ok(())
    }
}
