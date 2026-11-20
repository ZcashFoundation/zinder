//! Prometheus metrics recorder used by Zinder operational endpoints.

use std::sync::OnceLock;

use metrics::{Unit, describe_gauge};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use parking_lot::Mutex;
use thiserror::Error;

use crate::{
    BUILD_GIT_COMMIT, OpsServer, public_grpc_server::RECEIVE_SLOTS_HELD_AT_ADMISSION_METRIC,
    sections::RuntimeService,
};

static PROMETHEUS_HANDLE: OnceLock<MetricsHandle> = OnceLock::new();
static PROMETHEUS_INSTALL_LOCK: Mutex<()> = Mutex::new(());

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
        source: metrics_exporter_prometheus::BuildError,
    },
}

/// Installs the global Prometheus recorder and records this service's build
/// identity.
///
/// The recorder is process-wide because the `metrics` facade has one global
/// recorder. Calling this more than once through Zinder runtime returns the
/// already-installed handle and records another `zinder_build_info` label set.
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
        let prometheus = prometheus_builder()
            .and_then(PrometheusBuilder::install_recorder)
            .map_err(|source| MetricsInstallError::Install { source })?;
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

/// The Prometheus exporter every Zinder runtime installs, with its histogram
/// buckets.
fn prometheus_builder() -> Result<PrometheusBuilder, metrics_exporter_prometheus::BuildError> {
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
    use super::{RECEIVE_SLOTS_HELD_AT_ADMISSION_METRIC, prometheus_builder};

    #[test]
    fn receive_slot_occupancy_exports_buckets_that_single_out_a_full_pool()
    -> Result<(), metrics_exporter_prometheus::BuildError> {
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
