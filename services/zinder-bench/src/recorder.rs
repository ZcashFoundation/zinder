//! Process-wide Prometheus recorder and tracing subscriber setup.

use std::{fmt, sync::OnceLock};

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use parking_lot::Mutex;
use tracing_subscriber::{
    EnvFilter, Layer as _, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};

use crate::error::BenchError;

static RECORDER_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
static INSTALL_LOCK: Mutex<()> = Mutex::new(());
static TRACING_INSTALLED: OnceLock<()> = OnceLock::new();
static TRACING_INSTALL_LOCK: Mutex<()> = Mutex::new(());

/// Installs the process-wide Prometheus recorder and returns a render handle.
///
/// The `metrics` facade allows exactly one global recorder, so repeated calls
/// return the handle installed on the first call. Rendering the handle after a
/// replay yields the exposition text the report parses.
pub fn install_recorder() -> Result<PrometheusHandle, BenchError> {
    if let Some(handle) = RECORDER_HANDLE.get() {
        return Ok(handle.clone());
    }
    let install_guard = INSTALL_LOCK.lock();
    if let Some(handle) = RECORDER_HANDLE.get() {
        drop(install_guard);
        return Ok(handle.clone());
    }
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|source| BenchError::Recorder {
            reason: source.to_string(),
        })?;
    let _ = RECORDER_HANDLE.set(handle.clone());
    drop(install_guard);
    Ok(handle)
}

/// Installs the process-wide `tracing` subscriber for stderr logging.
///
/// Safe to invoke repeatedly once installed by this helper.
///
/// # Errors
///
/// Returns [`BenchError::Recorder`] if another global `tracing` subscriber was
/// already installed.
pub fn install_tracing_subscriber() -> Result<(), BenchError> {
    install_tracing_subscriber_with(&TRACING_INSTALLED, &TRACING_INSTALL_LOCK, || {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_filter(env_filter);
        tracing_subscriber::registry().with(fmt_layer).try_init()
    })
}

fn install_tracing_subscriber_with<F, E>(
    installed: &OnceLock<()>,
    install_lock: &Mutex<()>,
    init: F,
) -> Result<(), BenchError>
where
    F: FnOnce() -> Result<(), E>,
    E: fmt::Display,
{
    if installed.get().is_some() {
        return Ok(());
    }
    let install_guard = install_lock.lock();
    if installed.get().is_some() {
        drop(install_guard);
        return Ok(());
    }
    init().map_err(|source| BenchError::Recorder {
        reason: format!("failed to install tracing capture subscriber: {source}"),
    })?;
    let _ = installed.set(());
    drop(install_guard);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    };

    use parking_lot::Mutex;

    use super::{BenchError, install_tracing_subscriber_with};

    #[test]
    fn install_tracing_subscriber_propagates_error_and_does_not_mark_initialized()
    -> Result<(), Box<dyn std::error::Error>> {
        let installed = OnceLock::new();
        let install_lock = Mutex::new(());
        let calls = AtomicUsize::new(0);

        let Err(err) = install_tracing_subscriber_with(&installed, &install_lock, || {
            calls.fetch_add(1, Ordering::Relaxed);
            Err::<(), &str>("global subscriber already set")
        }) else {
            return Err("failing subscriber install must propagate error".into());
        };
        assert!(matches!(err, BenchError::Recorder { .. }));
        assert!(installed.get().is_none());
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        install_tracing_subscriber_with(&installed, &install_lock, || {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok::<(), &str>(())
        })?;
        assert!(installed.get().is_some());
        assert_eq!(calls.load(Ordering::Relaxed), 2);

        install_tracing_subscriber_with(&installed, &install_lock, || {
            calls.fetch_add(1, Ordering::Relaxed);
            Err::<(), &str>("should not invoke init once installed")
        })?;
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        Ok(())
    }
}
