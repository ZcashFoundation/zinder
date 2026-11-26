//! Process-wide Prometheus recorder and scoped `EncodedTail` lookup observer.

use std::{
    collections::HashMap,
    fmt,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use parking_lot::Mutex;
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
    span,
};
use tracing_subscriber::{
    EnvFilter, Layer,
    filter::filter_fn,
    layer::{Context, SubscriberExt as _},
    registry::LookupSpan,
    util::SubscriberInitExt as _,
};

use crate::error::BenchError;

type ProbeEventMap = Mutex<HashMap<(u16, u64), Vec<EncodedTailLookupRecord>>>;

static RECORDER_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
static INSTALL_LOCK: Mutex<()> = Mutex::new(());
static TRACING_INSTALLED: OnceLock<()> = OnceLock::new();
static TRACING_INSTALL_LOCK: Mutex<()> = Mutex::new(());
static NEXT_PROBE_ID: AtomicU64 = AtomicU64::new(1);
static PROBE_EVENTS: OnceLock<ProbeEventMap> = OnceLock::new();

pub(crate) const BENCH_PROBE_ID_HEADER: &str = "x-zinder-bench-probe-id";
pub(crate) const ENCODED_TAIL_TARGET: &str = "zinder::compat_lightwalletd::encoded_tail";
pub(crate) const BENCH_SERVER_SPAN_TARGET: &str = "zinder::bench::server";

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

/// Installs the process-wide `tracing` subscriber combining stderr logging with
/// request-scoped `EncodedTail` lookup capture.
///
/// Safe to invoke repeatedly once installed by this helper.
///
/// # Errors
///
/// Returns [`BenchError::Recorder`] if another global `tracing` subscriber was
/// already installed, which would leave request-scoped `EncodedTail` probe
/// capture inactive.
pub fn install_tracing_subscriber() -> Result<(), BenchError> {
    install_tracing_subscriber_with(&TRACING_INSTALLED, &TRACING_INSTALL_LOCK, || {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_filter(env_filter);
        let capture_layer = EncodedTailCaptureLayer.with_filter(filter_fn(|meta| {
            let target = meta.target();
            target == ENCODED_TAIL_TARGET || target == BENCH_SERVER_SPAN_TARGET
        }));
        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(capture_layer)
            .try_init()
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

/// One server-side `encoded_tail_lookup` resolution correlated to a request probe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EncodedTailLookupRecord {
    pub(crate) order: String,
    pub(crate) range_start: u32,
    pub(crate) range_end: u32,
    pub(crate) outcome: String,
    pub(crate) chain_epoch: u64,
    pub(crate) visible_tip_height: u32,
    pub(crate) catalog_end_height: u32,
    pub(crate) tail_generation: u64,
    pub(crate) byte_capped: bool,
}

/// Registers a unique request probe id scoped to `server_port` and drains all
/// correlated `encoded_tail_lookup` events once finished.
pub(crate) struct ProbeCaptureGuard {
    server_port: u16,
    probe_id: u64,
    taken: bool,
}

impl ProbeCaptureGuard {
    pub(crate) const fn probe_id(&self) -> u64 {
        self.probe_id
    }

    pub(crate) fn take_events(mut self) -> Vec<EncodedTailLookupRecord> {
        self.taken = true;
        probe_events()
            .lock()
            .remove(&(self.server_port, self.probe_id))
            .unwrap_or_default()
    }
}

impl Drop for ProbeCaptureGuard {
    fn drop(&mut self) {
        if !self.taken {
            probe_events()
                .lock()
                .remove(&(self.server_port, self.probe_id));
        }
    }
}

/// Begins capturing `encoded_tail_lookup` events tagged for `server_port`.
pub(crate) fn begin_probe_capture(server_port: u16) -> ProbeCaptureGuard {
    let probe_id = NEXT_PROBE_ID.fetch_add(1, Ordering::Relaxed);
    probe_events()
        .lock()
        .insert((server_port, probe_id), Vec::new());
    ProbeCaptureGuard {
        server_port,
        probe_id,
        taken: false,
    }
}

fn probe_events() -> &'static ProbeEventMap {
    PROBE_EVENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Clone, Copy)]
struct ProbeSpanMeta {
    server_port: u16,
    probe_id: u64,
}

#[derive(Default)]
struct SpanProbeVisitor {
    server_port: u64,
    probe_id: u64,
}

impl Visit for SpanProbeVisitor {
    fn record_u64(&mut self, field: &Field, num: u64) {
        match field.name() {
            "server_port" => self.server_port = num,
            "probe_id" => self.probe_id = num,
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _debug_val: &dyn fmt::Debug) {}
}

#[derive(Default)]
struct LookupRecordVisitor {
    event_name: String,
    order: String,
    range_start: u32,
    range_end: u32,
    outcome: String,
    chain_epoch: u64,
    visible_tip_height: u32,
    catalog_end_height: u32,
    tail_generation: u64,
    byte_capped: bool,
}

impl Visit for LookupRecordVisitor {
    fn record_bool(&mut self, field: &Field, flag: bool) {
        if field.name() == "byte_capped" {
            self.byte_capped = flag;
        }
    }

    fn record_u64(&mut self, field: &Field, num: u64) {
        match field.name() {
            "range_start" => self.range_start = u32::try_from(num).unwrap_or(u32::MAX),
            "range_end" => self.range_end = u32::try_from(num).unwrap_or(u32::MAX),
            "chain_epoch" => self.chain_epoch = num,
            "visible_tip_height" => {
                self.visible_tip_height = u32::try_from(num).unwrap_or(u32::MAX);
            }
            "catalog_end_height" => {
                self.catalog_end_height = u32::try_from(num).unwrap_or(u32::MAX);
            }
            "tail_generation" => self.tail_generation = num,
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, text: &str) {
        match field.name() {
            "event" => text.clone_into(&mut self.event_name),
            "order" => text.clone_into(&mut self.order),
            "outcome" => text.clone_into(&mut self.outcome),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, debug_val: &dyn fmt::Debug) {
        let rendered = format!("{debug_val:?}");
        let trimmed = rendered.trim_matches('"');
        self.record_str(field, trimmed);
    }
}

impl LookupRecordVisitor {
    fn into_record(self) -> Option<EncodedTailLookupRecord> {
        (self.event_name == "encoded_tail_lookup").then_some(EncodedTailLookupRecord {
            order: self.order,
            range_start: self.range_start,
            range_end: self.range_end,
            outcome: self.outcome,
            chain_epoch: self.chain_epoch,
            visible_tip_height: self.visible_tip_height,
            catalog_end_height: self.catalog_end_height,
            tail_generation: self.tail_generation,
            byte_capped: self.byte_capped,
        })
    }
}

struct EncodedTailCaptureLayer;

impl<S> Layer<S> for EncodedTailCaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        if attrs.metadata().target() != BENCH_SERVER_SPAN_TARGET {
            return;
        }
        let mut visitor = SpanProbeVisitor::default();
        attrs.record(&mut visitor);
        let Ok(server_port) = u16::try_from(visitor.server_port) else {
            return;
        };
        if visitor.probe_id == 0 {
            return;
        }
        if let Some(span_ref) = ctx.span(id) {
            span_ref.extensions_mut().insert(ProbeSpanMeta {
                server_port,
                probe_id: visitor.probe_id,
            });
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if event.metadata().target() != ENCODED_TAIL_TARGET {
            return;
        }
        let meta = ctx.event_scope(event).and_then(|mut scope| {
            scope.find_map(|span| span.extensions().get::<ProbeSpanMeta>().copied())
        });
        let Some(meta) = meta else {
            return;
        };
        let mut visitor = LookupRecordVisitor::default();
        event.record(&mut visitor);
        let Some(record) = visitor.into_record() else {
            return;
        };
        let mut map = probe_events().lock();
        if let Some(entries) = map.get_mut(&(meta.server_port, meta.probe_id)) {
            entries.push(record);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    };

    use parking_lot::Mutex;

    use super::{
        BENCH_SERVER_SPAN_TARGET, ENCODED_TAIL_TARGET, begin_probe_capture,
        install_tracing_subscriber, install_tracing_subscriber_with,
    };
    use crate::error::BenchError;

    #[test]
    fn scoped_capture_records_events_across_nested_spans() -> Result<(), Box<dyn std::error::Error>>
    {
        install_tracing_subscriber()?;
        install_tracing_subscriber()?;
        let guard = begin_probe_capture(42_420);
        let probe_id = guard.probe_id();
        let req_span = tracing::debug_span!(
            target: BENCH_SERVER_SPAN_TARGET,
            "bench_grpc_request",
            server_port = 42_420_u64,
            probe_id
        );
        req_span.in_scope(|| {
            let child_span = tracing::info_span!("child_work");
            child_span.in_scope(|| {
                tracing::debug!(
                    target: ENCODED_TAIL_TARGET,
                    event = "encoded_tail_lookup",
                    order = "descending",
                    range_start = 10_u32,
                    range_end = 20_u32,
                    outcome = "rebuild",
                    chain_epoch = 7_u64,
                    visible_tip_height = 20_u32,
                    catalog_end_height = 0_u32,
                    tail_generation = 3_u64,
                    byte_capped = false,
                    "resolved live encoded-tail lookup"
                );
            });
        });

        let events = guard.take_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].outcome, "rebuild");
        assert_eq!(events[0].chain_epoch, 7);
        assert_eq!(events[0].tail_generation, 3);
        Ok(())
    }

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
