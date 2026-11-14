//! Append-only JSON-lines result files and the run clock.

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU32, Ordering},
    time::Instant,
};

use parking_lot::Mutex;
use serde::Serialize;
use zinder_bench::BenchError;

pub(crate) struct JsonlSink {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
}

impl JsonlSink {
    pub(crate) fn create(path: &Path) -> Result<Self, BenchError> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|source| BenchError::io(path, source))?;
        Ok(Self {
            path: path.to_path_buf(),
            writer: Mutex::new(BufWriter::new(file)),
        })
    }

    pub(crate) fn write<T: Serialize>(&self, record: &T) {
        let Ok(mut line) = serde_json::to_vec(record) else {
            return;
        };
        line.push(b'\n');
        let mut writer = self.writer.lock();
        if let Err(error) = writer.write_all(&line) {
            tracing::warn!(target: "zinder::bench::spike", path = %self.path.display(), %error, "jsonl write failed");
        }
    }

    pub(crate) fn flush(&self) {
        let _ = self.writer.lock().flush();
    }
}

/// Every result file of one run.
pub(crate) struct Outputs {
    pub(crate) commits: JsonlSink,
    pub(crate) samples: JsonlSink,
    pub(crate) reader_cycles: JsonlSink,
    pub(crate) events: JsonlSink,
}

impl Outputs {
    pub(crate) fn create(out_dir: &Path) -> Result<Self, BenchError> {
        Ok(Self {
            commits: JsonlSink::create(&out_dir.join("commits.jsonl"))?,
            samples: JsonlSink::create(&out_dir.join("samples.jsonl"))?,
            reader_cycles: JsonlSink::create(&out_dir.join("reader-cycles.jsonl"))?,
            events: JsonlSink::create(&out_dir.join("events.jsonl"))?,
        })
    }

    pub(crate) fn flush(&self) {
        self.commits.flush();
        self.samples.flush();
        self.reader_cycles.flush();
        self.events.flush();
    }
}

#[derive(Serialize)]
struct EventRecord<'a> {
    t: f64,
    phase: &'a str,
    event: &'a str,
    detail: serde_json::Value,
}

/// Seconds since the run started, plus the current phase name.
pub(crate) struct RunClock {
    started: Instant,
    phase_index: AtomicU32,
    phase_names: Vec<String>,
}

impl RunClock {
    pub(crate) fn new(phase_names: Vec<String>) -> Self {
        Self {
            started: Instant::now(),
            phase_index: AtomicU32::new(0),
            phase_names,
        }
    }

    pub(crate) fn t(&self) -> f64 {
        self.started.elapsed().as_secs_f64()
    }

    pub(crate) fn set_phase(&self, index: usize) {
        self.phase_index.store(index as u32, Ordering::SeqCst);
    }

    pub(crate) fn phase(&self) -> &str {
        let index = self.phase_index.load(Ordering::SeqCst) as usize;
        self.phase_names.get(index).map_or("setup", String::as_str)
    }

    pub(crate) fn event(&self, outputs: &Outputs, event: &str, detail: serde_json::Value) {
        tracing::info!(target: "zinder::bench::spike", t = self.t(), phase = self.phase(), event, %detail, "spike event");
        outputs.events.write(&EventRecord {
            t: self.t(),
            phase: self.phase(),
            event,
            detail,
        });
        outputs.events.flush();
    }
}
