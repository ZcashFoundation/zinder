//! Process identity and resident memory for operational metrics.

use std::{
    fmt::{Display, Write as _},
    fs,
};

/// A single observation of the current process's resident memory.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProcessMemory {
    /// Resident set size in bytes, from Linux `VmRSS`.
    pub rss_bytes: Option<u64>,
    /// Anonymous resident memory in bytes, from Linux `RssAnon`.
    pub rss_anon_bytes: Option<u64>,
}

impl ProcessMemory {
    /// Samples both fields from one read. Each field is absent when unavailable.
    #[must_use]
    pub fn sample() -> Self {
        if cfg!(target_os = "linux")
            && let Ok(status) = fs::read("/proc/self/status")
        {
            return Self::from_status(&String::from_utf8_lossy(&status));
        }
        Self::default()
    }

    fn from_status(status: &str) -> Self {
        let bytes = |field| {
            let field_text = status.lines().find_map(|line| line.strip_prefix(field))?;
            let mut parts = field_text.split_ascii_whitespace();
            let kib = parts.next()?.parse::<u64>().ok()?;
            if parts.next() != Some("kB") || parts.next().is_some() {
                return None;
            }
            kib.checked_mul(1024)
        };
        Self {
            rss_bytes: bytes("VmRSS:"),
            rss_anon_bytes: bytes("RssAnon:"),
        }
    }
}

pub(super) fn append_metrics(exposition: &mut String) {
    // The recorder retains gauges. Render these optional samples directly so a
    // missing procfs field disappears even after a previous successful scrape.
    #[cfg(target_os = "linux")]
    append_gauge(
        exposition,
        "zinder_process_start_time_seconds",
        "Unix time when this process started, in seconds.",
        start_time_seconds(),
    );
    append_memory(exposition, ProcessMemory::sample());
}

fn append_memory(exposition: &mut String, memory: ProcessMemory) {
    append_gauge(
        exposition,
        "zinder_process_rss_bytes",
        "Resident memory of this process in bytes.",
        memory.rss_bytes,
    );
    append_gauge(
        exposition,
        "zinder_process_rss_anon_bytes",
        "Anonymous resident memory of this process in bytes.",
        memory.rss_anon_bytes,
    );
}

fn append_gauge(exposition: &mut String, name: &str, help: &str, sample: Option<impl Display>) {
    if let Some(sample) = sample {
        let _ = writeln!(
            exposition,
            "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {sample}"
        );
    }
}

#[cfg(target_os = "linux")]
fn start_time_seconds() -> Option<f64> {
    use std::sync::OnceLock;

    static START_TIME_SECONDS: OnceLock<f64> = OnceLock::new();
    if let Some(start_time) = START_TIME_SECONDS.get() {
        return Some(*start_time);
    }
    let process_stat = fs::read("/proc/self/stat").ok()?;
    let system_stat = fs::read_to_string("/proc/stat").ok()?;
    let start = parse_start_time(
        &String::from_utf8_lossy(&process_stat),
        &system_stat,
        rustix::param::clock_ticks_per_second(),
    )?;
    Some(*START_TIME_SECONDS.get_or_init(|| start))
}

#[cfg(target_os = "linux")]
#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus Unix timestamps use f64 seconds"
)]
fn parse_start_time(stat: &str, system_stat: &str, ticks_per_second: u64) -> Option<f64> {
    // Field 2 (comm) can contain spaces and right parentheses. The remaining
    // fields start at 3 (state), and field 22 is ticks since boot at start.
    let (_, fields) = stat.rsplit_once(')')?;
    let ticks = fields
        .split_ascii_whitespace()
        .nth(19)?
        .parse::<u64>()
        .ok()?;
    let boot = system_stat
        .lines()
        .find_map(|line| line.strip_prefix("btime "))?;
    let boot = boot.trim().parse::<u64>().ok()?;
    (ticks_per_second > 0).then(|| boot as f64 + ticks as f64 / ticks_per_second as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rss_fields_are_independent_and_only_current_samples_are_exposed() {
        let memory = ProcessMemory::from_status("VmHWM:\t50 kB\nVmRSS:\t42 kB\nRssAnon:\t40\tkB\n");
        assert_eq!(memory.rss_bytes, Some(42 * 1024));
        assert_eq!(memory.rss_anon_bytes, Some(40 * 1024));

        for status in [
            "",
            "VmRSS: -1 kB",
            "VmRSS: 10 bytes",
            "VmRSS: 10 kB junk",
            "VmRSS: 18446744073709551615 kB",
        ] {
            let memory = ProcessMemory::from_status(&format!("{status}\nRssAnon: 0 kB\n"));
            let mut exposition = String::new();
            append_memory(&mut exposition, memory);
            assert!(!exposition.contains("zinder_process_rss_bytes"));
            assert!(
                exposition
                    .lines()
                    .any(|line| line == "zinder_process_rss_anon_bytes 0")
            );
        }
        let mut exposition = String::new();
        append_memory(&mut exposition, ProcessMemory::default());
        assert_eq!(exposition, "");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn start_time_uses_the_kernel_start_tick_for_each_process() {
        let fields = "1 (worker ) 2) S 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21";
        let system_stat = "intr 1234\nbtime 1000000\nprocesses 99\n";
        let first = format!("{fields} 256 1000");
        let second = format!("{fields} 257 1000");
        assert_eq!(parse_start_time(&first, system_stat, 64), Some(1_000_004.0));
        assert_eq!(
            parse_start_time(&second, system_stat, 64),
            Some(1_000_004.015_625)
        );
        assert_eq!(parse_start_time(fields, system_stat, 64), None);
        assert_eq!(parse_start_time(&first, "", 64), None);
        assert_eq!(parse_start_time(&first, system_stat, 0), None);
    }
}
