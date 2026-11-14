//! Latency summaries and small process helpers.

use std::{fs, path::Path};

use serde::Serialize;

/// Nearest-rank percentiles of one latency population, in seconds.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct LatencySummary {
    pub(crate) count: usize,
    pub(crate) mean: f64,
    pub(crate) p50: f64,
    pub(crate) p90: f64,
    pub(crate) p99: f64,
    pub(crate) p999: f64,
    pub(crate) max: f64,
}

impl LatencySummary {
    pub(crate) fn from_values(values: &[f64]) -> Self {
        if values.is_empty() {
            return Self::default();
        }
        let mut sorted = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        let sum: f64 = sorted.iter().sum();
        Self {
            count: sorted.len(),
            mean: sum / sorted.len() as f64,
            p50: nearest_rank(&sorted, 0.50),
            p90: nearest_rank(&sorted, 0.90),
            p99: nearest_rank(&sorted, 0.99),
            p999: nearest_rank(&sorted, 0.999),
            max: sorted[sorted.len() - 1],
        }
    }
}

/// Nearest-rank percentile: rank = ceil(p * n), 1-based.
pub(crate) fn nearest_rank(sorted: &[f64], percentile: f64) -> f64 {
    let n = sorted.len();
    let rank = ((percentile * n as f64).ceil() as usize).clamp(1, n);
    sorted[rank - 1]
}

/// Current and peak resident set size from `/proc/self/status`.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(crate) struct ProcessMemory {
    pub(crate) rss_bytes: Option<u64>,
    pub(crate) peak_rss_bytes: Option<u64>,
}

pub(crate) fn process_memory() -> ProcessMemory {
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return ProcessMemory::default();
    };
    let field = |name: &str| {
        status.lines().find_map(|line| {
            let rest = line.strip_prefix(name)?;
            let kib = rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<u64>()
                .ok()?;
            Some(kib * 1024)
        })
    };
    ProcessMemory {
        rss_bytes: field("VmRSS:"),
        peak_rss_bytes: field("VmHWM:"),
    }
}

/// Sum and count of `*.sst` files directly inside `dir`.
pub(crate) fn sst_files_on_disk(dir: &Path) -> (u64, u64) {
    let Ok(entries) = fs::read_dir(dir) else {
        return (0, 0);
    };
    let mut bytes = 0;
    let mut count = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "sst")
            && let Ok(metadata) = entry.metadata()
        {
            bytes += metadata.len();
            count += 1;
        }
    }
    (bytes, count)
}

/// Total bytes of all regular files under `dir`, recursively.
pub(crate) fn directory_bytes(dir: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0;
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_dir() {
            total += directory_bytes(&entry.path());
        } else {
            total += metadata.len();
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::{LatencySummary, nearest_rank};

    #[test]
    fn nearest_rank_matches_definition() {
        let values: Vec<f64> = (1..=100).map(f64::from).collect();
        assert!((nearest_rank(&values, 0.99) - 99.0).abs() < f64::EPSILON);
        assert!((nearest_rank(&values, 0.50) - 50.0).abs() < f64::EPSILON);
        let summary = LatencySummary::from_values(&[3.0, 1.0, 2.0]);
        assert!((summary.p50 - 2.0).abs() < f64::EPSILON);
        assert!((summary.max - 3.0).abs() < f64::EPSILON);
    }
}
