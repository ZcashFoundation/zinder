//! Peak resident-set-size sampling with graceful platform degradation.

use serde::Serialize;

/// Source label used when a peak-RSS reading came from `/proc/self/status`.
pub const PEAK_RSS_SOURCE_PROC_VMHWM: &str = "proc_status_vmhwm";
/// Source label used when no peak-RSS reading is available on this platform.
pub const PEAK_RSS_SOURCE_UNAVAILABLE: &str = "unavailable";

/// A peak resident-set-size reading and where it came from.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct PeakRss {
    /// Peak resident bytes, when the platform exposes a high-water mark.
    pub bytes: Option<u64>,
    /// Origin of the reading.
    pub source: &'static str,
}

/// Reads the process peak resident set size, degrading to `None` off Linux.
#[must_use]
pub fn peak_rss() -> PeakRss {
    #[cfg(target_os = "linux")]
    {
        linux_peak_rss()
    }
    #[cfg(not(target_os = "linux"))]
    {
        PeakRss {
            bytes: None,
            source: PEAK_RSS_SOURCE_UNAVAILABLE,
        }
    }
}

/// Reads the process's current resident set size, or `None` off Linux.
#[must_use]
pub fn current_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        proc_status_bytes("VmRSS:")
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Resets the process peak resident set size to its current size, so a later
/// [`peak_rss`] covers only what follows. Returns whether the reset happened;
/// it needs Linux 4.0 or later.
#[must_use]
pub fn reset_peak_rss() -> bool {
    #[cfg(target_os = "linux")]
    {
        std::fs::write("/proc/self/clear_refs", b"5").is_ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

#[cfg(target_os = "linux")]
fn linux_peak_rss() -> PeakRss {
    let bytes = proc_status_bytes("VmHWM:");
    PeakRss {
        bytes,
        source: if bytes.is_some() {
            PEAK_RSS_SOURCE_PROC_VMHWM
        } else {
            PEAK_RSS_SOURCE_UNAVAILABLE
        },
    }
}

/// Reads one kibibyte field of `/proc/self/status`, in bytes.
#[cfg(target_os = "linux")]
fn proc_status_bytes(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix(field)?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()
            .map(|kib| kib.saturating_mul(1024))
    })
}
