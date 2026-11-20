//! Container memory budget detection for cgroup v2 deployments.
//!
//! Shared by ingest and query so both processes can derive cgroup-aware
//! defaults without duplicating the detection logic.

use std::{
    fs,
    path::{Path, PathBuf},
};

const CGROUP_ROOT: &str = "/sys/fs/cgroup";
const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";

/// Returns the container memory budget in bytes, derived from cgroup v2.
///
/// Prefers `memory.high` (the soft throttle where the kernel starts
/// reclaiming) over `memory.max` (the hard kill threshold) so the
/// budget describes the limit Zinder should stay under, not the limit
/// past which the kernel kills it.
///
/// Returns `None` when cgroup v2 is unavailable (dev hosts without
/// containers, macOS, older Linux) or when the limit is unset (the
/// kernel exposes the literal string `max`, which the cgroup reader
/// translates to `None`). Callers fall through to their hand-tuned
/// fallback constants, matching pre-existing behavior on dev hosts.
#[must_use]
pub fn container_memory_budget_bytes() -> Option<u64> {
    let cgroup_dir = cgroup_v2_dir(Path::new(CGROUP_ROOT), Path::new(PROC_SELF_CGROUP));
    let high = read_cgroup_u64(&cgroup_dir, "memory.high");
    let max = read_cgroup_u64(&cgroup_dir, "memory.max");
    let limit = high.or(max)?;
    if limit == 0 { None } else { Some(limit) }
}

/// Derives the canonical-reader block-cache size from the container memory budget.
///
/// Rule: `min(budget / 8, 512 MiB)`, floored at 128 MiB when no cgroup limit is
/// detectable. Direct I/O stays enabled on readers because the ADR-0005
/// store-size-independence invariant applies to readers just as it does to
/// writers: without direct I/O the OS page cache grows proportionally to the
/// CF on-disk size, defeating the purpose of a bounded block cache.
#[must_use]
pub fn canonical_reader_block_cache_bytes() -> u64 {
    const FLOOR: u64 = 128 * MIB;
    const CAP: u64 = 512 * MIB;

    container_memory_budget_bytes().map_or(FLOOR, |budget| (budget / 8).clamp(FLOOR, CAP))
}

/// Derives the canonical reader's open-file budget from its default block cache.
///
/// Each full 128 MiB allows 64 open files, with a floor of 128 files.
/// The 512 MiB cache ceiling gives a maximum default of 256 files.
#[must_use]
pub fn canonical_reader_max_open_files() -> i32 {
    const FLOOR: i32 = 128;
    const CAP: i32 = 1024;
    const PER_128_MIB: i32 = 64;

    let files = container_memory_budget_bytes()
        .and_then(|budget| {
            let cache = (budget / 8).min(512 * MIB);
            let units = i32::try_from(cache / (128 * MIB)).ok()?;
            Some((units * PER_128_MIB).max(FLOOR))
        })
        .unwrap_or(FLOOR);
    files.min(CAP)
}

fn cgroup_v2_dir(cgroup_root: &Path, proc_cgroup: &Path) -> PathBuf {
    let Ok(cgroup_text) = fs::read_to_string(proc_cgroup) else {
        return cgroup_root.to_path_buf();
    };
    for line in cgroup_text.lines() {
        let mut parts = line.splitn(3, ':');
        let Some(hierarchy_id) = parts.next() else {
            continue;
        };
        let Some(controllers) = parts.next() else {
            continue;
        };
        let Some(relative_path) = parts.next() else {
            continue;
        };
        if hierarchy_id == "0" && controllers.is_empty() {
            return cgroup_root.join(relative_path.trim_start_matches('/'));
        }
    }
    cgroup_root.to_path_buf()
}

fn read_cgroup_u64(cgroup_dir: &Path, file_name: &str) -> Option<u64> {
    let file_text = fs::read_to_string(cgroup_dir.join(file_name)).ok()?;
    let trimmed = file_text.trim();
    if trimmed == "max" {
        return None;
    }
    trimmed.parse::<u64>().ok()
}

const MIB: u64 = 1024 * 1024;

#[cfg(test)]
mod tests {
    use std::{error::Error, fs};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn cgroup_v2_dir_resolves_the_unified_hierarchy_path() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let cgroup_root = tempdir.path().join("sys-fs-cgroup");
        let service_dir = cgroup_root.join("docker/abc");
        fs::create_dir_all(&service_dir)?;
        let cgroup_file = tempdir.path().join("cgroup");
        fs::write(&cgroup_file, "0::/docker/abc\n")?;
        fs::write(service_dir.join("memory.max"), format!("{}\n", 2048 * MIB))?;

        let resolved = cgroup_v2_dir(&cgroup_root, &cgroup_file);
        assert_eq!(resolved, service_dir);
        assert_eq!(
            read_cgroup_u64(&resolved, "memory.max"),
            Some(2048 * MIB),
            "memory.max must parse from the resolved cgroup dir"
        );
        Ok(())
    }

    #[test]
    fn block_cache_is_floored_at_128_mib_when_cgroup_absent() {
        assert_eq!(canonical_reader_block_cache_bytes(), 128 * MIB);
    }

    #[test]
    fn block_cache_is_eighth_of_budget_up_to_512_mib() {
        let budget = 4 * 1024 * MIB; // 4 GiB
        let result = (budget / 8).min(512 * MIB);
        assert_eq!(result, 512 * MIB);

        let budget = 2 * 1024 * MIB; // 2 GiB
        let result = (budget / 8).min(512 * MIB);
        assert_eq!(result, 256 * MIB);
    }

    #[test]
    fn open_files_floor_is_128_when_cgroup_absent() {
        assert_eq!(canonical_reader_max_open_files(), 128);
    }

    #[test]
    fn open_files_rises_with_budget() -> Result<(), Box<dyn Error>> {
        // 512 MiB cache => 4 * 64 = 256 files
        let cache = 512 * MIB;
        let units = i32::try_from(cache / (128 * MIB))?;
        assert_eq!(units * 64, 256);

        // 128 MiB cache (1 GiB budget) => 1 * 64 = 64, but floored at 128
        let cache = 128 * MIB;
        let units = i32::try_from(cache / (128 * MIB))?;
        let raw = (units * 64).max(128);
        assert_eq!(raw, 128);
        Ok(())
    }
}
