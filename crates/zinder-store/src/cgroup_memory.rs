//! Linux cgroup v2 memory limits and usage for storage and service budgets.

use std::{
    fs,
    path::{Component, Path, PathBuf},
};

const CGROUP_ROOT: &str = "/sys/fs/cgroup";
const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";

/// Point-in-time Linux cgroup v2 memory usage and page-cache snapshot for the
/// most-pressured cgroup in the process's ancestor chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CgroupMemorySnapshot {
    /// Current memory charged to the selected cgroup (`memory.current`).
    pub current_bytes: u64,
    /// Effective finite limit (`min(memory.high, memory.max)`) of the selected cgroup.
    pub limit_bytes: u64,
    /// File-backed page cache charged to the selected cgroup (`file` in `memory.stat`).
    pub file_cache_bytes: Option<u64>,
}

/// Returns a cgroup v2 memory snapshot for the most-pressured ancestor cgroup.
///
/// Walks the process's ancestor chain and selects the cgroup with the highest
/// `current / limit` ratio (breaking ties by tighter limit) where both a finite
/// limit and `memory.current` are readable. All three fields in the returned
/// snapshot come from that same cgroup so callers can compare usage against its
/// matching limit. For the tightest effective memory budget across all
/// ancestors, use [`container_memory_budget_bytes`].
///
/// Returns `None` outside cgroup v2 or when no finite ancestor limit exists.
#[must_use]
pub fn container_memory_snapshot() -> Option<CgroupMemorySnapshot> {
    cgroup_memory_snapshot_in(Path::new(CGROUP_ROOT), Path::new(PROC_SELF_CGROUP))
}

fn cgroup_memory_snapshot_in(
    cgroup_root: &Path,
    proc_cgroup: &Path,
) -> Option<CgroupMemorySnapshot> {
    let cgroup_dir = cgroup_v2_dir(cgroup_root, proc_cgroup);
    let mut most_pressured: Option<CgroupMemorySnapshot> = None;
    for ancestor in cgroup_dir.ancestors() {
        if !ancestor.starts_with(cgroup_root) {
            break;
        }
        let high = read_cgroup_u64(ancestor, "memory.high");
        let max = read_cgroup_u64(ancestor, "memory.max");
        let Some(limit_bytes) = (match (high, max) {
            (Some(high_bytes), Some(max_bytes)) => Some(high_bytes.min(max_bytes)),
            (high_bytes, max_bytes) => high_bytes.or(max_bytes),
        }) else {
            continue;
        };
        let Some(current_bytes) = read_cgroup_u64(ancestor, "memory.current") else {
            continue;
        };
        let candidate = CgroupMemorySnapshot {
            current_bytes,
            limit_bytes,
            file_cache_bytes: read_cgroup_stat_u64(ancestor, "memory.stat", "file"),
        };
        let replaces_best = most_pressured.is_none_or(|best| {
            let candidate_lhs = u128::from(candidate.current_bytes) * u128::from(best.limit_bytes);
            let best_rhs = u128::from(best.current_bytes) * u128::from(candidate.limit_bytes);
            candidate_lhs > best_rhs
                || (candidate_lhs == best_rhs && candidate.limit_bytes < best.limit_bytes)
        });
        if replaces_best {
            most_pressured = Some(candidate);
        }
    }
    most_pressured
}

fn read_cgroup_stat_u64(dir: &Path, file_name: &str, key_name: &str) -> Option<u64> {
    let contents = fs::read_to_string(dir.join(file_name)).ok()?;
    for line in contents.lines() {
        let mut fields = line.split_ascii_whitespace();
        let Some(field_key) = fields.next() else {
            continue;
        };
        if field_key == key_name {
            return fields.next()?.parse::<u64>().ok();
        }
    }
    None
}

/// Returns the container memory budget in bytes, derived from cgroup v2.
///
/// Walks ancestors from the process's cgroup v2 directory up to
/// `/sys/fs/cgroup` inclusive so limits enforced on parent slices (such as
/// a systemd service slice whose own leaf reports `max`) are detected too.
/// Takes the tightest finite (`!= "max"`, `> 0`) limit across `memory.high`
/// and `memory.max`, preserving `memory.high` whenever it is tighter than
/// `memory.max` so the budget stays below soft kernel reclaim thresholds and
/// hard container OOM limits alike.
///
/// Returns `None` when cgroup v2 is unavailable (dev hosts without
/// containers, macOS, older Linux) or when every ancestor's limit is unset
/// (`max` or `0`). Callers fall through to their hand-tuned fallback
/// constants, matching pre-existing behavior on dev hosts.
#[must_use]
pub fn container_memory_budget_bytes() -> Option<u64> {
    cgroup_memory_budget_bytes_in(Path::new(CGROUP_ROOT), Path::new(PROC_SELF_CGROUP))
}

fn cgroup_memory_budget_bytes_in(cgroup_root: &Path, proc_cgroup: &Path) -> Option<u64> {
    let cgroup_dir = cgroup_v2_dir(cgroup_root, proc_cgroup);
    let mut min_high: Option<u64> = None;
    let mut min_max: Option<u64> = None;
    for ancestor in cgroup_dir.ancestors() {
        if !ancestor.starts_with(cgroup_root) {
            break;
        }
        if let Some(high) = read_cgroup_u64(ancestor, "memory.high") {
            min_high = Some(min_high.map_or(high, |current| current.min(high)));
        }
        if let Some(max) = read_cgroup_u64(ancestor, "memory.max") {
            min_max = Some(min_max.map_or(max, |current| current.min(max)));
        }
    }
    match (min_high, min_max) {
        (Some(high), Some(max)) => Some(high.min(max)),
        (high, max) => high.or(max),
    }
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
            return resolve_cgroup_relative_dir(cgroup_root, relative_path);
        }
    }
    cgroup_root.to_path_buf()
}

fn resolve_cgroup_relative_dir(cgroup_root: &Path, relative_path: &str) -> PathBuf {
    let mut resolved = cgroup_root.to_path_buf();
    for component in Path::new(relative_path.trim_start_matches('/')).components() {
        match component {
            Component::Normal(segment) => resolved.push(segment),
            Component::ParentDir => {
                if resolved != cgroup_root {
                    let _was_popped = resolved.pop();
                }
            }
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
        }
    }
    resolved
}

fn read_cgroup_u64(cgroup_dir: &Path, file_name: &str) -> Option<u64> {
    let file_text = fs::read_to_string(cgroup_dir.join(file_name)).ok()?;
    let trimmed = file_text.trim();
    if trimmed == "max" {
        return None;
    }
    trimmed.parse::<u64>().ok().filter(|bytes| *bytes > 0)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use tempfile::tempdir;

    use super::*;

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn cgroup_budget_is_absent_when_cgroup_is_unavailable() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        assert_eq!(
            cgroup_memory_budget_bytes_in(
                &tempdir.path().join("sys"),
                &tempdir.path().join("proc")
            ),
            None
        );
        Ok(())
    }

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
    fn cgroup_budget_inherits_ancestor_limit_when_leaf_is_max() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let cgroup_root = tempdir.path().join("sys-fs-cgroup");
        let slice_dir = cgroup_root.join("zbmidx.slice");
        let service_dir = slice_dir.join("zbm-projector.service");
        fs::create_dir_all(&service_dir)?;
        let cgroup_file = tempdir.path().join("cgroup");
        fs::write(&cgroup_file, "0::/zbmidx.slice/zbm-projector.service\n")?;
        let slice_limit = 64 * 1024 * MIB;
        fs::write(slice_dir.join("memory.max"), format!("{slice_limit}\n"))?;
        fs::write(slice_dir.join("memory.high"), "max\n")?;
        fs::write(service_dir.join("memory.max"), "max\n")?;
        fs::write(service_dir.join("memory.high"), "max\n")?;

        assert_eq!(
            cgroup_memory_budget_bytes_in(&cgroup_root, &cgroup_file),
            Some(slice_limit)
        );
        Ok(())
    }

    #[test]
    fn cgroup_budget_picks_tightest_ancestor_limit_across_high_and_max()
    -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let cgroup_root = tempdir.path().join("sys-fs-cgroup");
        let slice_dir = cgroup_root.join("zbmidx.slice");
        let service_dir = slice_dir.join("zbm-projector.service");
        fs::create_dir_all(&service_dir)?;
        let cgroup_file = tempdir.path().join("cgroup");
        fs::write(&cgroup_file, "0::/zbmidx.slice/zbm-projector.service\n")?;

        fs::write(
            cgroup_root.join("memory.max"),
            format!("{}\n", 128 * 1024 * MIB),
        )?;
        fs::write(
            slice_dir.join("memory.max"),
            format!("{}\n", 64 * 1024 * MIB),
        )?;
        fs::write(
            slice_dir.join("memory.high"),
            format!("{}\n", 56 * 1024 * MIB),
        )?;
        fs::write(
            service_dir.join("memory.max"),
            format!("{}\n", 96 * 1024 * MIB),
        )?;
        fs::write(
            service_dir.join("memory.high"),
            format!("{}\n", 72 * 1024 * MIB),
        )?;

        assert_eq!(
            cgroup_memory_budget_bytes_in(&cgroup_root, &cgroup_file),
            Some(56 * 1024 * MIB)
        );

        // When a parent slice imposes a tighter memory.max than the leaf's memory.high,
        // the kernel enforces the parent hard limit first.
        fs::write(slice_dir.join("memory.high"), "max\n")?;
        fs::write(
            slice_dir.join("memory.max"),
            format!("{}\n", 32 * 1024 * MIB),
        )?;
        assert_eq!(
            cgroup_memory_budget_bytes_in(&cgroup_root, &cgroup_file),
            Some(32 * 1024 * MIB)
        );
        Ok(())
    }

    #[test]
    fn cgroup_budget_stops_at_root_and_ignores_unset_or_zero() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let cgroup_root = tempdir.path().join("sys-fs-cgroup");
        let service_dir = cgroup_root.join("zbmidx.slice/zbm-projector.service");
        fs::create_dir_all(&service_dir)?;
        // A file outside cgroup_root must never be consulted even with `..` in proc_cgroup.
        fs::write(
            tempdir.path().join("memory.max"),
            format!("{}\n", 512 * MIB),
        )?;
        fs::write(cgroup_root.join("memory.max"), "max\n")?;
        fs::write(service_dir.join("memory.max"), "0\n")?;
        fs::write(service_dir.join("memory.high"), "max\n")?;

        let cgroup_file = tempdir.path().join("cgroup");
        fs::write(&cgroup_file, "0::/zbmidx.slice/zbm-projector.service\n")?;
        assert_eq!(
            cgroup_memory_budget_bytes_in(&cgroup_root, &cgroup_file),
            None
        );

        fs::write(&cgroup_file, "0::/../../../etc\n")?;
        assert_eq!(
            cgroup_v2_dir(&cgroup_root, &cgroup_file),
            cgroup_root.join("etc")
        );
        assert_eq!(
            cgroup_memory_budget_bytes_in(&cgroup_root, &cgroup_file),
            None
        );

        fs::write(&cgroup_file, "0::/zbmidx.slice/../../..\n")?;
        assert_eq!(cgroup_v2_dir(&cgroup_root, &cgroup_file), cgroup_root);
        assert_eq!(
            cgroup_memory_budget_bytes_in(&cgroup_root, &cgroup_file),
            None
        );
        Ok(())
    }

    #[test]
    fn memory_snapshot_reads_current_limit_and_file_cache() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let cgroup_root = tempdir.path().join("cgroup");
        let leaf = cgroup_root.join("system.slice").join("zinder.service");
        fs::create_dir_all(&leaf)?;
        fs::write(leaf.join("memory.max"), "4294967296\n")?;
        fs::write(leaf.join("memory.current"), "3900000000\n")?;
        fs::write(
            leaf.join("memory.stat"),
            "anon 400000000\nfile 3500000000\nkernel 50000000\n",
        )?;
        let proc_cgroup = tempdir.path().join("proc-cgroup");
        fs::write(&proc_cgroup, "0::/system.slice/zinder.service\n")?;

        let snapshot = cgroup_memory_snapshot_in(&cgroup_root, &proc_cgroup)
            .ok_or("expected cgroup memory snapshot")?;
        assert_eq!(snapshot.limit_bytes, 4_294_967_296);
        assert_eq!(snapshot.current_bytes, 3_900_000_000);
        assert_eq!(snapshot.file_cache_bytes, Some(3_500_000_000));
        Ok(())
    }

    #[test]
    fn memory_snapshot_selects_most_pressured_ancestor_when_parent_slice_is_tighter()
    -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let cgroup_root = tempdir.path().join("cgroup");
        let parent = cgroup_root.join("system.slice");
        let leaf = parent.join("zinder.service");
        fs::create_dir_all(&leaf)?;
        // Parent slice holds 7.5 GiB / 8 GiB (93.75% utilized due to siblings).
        fs::write(parent.join("memory.max"), format!("{}\n", 8 * 1024 * MIB))?;
        fs::write(parent.join("memory.current"), format!("{}\n", 7_680 * MIB))?;
        fs::write(
            parent.join("memory.stat"),
            format!("file {}\n", 6_144 * MIB),
        )?;
        // Leaf has its own 8 GiB limit but only 2 GiB usage (25%).
        fs::write(leaf.join("memory.max"), format!("{}\n", 8 * 1024 * MIB))?;
        fs::write(leaf.join("memory.current"), format!("{}\n", 2 * 1024 * MIB))?;
        fs::write(leaf.join("memory.stat"), format!("file {}\n", 1_536 * MIB))?;
        let proc_cgroup = tempdir.path().join("proc-cgroup");
        fs::write(&proc_cgroup, "0::/system.slice/zinder.service\n")?;

        let snapshot = cgroup_memory_snapshot_in(&cgroup_root, &proc_cgroup)
            .ok_or("expected parent-slice memory snapshot")?;
        assert_eq!(snapshot.limit_bytes, 8 * 1024 * MIB);
        assert_eq!(snapshot.current_bytes, 7_680 * MIB);
        assert_eq!(snapshot.file_cache_bytes, Some(6_144 * MIB));
        Ok(())
    }
}
