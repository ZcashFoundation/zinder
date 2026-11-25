//! Container memory budget detection for cgroup v2 deployments.
//!
//! Shared by ingest and query so both processes can derive cgroup-aware
//! defaults without duplicating the detection logic.

use std::{
    fs,
    path::{Component, Path, PathBuf},
};

const CGROUP_ROOT: &str = "/sys/fs/cgroup";
const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";

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

/// Derives the canonical-reader block-cache size from the container memory budget.
///
/// Rule: `min(budget / 8, 512 MiB)`, floored at 128 MiB when no cgroup limit is
/// detectable. Direct I/O stays enabled on readers because the ADR-0005
/// store-size-independence invariant applies to readers just as it does to
/// writers: without direct I/O the OS page cache grows proportionally to the
/// CF on-disk size, defeating the purpose of a bounded block cache.
#[must_use]
pub fn canonical_reader_block_cache_bytes() -> u64 {
    canonical_reader_block_cache_bytes_for(container_memory_budget_bytes())
}

fn canonical_reader_block_cache_bytes_for(budget: Option<u64>) -> u64 {
    const FLOOR: u64 = 128 * MIB;
    const CAP: u64 = 512 * MIB;

    budget.map_or(FLOOR, |bytes| (bytes / 8).clamp(FLOOR, CAP))
}

/// Derives the canonical reader's open-file budget from its default block cache.
///
/// Each full 128 MiB allows 64 open files, with a floor of 128 files.
/// The 512 MiB cache ceiling gives a maximum default of 256 files.
#[must_use]
pub fn canonical_reader_max_open_files() -> i32 {
    canonical_reader_max_open_files_for(container_memory_budget_bytes())
}

fn canonical_reader_max_open_files_for(budget: Option<u64>) -> i32 {
    const FLOOR: i32 = 128;
    const CAP: i32 = 1024;
    const PER_128_MIB: i32 = 64;

    let files = budget
        .and_then(|bytes| {
            let cache = (bytes / 8).min(512 * MIB);
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

const MIB: u64 = 1024 * 1024;

const DEFAULT_WALLET_OUTPOINT_SORT_MEMORY_BYTES: u64 = 4 * 1024 * MIB;
const MIN_WALLET_OUTPOINT_SORT_MEMORY_BYTES: u64 = 64 * MIB;
const DEFAULT_WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER: u64 = 1024 * MIB;
const MIN_WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER: u64 = 32 * MIB;
const DEFAULT_WALLET_REORG_UNDO_MEMORY_BYTES: u64 = 512 * MIB;
const MIN_WALLET_REORG_UNDO_MEMORY_BYTES: u64 = 16 * MIB;

fn scaled_wallet_budget_for(
    budget_bytes: Option<u64>,
    default_bytes: u64,
    percent: u64,
    min_floor_bytes: u64,
) -> u64 {
    let Some(limit) = budget_bytes else {
        return default_bytes;
    };
    let bounded_limit = limit.max(1);
    let scaled = u64::try_from(u128::from(bounded_limit).saturating_mul(u128::from(percent)) / 100)
        .unwrap_or(default_bytes);
    let safe_floor = min_floor_bytes.min((bounded_limit / 4).max(1));
    scaled.clamp(safe_floor, default_bytes)
}

/// Returns the default wallet-builder outpoint sort memory budget scaled from a
/// detected container memory limit, or the full ADR-0005 default without one.
#[must_use]
pub fn wallet_build_outpoint_sort_memory_bytes() -> u64 {
    wallet_build_outpoint_sort_memory_bytes_for(container_memory_budget_bytes())
}

/// Pure testable scaling helper for [`wallet_build_outpoint_sort_memory_bytes`].
#[must_use]
pub fn wallet_build_outpoint_sort_memory_bytes_for(budget_bytes: Option<u64>) -> u64 {
    scaled_wallet_budget_for(
        budget_bytes,
        DEFAULT_WALLET_OUTPOINT_SORT_MEMORY_BYTES,
        40,
        MIN_WALLET_OUTPOINT_SORT_MEMORY_BYTES,
    )
}

/// Returns the default wallet-builder secondary sort memory budget per sorter
/// scaled from a detected container memory limit.
#[must_use]
pub fn wallet_build_secondary_sort_memory_bytes_per_sorter() -> u64 {
    wallet_build_secondary_sort_memory_bytes_per_sorter_for(container_memory_budget_bytes())
}

/// Pure testable scaling helper for
/// [`wallet_build_secondary_sort_memory_bytes_per_sorter`].
#[must_use]
pub fn wallet_build_secondary_sort_memory_bytes_per_sorter_for(budget_bytes: Option<u64>) -> u64 {
    scaled_wallet_budget_for(
        budget_bytes,
        DEFAULT_WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER,
        10,
        MIN_WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER,
    )
}

/// Returns the default wallet-builder reorg-undo accounted memory budget scaled
/// from a detected container memory limit.
#[must_use]
pub fn wallet_build_reorg_undo_memory_bytes() -> u64 {
    wallet_build_reorg_undo_memory_bytes_for(container_memory_budget_bytes())
}

/// Pure testable scaling helper for [`wallet_build_reorg_undo_memory_bytes`].
#[must_use]
pub fn wallet_build_reorg_undo_memory_bytes_for(budget_bytes: Option<u64>) -> u64 {
    scaled_wallet_budget_for(
        budget_bytes,
        DEFAULT_WALLET_REORG_UNDO_MEMORY_BYTES,
        5,
        MIN_WALLET_REORG_UNDO_MEMORY_BYTES,
    )
}

#[cfg(test)]
mod wallet_build_budget_tests {
    use super::*;

    const GIB: u64 = 1024 * MIB;

    #[test]
    fn unlimited_container_uses_full_adr_0005_wallet_build_defaults() {
        assert_eq!(wallet_build_outpoint_sort_memory_bytes_for(None), 4 * GIB);
        assert_eq!(
            wallet_build_secondary_sort_memory_bytes_per_sorter_for(None),
            GIB
        );
        assert_eq!(wallet_build_reorg_undo_memory_bytes_for(None), 512 * MIB);
    }

    #[test]
    fn wallet_build_budgets_leave_container_headroom_for_two_six_and_ten_gib() {
        for container in [2 * GIB, 6 * GIB, 10 * GIB, 64 * GIB] {
            let outpoint = wallet_build_outpoint_sort_memory_bytes_for(Some(container));
            let secondary =
                wallet_build_secondary_sort_memory_bytes_per_sorter_for(Some(container));
            let undo = wallet_build_reorg_undo_memory_bytes_for(Some(container));
            let canonical_cache = canonical_reader_block_cache_bytes_for(Some(container));
            let recycled_address_tx = secondary.max(outpoint / 2);

            assert!(outpoint >= MIN_WALLET_OUTPOINT_SORT_MEMORY_BYTES);
            assert!(outpoint <= DEFAULT_WALLET_OUTPOINT_SORT_MEMORY_BYTES);
            assert!(secondary >= MIN_WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER);
            assert!(secondary <= DEFAULT_WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER);
            assert!(undo >= MIN_WALLET_REORG_UNDO_MEMORY_BYTES);
            assert!(undo <= DEFAULT_WALLET_REORG_UNDO_MEMORY_BYTES);

            // Phase 1 (scan+sort+undo) and Phase 2 (dual secondary sort+undo) must
            // both leave headroom for canonical cache, wallet 256 MiB cache,
            // hashing (<= 33 MiB), SST queues (<= 16 MiB), and RocksDB buffers.
            let scan_working_set = outpoint + undo + canonical_cache;
            let drain_working_set = secondary + recycled_address_tx + undo + canonical_cache;
            assert!(
                scan_working_set < container * 3 / 4,
                "container={container}"
            );
            assert!(
                drain_working_set < container * 3 / 4,
                "container={container}"
            );
        }

        // 10 GiB target matches historical ADR-0005 defaults exactly.
        assert_eq!(
            wallet_build_outpoint_sort_memory_bytes_for(Some(10 * GIB)),
            4 * GIB
        );
        assert_eq!(
            wallet_build_secondary_sort_memory_bytes_per_sorter_for(Some(10 * GIB)),
            GIB
        );
        assert_eq!(
            wallet_build_reorg_undo_memory_bytes_for(Some(10 * GIB)),
            512 * MIB
        );
    }

    #[test]
    fn tiny_containers_clamp_safely_without_zero_or_overflow() {
        for tiny in [0_u64, 1, 16 * MIB, 64 * MIB] {
            let outpoint = wallet_build_outpoint_sort_memory_bytes_for(Some(tiny));
            let secondary = wallet_build_secondary_sort_memory_bytes_per_sorter_for(Some(tiny));
            let undo = wallet_build_reorg_undo_memory_bytes_for(Some(tiny));
            assert!(outpoint >= 1);
            assert!(secondary >= 1);
            assert!(undo >= 1);
            if tiny >= 16 * MIB {
                assert!(outpoint + undo < tiny);
            }
        }
    }
}

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
    fn block_cache_is_floored_at_128_mib_when_cgroup_absent() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let absent_budget = cgroup_memory_budget_bytes_in(
            &tempdir.path().join("missing-cgroup"),
            &tempdir.path().join("missing-proc-cgroup"),
        );
        assert_eq!(absent_budget, None);
        assert_eq!(
            canonical_reader_block_cache_bytes_for(absent_budget),
            128 * MIB
        );
        Ok(())
    }

    #[test]
    fn block_cache_is_eighth_of_budget_up_to_512_mib() {
        let large_budget = 4 * 1024 * MIB; // 4 GiB
        assert_eq!(
            canonical_reader_block_cache_bytes_for(Some(large_budget)),
            512 * MIB
        );

        let small_budget = 2 * 1024 * MIB; // 2 GiB
        assert_eq!(
            canonical_reader_block_cache_bytes_for(Some(small_budget)),
            256 * MIB
        );
    }

    #[test]
    fn open_files_floor_is_128_when_cgroup_absent() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let absent_budget = cgroup_memory_budget_bytes_in(
            &tempdir.path().join("missing-cgroup"),
            &tempdir.path().join("missing-proc-cgroup"),
        );
        assert_eq!(canonical_reader_max_open_files_for(absent_budget), 128);
        Ok(())
    }

    #[test]
    fn open_files_rises_with_budget() {
        // 4 GiB budget => 512 MiB cache => 4 * 64 = 256 files
        assert_eq!(
            canonical_reader_max_open_files_for(Some(4 * 1024 * MIB)),
            256
        );
        // 1 GiB budget => 128 MiB cache => 1 * 64 = 64, floored at 128
        assert_eq!(canonical_reader_max_open_files_for(Some(1024 * MIB)), 128);
    }
}
