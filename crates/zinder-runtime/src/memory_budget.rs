//! Container-aware defaults for canonical readers and wallet construction.

use zinder_store::container_memory_budget_bytes;

/// Derives the canonical-reader block-cache size from the container memory budget.
///
/// Rule: `min(budget / 8, 512 MiB)`, floored at 128 MiB when no cgroup limit is
/// detectable. The budget bounds the `RocksDB` block cache. Canonical settled
/// segments use buffered file reads and have a separate readahead policy.
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
/// Each full 128 MiB allows 512 open files, floored at 1024 files and capped at
/// 2048 files (matching the 512 MiB cache ceiling). Combined with the secondary
/// open path's 4-shard table cache (`table_cache_numshardbits = 2`), the 1024
/// floor provides 254 table-reader slots per shard and the 2048 ceiling
/// provides 510 per shard, retaining both Mainnet (243 SSTs) and Testnet
/// (~710 SSTs) layouts without per-shard eviction. When no cgroup limit is
/// detectable, defaults to 2048 files, matching
/// `RocksDbResourceBudget::canonical_reader_defaults`.
#[must_use]
pub fn canonical_reader_max_open_files() -> i32 {
    canonical_reader_max_open_files_for(container_memory_budget_bytes())
}

fn canonical_reader_max_open_files_for(budget: Option<u64>) -> i32 {
    const FLOOR: i32 = 1024;
    const CAP: i32 = 2048;
    const PER_128_MIB: i32 = 512;

    let files = budget
        .and_then(|bytes| {
            let cache = (bytes / 8).min(512 * MIB);
            let units = i32::try_from(cache / (128 * MIB)).ok()?;
            Some((units * PER_128_MIB).max(FLOOR))
        })
        .unwrap_or(CAP);
    files.min(CAP)
}

const MIB: u64 = 1024 * 1024;

const DEFAULT_WALLET_OUTPOINT_SORT_MEMORY_BYTES: u64 = 4 * 1024 * MIB;
const MIN_WALLET_OUTPOINT_SORT_MEMORY_BYTES: u64 = 64 * MIB;
const DEFAULT_WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER: u64 = 1024 * MIB;
const MIN_WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER: u64 = 32 * MIB;
const DEFAULT_WALLET_REORG_UNDO_MEMORY_BYTES: u64 = 512 * MIB;
const MIN_WALLET_REORG_UNDO_MEMORY_BYTES: u64 = 16 * MIB;

#[derive(Clone, Copy)]
struct WalletBudgetScale {
    default_bytes: u64,
    percent: u64,
    min_floor_bytes: u64,
    tiny_limit_divisor: u64,
}

fn scaled_wallet_budget_for(budget_bytes: Option<u64>, scale: WalletBudgetScale) -> u64 {
    let Some(limit) = budget_bytes else {
        return scale.default_bytes;
    };
    let bounded_limit = limit.max(1);
    let scaled =
        u64::try_from(u128::from(bounded_limit).saturating_mul(u128::from(scale.percent)) / 100)
            .unwrap_or(scale.default_bytes);
    let safe_floor = scale
        .min_floor_bytes
        .min((bounded_limit / scale.tiny_limit_divisor).max(1));
    scaled.clamp(safe_floor, scale.default_bytes)
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
        WalletBudgetScale {
            default_bytes: DEFAULT_WALLET_OUTPOINT_SORT_MEMORY_BYTES,
            percent: 40,
            min_floor_bytes: MIN_WALLET_OUTPOINT_SORT_MEMORY_BYTES,
            tiny_limit_divisor: 2,
        },
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
        WalletBudgetScale {
            default_bytes: DEFAULT_WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER,
            percent: 10,
            min_floor_bytes: MIN_WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER,
            tiny_limit_divisor: 8,
        },
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
        WalletBudgetScale {
            default_bytes: DEFAULT_WALLET_REORG_UNDO_MEMORY_BYTES,
            percent: 5,
            min_floor_bytes: MIN_WALLET_REORG_UNDO_MEMORY_BYTES,
            tiny_limit_divisor: 16,
        },
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
                assert!(secondary + (outpoint / 2).max(secondary) + undo < tiny);
            }
        }

        // Exact documented divisors around the 128 MiB / 256 MiB / 512 MiB thresholds.
        assert_eq!(
            wallet_build_outpoint_sort_memory_bytes_for(Some(64 * MIB)),
            32 * MIB
        );
        assert_eq!(
            wallet_build_secondary_sort_memory_bytes_per_sorter_for(Some(64 * MIB)),
            8 * MIB
        );
        assert_eq!(
            wallet_build_reorg_undo_memory_bytes_for(Some(64 * MIB)),
            4 * MIB
        );

        assert_eq!(
            wallet_build_outpoint_sort_memory_bytes_for(Some(128 * MIB)),
            64 * MIB
        );
        assert_eq!(
            wallet_build_secondary_sort_memory_bytes_per_sorter_for(Some(128 * MIB)),
            16 * MIB
        );
        assert_eq!(
            wallet_build_reorg_undo_memory_bytes_for(Some(128 * MIB)),
            8 * MIB
        );

        assert_eq!(
            wallet_build_outpoint_sort_memory_bytes_for(Some(160 * MIB)),
            64 * MIB
        );
        assert_eq!(
            wallet_build_outpoint_sort_memory_bytes_for(Some(256 * MIB)),
            256 * MIB * 40 / 100
        );
        assert_eq!(
            wallet_build_secondary_sort_memory_bytes_per_sorter_for(Some(256 * MIB)),
            32 * MIB
        );
        assert_eq!(
            wallet_build_reorg_undo_memory_bytes_for(Some(256 * MIB)),
            16 * MIB
        );

        assert_eq!(
            wallet_build_outpoint_sort_memory_bytes_for(Some(320 * MIB)),
            128 * MIB
        );
        assert_eq!(
            wallet_build_secondary_sort_memory_bytes_per_sorter_for(Some(320 * MIB)),
            32 * MIB
        );
        assert_eq!(
            wallet_build_reorg_undo_memory_bytes_for(Some(320 * MIB)),
            16 * MIB
        );

        assert_eq!(
            wallet_build_outpoint_sort_memory_bytes_for(Some(512 * MIB)),
            512 * MIB * 40 / 100
        );
        assert_eq!(
            wallet_build_secondary_sort_memory_bytes_per_sorter_for(Some(512 * MIB)),
            512 * MIB * 10 / 100
        );
        assert_eq!(
            wallet_build_reorg_undo_memory_bytes_for(Some(512 * MIB)),
            512 * MIB * 5 / 100
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_cache_is_floored_at_128_mib_when_cgroup_absent() {
        assert_eq!(canonical_reader_block_cache_bytes_for(None), 128 * MIB);
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
    fn open_files_defaults_to_2048_when_cgroup_absent() {
        assert_eq!(canonical_reader_max_open_files_for(None), 2048);
    }

    #[test]
    fn open_files_rises_with_budget() {
        // 4 GiB budget => 512 MiB cache => 4 * 512 = 2048 files
        assert_eq!(
            canonical_reader_max_open_files_for(Some(4 * 1024 * MIB)),
            2048
        );
        // 3 GiB budget => 384 MiB cache => 3 * 512 = 1536 files
        assert_eq!(
            canonical_reader_max_open_files_for(Some(3 * 1024 * MIB)),
            1536
        );
        // 2 GiB budget => 256 MiB cache => 2 * 512 = 1024 files
        assert_eq!(
            canonical_reader_max_open_files_for(Some(2 * 1024 * MIB)),
            1024
        );
        // 1 GiB budget => 128 MiB cache => 1 * 512 = 512, floored at 1024
        assert_eq!(canonical_reader_max_open_files_for(Some(1024 * MIB)), 1024);
        // 512 MiB budget => 64 MiB raw eighth => 0 * 512 = 0, floored at 1024
        assert_eq!(canonical_reader_max_open_files_for(Some(512 * MIB)), 1024);
    }

    fn max_deterministic_shard_load(
        live_ssts: usize,
        base_file_number: u64,
        shards: usize,
    ) -> usize {
        let mut counts = vec![0_usize; shards];
        for offset in 0..live_ssts {
            let file_number = base_file_number + (offset as u64);
            let mut mixed = file_number.wrapping_add(0x9e37_79b9_7f4a_7c15);
            mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            mixed ^= mixed >> 31;
            let shard_idx = usize::try_from(mixed & ((shards as u64) - 1)).unwrap_or(0);
            if let Some(slot) = counts.get_mut(shard_idx) {
                *slot += 1;
            }
        }
        counts.into_iter().max().unwrap_or(0)
    }

    #[test]
    fn resolved_reader_cap_accommodates_mainnet_243_and_testnet_710_ssts_per_shard() {
        const MAINNET_LIVE_SSTS: usize = 243;
        const TESTNET_LIVE_SSTS: usize = 710;
        // 6-sigma multinomial shard bound for N=710 across 4 shards:
        // ceil(177.5 + 6 * sqrt(710 * 0.25 * 0.75)) = 247.
        const TESTNET_SIX_SIGMA_PER_SHARD: usize = 247;

        let shards = zinder_store::RocksDbResourceBudget::secondary_table_cache_shards();
        assert_eq!(shards, 4);

        for cgroup_budget in [
            None,
            Some(512 * MIB),
            Some(1024 * MIB),
            Some(2 * 1024 * MIB),
            Some(3 * 1024 * MIB),
            Some(4 * 1024 * MIB),
            Some(16 * 1024 * MIB),
        ] {
            let mut budget = zinder_store::RocksDbResourceBudget::canonical_reader_defaults();
            budget.max_open_files = canonical_reader_max_open_files_for(cgroup_budget);
            assert_eq!(budget.validate(), Ok(()));

            let per_shard = budget.secondary_table_cache_per_shard_capacity();
            assert!(per_shard * shards > TESTNET_LIVE_SSTS);
            assert!(
                per_shard >= MAINNET_LIVE_SSTS,
                "cgroup_budget={cgroup_budget:?} per_shard={per_shard}"
            );
            assert!(
                per_shard >= TESTNET_SIX_SIGMA_PER_SHARD,
                "cgroup_budget={cgroup_budget:?} per_shard={per_shard}"
            );
            for (live_ssts, base) in [
                (MAINNET_LIVE_SSTS, 1_u64),
                (MAINNET_LIVE_SSTS, 50_000_u64),
                (TESTNET_LIVE_SSTS, 1_u64),
                (TESTNET_LIVE_SSTS, 250_000_u64),
            ] {
                assert!(max_deterministic_shard_load(live_ssts, base, shards) <= per_shard);
            }
        }
    }
}
