//! Integration checks that canonical secondary readers retain open SST table
//! readers across repeated point-lookup passes without steady-state reopens.

use std::{error::Error, path::Path};

use metrics_exporter_prometheus::PrometheusBuilder;
use rust_rocksdb::{CompactOptions, DB, Options, statistics::Ticker};
use tempfile::TempDir;
use zinder_core::{BlockHeight, Network};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreRequirements, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbResourceBudget, SharedSecondaryResources, StoreRole, record_instance_memory_gauges,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture,
    network_upgrade_fixtures::sample_regtest_upgrade_activations,
};

const MAINNET_SCALE_BLOCKS: u32 = 243;
const PREVIOUS_READER_MAX_OPEN_FILES: i32 = 128;
const PREVIOUS_TABLE_CACHE_CAPACITY: usize = 118;
const CGROUP_FLOOR_MAX_OPEN_FILES: i32 = 1024;
const TEST_REORG_WINDOW: u32 = 100;

fn split_family_into_bottom_level_ssts(
    primary_path: &Path,
    family_name: &str,
) -> Result<(), Box<dyn Error>> {
    let families = DB::list_cf(&Options::default(), primary_path)?;
    let db = DB::open_cf(&Options::default(), primary_path, &families)?;
    let handle = db
        .cf_handle(family_name)
        .ok_or("target column family must exist")?;
    db.set_options_cf(
        &handle,
        &[
            ("disable_auto_compactions", "true"),
            ("target_file_size_base", "1"),
            ("target_file_size_is_upper_bound", "true"),
        ],
    )?;
    let rows = db
        .iterator_cf(&handle, rust_rocksdb::IteratorMode::Start)
        .collect::<Result<Vec<_>, _>>()?;
    let mut write_opts = rust_rocksdb::WriteOptions::default();
    write_opts.disable_wal(true);
    for (key, row_bytes) in rows {
        db.put_cf_opt(&handle, key, row_bytes, &write_opts)?;
        db.flush_cf(&handle)?;
    }
    let mut compact_opts = CompactOptions::default();
    compact_opts.set_change_level(true);
    db.compact_range_cf_opt(&handle, None::<&[u8]>, None::<&[u8]>, &compact_opts);
    Ok(())
}

fn run_point_lookup_pass(
    secondary: &RocksDbCanonicalSecondary,
    chain: &ChainFixture,
) -> Result<(), Box<dyn Error>> {
    for raw_height in 1..=MAINNET_SCALE_BLOCKS {
        let height = BlockHeight::new(raw_height);
        let expected = chain
            .block_at(height)
            .ok_or("fixture must contain every constructed height")?;
        let compact = secondary
            .compact_block_at(height)?
            .ok_or("retained compact block must exist")?;
        assert_eq!(compact.block_hash(), expected.hash);
        let header = secondary
            .block_header_at(height)?
            .ok_or("retained block header must exist")?;
        assert_eq!(header.block_hash, expected.hash);
    }
    Ok(())
}

fn parse_metric_sample(rendered: &str, name: &str, required_labels: &[&str]) -> Option<f64> {
    rendered.lines().find_map(|line| {
        if !line.starts_with(name) || line.starts_with('#') {
            return None;
        }
        if !required_labels.iter().all(|label| line.contains(label)) {
            return None;
        }
        line.split_whitespace().last()?.parse::<f64>().ok()
    })
}

fn measure_legacy_128_open_file_churn(
    primary_path: &Path,
    secondary_path: &Path,
    chain: &ChainFixture,
) -> Result<u64, Box<dyn Error>> {
    let mut legacy_budget = RocksDbResourceBudget::canonical_reader_defaults();
    legacy_budget.max_open_files = PREVIOUS_READER_MAX_OPEN_FILES;
    let shared = SharedSecondaryResources::new(legacy_budget);
    let secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        primary_path,
        secondary_path,
        Network::ZcashRegtest,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
        &shared,
    )?;
    run_point_lookup_pass(&secondary, chain)?;
    let opens_pass_1 = secondary.rocksdb_ticker_count(Ticker::NoFileOpens);
    run_point_lookup_pass(&secondary, chain)?;
    let opens_pass_2 = secondary.rocksdb_ticker_count(Ticker::NoFileOpens);
    assert!(
        opens_pass_2 > opens_pass_1,
        "128 max_open_files must churn on >243 SSTs ({opens_pass_1} -> {opens_pass_2})"
    );
    Ok(secondary.instance_memory().table_reader_bytes)
}

fn assert_zero_steady_state_file_opens(
    primary_path: &Path,
    secondary_path: &Path,
    chain: &ChainFixture,
    label: &str,
    budget: RocksDbResourceBudget,
) -> Result<u64, Box<dyn Error>> {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let shared = SharedSecondaryResources::new(budget);
    let secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        primary_path,
        secondary_path,
        Network::ZcashRegtest,
        CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
        CanonicalReorgPolicy::new(TEST_REORG_WINDOW)?,
        &shared,
    )?;
    let live_sst_count = secondary.rocksdb_live_files()?.len();
    assert!(
        live_sst_count > PREVIOUS_TABLE_CACHE_CAPACITY
            && live_sst_count >= MAINNET_SCALE_BLOCKS as usize,
        "store must hold more live SSTs than the previous 118-slot table cache, got {live_sst_count}"
    );

    run_point_lookup_pass(&secondary, chain)?;
    let opens_after_pass_1 = secondary.rocksdb_ticker_count(Ticker::NoFileOpens);
    let data_misses_after_pass_1 = secondary.rocksdb_ticker_count(Ticker::BlockCacheDataMiss);

    run_point_lookup_pass(&secondary, chain)?;
    assert_eq!(
        secondary.rocksdb_ticker_count(Ticker::NoFileOpens),
        opens_after_pass_1,
        "{label}: rocksdb.no.file.opens must stop growing after pass 1 across {live_sst_count} SSTs"
    );

    run_point_lookup_pass(&secondary, chain)?;
    let opens_after_pass_3 = secondary.rocksdb_ticker_count(Ticker::NoFileOpens);
    assert_eq!(
        opens_after_pass_3, opens_after_pass_1,
        "{label}: rocksdb.no.file.opens must stay flat on pass 3 across {live_sst_count} SSTs"
    );
    assert_eq!(
        secondary.rocksdb_ticker_count(Ticker::BlockCacheDataMiss),
        data_misses_after_pass_1,
        "{label}: warm point lookups must hit the shared block cache"
    );

    let memory = secondary.instance_memory();
    metrics::with_local_recorder(&recorder, || {
        shared.record_resource_gauges(StoreRole::CanonicalSecondary);
        record_instance_memory_gauges(StoreRole::CanonicalSecondary, memory);
    });
    let rendered = handle.render();
    let exported_opens = parse_metric_sample(
        &rendered,
        "zinder_store_rocksdb_ticker",
        &[
            "store_role=\"canonical_secondary\"",
            "ticker=\"rocksdb.no.file.opens\"",
        ],
    )
    .ok_or("exported rocksdb.no.file.opens gauge must be present")?;
    let exported_reader_bytes = parse_metric_sample(
        &rendered,
        "zinder_store_table_reader_memory_bytes",
        &["store_role=\"canonical_secondary\""],
    )
    .ok_or("exported zinder_store_table_reader_memory_bytes gauge must be present")?;
    assert!((exported_opens - f64::from(u32::try_from(opens_after_pass_3)?)).abs() < f64::EPSILON);
    assert!(
        (exported_reader_bytes - f64::from(u32::try_from(memory.table_reader_bytes)?)).abs()
            < f64::EPSILON
    );
    Ok(memory.table_reader_bytes)
}

#[test]
fn canonical_secondary_retains_table_readers_without_reopening_files_across_passes()
-> Result<(), Box<dyn Error>> {
    let activations = sample_regtest_upgrade_activations();
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(MAINNET_SCALE_BLOCKS);
    let mut writer =
        CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
    let _fixture_guard = writer.serving_store_fixture()?;
    let primary_path = writer.canonical_primary_path().to_path_buf();
    drop(writer);

    split_family_into_bottom_level_ssts(&primary_path, "compact_block")?;

    let secondary_dirs = TempDir::new()?;
    let legacy_reader_bytes = measure_legacy_128_open_file_churn(
        &primary_path,
        &secondary_dirs.path().join("legacy"),
        &chain,
    )?;

    let mut floor_budget = RocksDbResourceBudget::canonical_reader_defaults();
    floor_budget.max_open_files = CGROUP_FLOOR_MAX_OPEN_FILES;
    let default_budget = RocksDbResourceBudget::canonical_reader_defaults();

    for (label, budget) in [
        ("cgroup-floor-1024", floor_budget),
        ("default-2048", default_budget),
    ] {
        let reader_bytes = assert_zero_steady_state_file_opens(
            &primary_path,
            &secondary_dirs.path().join(label),
            &chain,
            label,
            budget,
        )?;
        assert!(
            reader_bytes > legacy_reader_bytes,
            "{label}: retaining all table readers ({reader_bytes}) must exceed evicted legacy residency ({legacy_reader_bytes})"
        );
        assert!(
            reader_bytes < 1024 * 1024,
            "{label}: table_reader_bytes ({reader_bytes}) must stay well under 1 MiB"
        );
    }
    Ok(())
}
