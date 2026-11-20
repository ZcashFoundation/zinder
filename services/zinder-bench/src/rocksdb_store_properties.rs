//! Column-family properties and live SST format versions from an admitted secondary.

use std::{
    collections::BTreeMap,
    fmt::Display,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
    str::FromStr,
};

use serde::Serialize;
use zinder_bench::BenchError;
use zinder_store::RocksDbCanonicalSecondary;

use crate::rocksdb_read_bench::{ReadEnvironment, ReadStoreArgs, ReadyFenceSummary};

#[derive(Serialize)]
pub(crate) struct StoreProperties {
    contract_identity: &'static str,
    report_format_version: u16,
    #[serde(flatten)]
    environment: ReadEnvironment,
    ready_fence: ReadyFenceSummary,
    column_families: BTreeMap<String, FamilyProperties>,
}

#[derive(Serialize)]
struct FamilyProperties {
    total_sst_files_size: u64,
    estimate_live_data_size: u64,
    levels: Vec<LevelProperties>,
    aggregated_table_properties: BTreeMap<&'static str, u64>,
    sst_files_by_format_version: BTreeMap<u32, usize>,
}

#[derive(Serialize)]
struct LevelProperties {
    level: u32,
    num_files: u64,
    compression_ratio: Option<f64>,
}

pub(crate) fn run(args: &ReadStoreArgs) -> Result<StoreProperties, BenchError> {
    let (store, environment) = args.prepare()?;
    let (secondary, _) = store.open("properties", None)?;
    let mut column_families = BTreeMap::new();
    for family in secondary.rocksdb_column_family_names() {
        let properties = family_properties(&secondary, &family)?;
        column_families.insert(family, properties);
    }
    for file in secondary.rocksdb_live_files()? {
        let family = column_families
            .get_mut(&file.column_family_name)
            .ok_or_else(|| {
                BenchError::report_format("live SST names an unadmitted column family")
            })?;
        let name = Path::new(&file.name)
            .file_name()
            .ok_or_else(|| BenchError::report_format("live SST has no file name"))?;
        let format = sst_format_version(&store.primary.join(name))?;
        *family
            .sst_files_by_format_version
            .entry(format)
            .or_default() += 1;
    }
    Ok(StoreProperties {
        contract_identity: "rocksdb-store-properties",
        report_format_version: 1,
        ready_fence: secondary.ready_evidence().into(),
        environment,
        column_families,
    })
}

fn family_properties(
    secondary: &RocksDbCanonicalSecondary,
    family: &str,
) -> Result<FamilyProperties, BenchError> {
    let property = |name: &str| {
        secondary
            .rocksdb_property_value_cf(family, name)?
            .ok_or_else(|| BenchError::report_format(format!("missing {name} on family {family}")))
    };
    let mut levels = Vec::new();
    for level in 0..=6 {
        let files = format!("rocksdb.num-files-at-level{level}");
        let ratio = format!("rocksdb.compression-ratio-at-level{level}");
        let num_files = parse(&files, &property(&files)?)?;
        let ratio: f64 = parse(&ratio, &property(&ratio)?)?;
        levels.push(LevelProperties {
            level,
            num_files,
            compression_ratio: (ratio >= 0.0).then_some(ratio),
        });
    }
    Ok(FamilyProperties {
        total_sst_files_size: parse(
            "total-sst-files-size",
            &property("rocksdb.total-sst-files-size")?,
        )?,
        estimate_live_data_size: parse(
            "estimate-live-data-size",
            &property("rocksdb.estimate-live-data-size")?,
        )?,
        levels,
        aggregated_table_properties: aggregate(&property("rocksdb.aggregated-table-properties")?)?,
        sst_files_by_format_version: BTreeMap::new(),
    })
}

fn parse<T: FromStr>(name: &str, text: &str) -> Result<T, BenchError>
where
    T::Err: Display,
{
    text.trim()
        .parse()
        .map_err(|error| BenchError::report_format(format!("invalid {name}: {error}")))
}

fn aggregate(properties: &str) -> Result<BTreeMap<&'static str, u64>, BenchError> {
    let mut parsed = BTreeMap::new();
    for field in properties.split(';') {
        let Some((label, bytes)) = field.trim().split_once('=') else {
            continue;
        };
        let name = match label {
            "# entries" => "num_entries",
            "# data blocks" => "num_data_blocks",
            "# entries for filter" => "num_filter_entries",
            "# index partitions" => "index_partitions",
            "top-level index size" => "top_level_index_size",
            "# deletions" => "num_deletions",
            "raw key size" => "raw_key_size",
            "raw value size" => "raw_value_size",
            "data block size" => "data_size",
            "data uncompressed size" => "uncompressed_data_size",
            index if index.starts_with("index block size (") => "index_size",
            "filter block size" => "filter_size",
            _ => continue,
        };
        parsed.insert(name, parse(name, bytes)?);
    }
    for required in ["num_entries", "data_size", "index_size", "filter_size"] {
        if !parsed.contains_key(required) {
            return Err(BenchError::report_format(format!(
                "missing aggregate table property {required}"
            )));
        }
    }
    Ok(parsed)
}

// Block-based SSTs end with a little-endian version and an eight-byte magic number.
// The engine omits format versions from aggregated table properties.
fn sst_format_version(path: &Path) -> Result<u32, BenchError> {
    let mut file = File::open(path).map_err(|source| BenchError::io(path, source))?;
    let mut version = [0; 4];
    let mut magic = [0; 8];
    file.seek(SeekFrom::End(-12))
        .and_then(|_| file.read_exact(&mut version))
        .and_then(|()| file.read_exact(&mut magic))
        .map_err(|source| BenchError::io(path, source))?;
    if u64::from_le_bytes(magic) != 0x88e2_41b7_85f4_cff7 {
        return Err(BenchError::report_format(format!(
            "{} lacks the block-based table footer",
            path.display()
        )));
    }
    Ok(u32::from_le_bytes(version))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{BenchError, aggregate, sst_format_version};

    #[test]
    fn aggregate_reads_index_flags_and_rejects_missing_measurements()
    -> Result<(), Box<dyn std::error::Error>> {
        let properties = "# entries=7; data block size=322; index block size (user-key? 0, delta-value? 1)=13; filter block size=0";
        let parsed = aggregate(properties)?;
        assert_eq!(parsed["index_size"], 13);
        assert_eq!(parsed["filter_size"], 0);
        assert!(aggregate("data block size=322").is_err());
        assert!(aggregate(&properties.replace("=13", "=unknown")).is_err());
        Ok(())
    }

    #[test]
    fn sst_footer_reads_a_little_endian_version_without_mutating_the_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("000001.sst");
        let mut bytes = vec![0xff; 53];
        bytes.extend_from_slice(&[7, 0, 0, 0, 0xf7, 0xcf, 0xf4, 0x85, 0xb7, 0x41, 0xe2, 0x88]);
        fs::write(&path, &bytes)?;

        assert_eq!(sst_format_version(&path)?, 7);
        assert_eq!(fs::read(&path)?, bytes);
        Ok(())
    }

    #[test]
    fn sst_footer_rejects_invalid_magic_and_truncated_input()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let invalid_magic = directory.path().join("000001.sst");
        let truncated = directory.path().join("000002.sst");
        fs::write(&invalid_magic, [0; 12])?;
        fs::write(&truncated, [0; 11])?;

        assert!(matches!(
            sst_format_version(&invalid_magic),
            Err(BenchError::ReportFormat { .. })
        ));
        assert!(matches!(
            sst_format_version(&truncated),
            Err(BenchError::Io { .. })
        ));
        Ok(())
    }
}
