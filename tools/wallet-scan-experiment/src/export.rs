//! Export public compact scan actions and measure native protobuf item sizes.
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use clap::Parser;
use eyre::{Result, WrapErr as _};
use zinder_store::{CanonicalStoreError, RawBlobRetention};

use crate::{archive::ProjectionReport, corpus::ScanAction};

mod archive;
mod corpus;

const MAX_SCAN_ATTEMPTS: usize = 3;

#[derive(Parser)]
#[command(about = "Measure a Testnet compact-block range from a read-only canonical secondary")]
struct Args {
    /// Canonical Testnet wallet-serving store.
    #[arg(long)]
    store: PathBuf,
    /// Raw blob policy used by the canonical writer (none, transactions, or all).
    #[arg(long, default_value = "transactions", value_parser = parse_raw_blob_retention)]
    raw_blob_retention: RawBlobRetention,
    /// First height, inclusive.
    #[arg(long)]
    start: u32,
    /// Last height, inclusive.
    #[arg(long)]
    end: u32,
    /// Write the public candidate actions to a new file.
    #[arg(long)]
    corpus: Option<PathBuf>,
}

fn parse_raw_blob_retention(encoded: &str) -> Result<RawBlobRetention, String> {
    RawBlobRetention::from_kebab_case(encoded)
        .ok_or_else(|| "expected none, transactions, or all".to_owned())
}

fn write_action(writer: &mut impl Write, action: &ScanAction) -> Result<()> {
    let tag = match action.pool {
        corpus::Pool::Orchard => b'O',
        corpus::Pool::Ironwood => b'I',
    };
    writer.write_all(&[tag])?;
    writer.write_all(&action.nullifier)?;
    writer.write_all(&action.cmx)?;
    writer.write_all(&action.epk)?;
    writer.write_all(&action.ciphertext)?;
    if let Some(hint) = action.hint {
        writer.write_all(&[32])?;
        writer.write_all(&hint)?;
    } else {
        writer.write_all(&[0])?;
    }
    Ok(())
}

fn export(
    path: &Path,
    scan: impl FnOnce(&mut BufWriter<&mut File>) -> Result<ProjectionReport>,
) -> Result<ProjectionReport> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut file = tempfile::NamedTempFile::new_in(dir)?;
    let report = {
        let mut writer = BufWriter::new(file.as_file_mut());
        writer.write_all(corpus::MAGIC)?;
        let report = scan(&mut writer)?;
        writer.flush()?;
        report
    };
    file.persist_noclobber(path)
        .wrap_err("creating corpus output")?;
    Ok(report)
}

fn measure(
    corpus: Option<&Path>,
    mut read_range: impl FnMut(&mut dyn FnMut(ScanAction) -> Result<()>) -> Result<ProjectionReport>,
) -> Result<ProjectionReport> {
    let mut scan = || match corpus {
        Some(path) => export(path, |writer| {
            read_range(&mut |action| write_action(writer, &action))
        }),
        None => read_range(&mut |_| Ok(())),
    };

    // A new secondary can see a different chain, so restart the range and its output together.
    for _ in 1..MAX_SCAN_ATTEMPTS {
        match scan() {
            Err(error)
                if error
                    .downcast_ref::<CanonicalStoreError>()
                    .is_some_and(CanonicalStoreError::is_missing_table_file) => {}
            completed => return completed,
        }
    }
    scan()
}

fn main() -> Result<()> {
    let args = Args::parse();
    let report = measure(args.corpus.as_deref(), |visit| {
        archive::read_range(
            &args.store,
            args.raw_blob_retention,
            args.start,
            args.end,
            visit,
        )
    })?;
    let stdout = std::io::stdout().lock();
    serde_json::to_writer_pretty(stdout, &report)?;
    writeln!(std::io::stdout().lock())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use eyre::OptionExt as _;

    use super::*;

    #[test]
    fn archive_retention_defaults_to_transactions_and_accepts_full_blocks() -> Result<()> {
        let arguments = [
            "export-wallet-scan-corpus",
            "--store",
            ".",
            "--start",
            "1",
            "--end",
            "1",
        ];
        assert_eq!(
            Args::try_parse_from(arguments)?.raw_blob_retention,
            RawBlobRetention::Transactions
        );
        let full_blocks = arguments.into_iter().chain(["--raw-blob-retention", "all"]);
        assert_eq!(
            Args::try_parse_from(full_blocks)?.raw_blob_retention,
            RawBlobRetention::All
        );
        Ok(())
    }

    fn missing_table_file() -> Result<CanonicalStoreError> {
        let directory = tempfile::tempdir()?;
        let primary = rust_rocksdb::DB::open_default(directory.path())?;
        let source = primary
            .ingest_external_file(vec![directory.path().join("000123.sst")])
            .err()
            .ok_or_eyre("RocksDB ingested an absent table file")?;
        Ok(CanonicalStoreError::RocksDbOperation {
            operation: "read compact blocks",
            source,
        })
    }

    #[test]
    fn retry_discards_the_partial_corpus_before_restarting() -> Result<()> {
        let output = tempfile::tempdir()?;
        let path = output.path().join("wallet.scan");
        let action = ScanAction {
            pool: corpus::Pool::Orchard,
            nullifier: [1; 32],
            cmx: [2; 32],
            epk: [3; 32],
            ciphertext: [4; 52],
            hint: None,
        };
        let mut attempts = 0;
        measure(Some(&path), |visit| {
            attempts += 1;
            assert!(!path.exists());
            assert_eq!(output.path().read_dir()?.count(), 1);
            if attempts == 1 {
                for _ in 0..128 {
                    visit(action.clone())?;
                }
                return Err(missing_table_file()?.into());
            }
            visit(action.clone())?;
            Ok(ProjectionReport::default())
        })?;
        let mut expected = corpus::MAGIC.to_vec();
        write_action(&mut expected, &action)?;
        assert_eq!(std::fs::read(path)?, expected);
        assert_eq!(attempts, 2);
        Ok(())
    }

    #[test]
    fn measurement_stops_after_three_missing_table_files() -> Result<()> {
        let mut attempts = 0;
        let failure = measure(None, |_| {
            attempts += 1;
            Err(missing_table_file()?.into())
        })
        .err()
        .ok_or_eyre("expected the final missing table file error")?;
        assert!(
            failure
                .downcast::<CanonicalStoreError>()?
                .is_missing_table_file()
        );
        assert_eq!(attempts, 3);
        Ok(())
    }

    #[test]
    fn other_store_errors_fail_on_the_first_attempt() -> Result<()> {
        let mut attempts = 0;
        let failure = measure(None, |_| {
            attempts += 1;
            Err(CanonicalStoreError::NetworkUpgradeScheduleNotAdmitted {
                path: PathBuf::from("canonical"),
            }
            .into())
        })
        .err()
        .ok_or_eyre("expected the admission error")?;
        assert!(matches!(
            failure.downcast::<CanonicalStoreError>()?,
            CanonicalStoreError::NetworkUpgradeScheduleNotAdmitted { .. }
        ));
        assert_eq!(attempts, 1);
        Ok(())
    }
}
