//! Export public compact scan actions and measure native protobuf item sizes.
use std::{
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use clap::Parser;
use eyre::WrapErr as _;

mod archive;
mod corpus;

#[derive(Parser)]
#[command(about = "Measure a Testnet compact-block range from a read-only canonical secondary")]
struct Args {
    /// Canonical Testnet wallet-serving store.
    #[arg(long)]
    store: PathBuf,
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

fn write_action(writer: &mut impl Write, action: &corpus::ScanAction) -> eyre::Result<()> {
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

fn export(args: &Args, path: &Path) -> eyre::Result<archive::ProjectionReport> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut file = tempfile::NamedTempFile::new_in(dir)?;
    let report = {
        let mut writer = BufWriter::new(file.as_file_mut());
        writer.write_all(corpus::MAGIC)?;
        let report = archive::read_range(&args.store, args.start, args.end, |action| {
            write_action(&mut writer, &action)
        })?;
        writer.flush()?;
        report
    };
    file.persist_noclobber(path)
        .wrap_err("creating corpus output")?;
    Ok(report)
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    let report = match &args.corpus {
        Some(path) => export(&args, path)?,
        None => archive::read_range(&args.store, args.start, args.end, |_| Ok(()))?,
    };
    let stdout = std::io::stdout().lock();
    serde_json::to_writer_pretty(stdout, &report)?;
    writeln!(std::io::stdout().lock())?;
    Ok(())
}
