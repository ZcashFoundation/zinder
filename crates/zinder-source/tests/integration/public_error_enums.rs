#![allow(
    missing_docs,
    reason = "Integration test names describe the public error-enum invariant under test."
)]

use std::fs;
use std::path::{Path, PathBuf};

use eyre::{Result, ensure, eyre};

/// Floor for the error enums the scan must find. The crate holds more, so only
/// a scan that stopped reading the sources falls below it.
const MINIMUM_PUBLIC_ERROR_ENUMS: usize = 5;

/// Every public error enum of `zinder-source` gains variants as nodes add
/// refusals, so each one is `#[non_exhaustive]`: a new variant must not break
/// a caller's `match`.
#[test]
fn public_error_enums_are_non_exhaustive() -> Result<()> {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut error_enums = Vec::new();
    let mut offenders = Vec::new();
    for source_path in rust_source_files(&source_root)? {
        let source = fs::read_to_string(&source_path)?;
        let lines: Vec<&str> = source.lines().collect();
        for (line_index, line) in lines.iter().enumerate() {
            let Some(enum_name) = public_error_enum_name(line) else {
                continue;
            };
            error_enums.push(enum_name.to_owned());
            if !attributes_above(&lines, line_index)
                .any(|attribute| attribute == "#[non_exhaustive]")
            {
                offenders.push(format!("{}: {enum_name}", source_path.display()));
            }
        }
    }

    ensure!(
        error_enums.len() >= MINIMUM_PUBLIC_ERROR_ENUMS,
        "found only {} public error enums under {}: {error_enums:?}",
        error_enums.len(),
        source_root.display()
    );
    ensure!(
        error_enums.iter().any(|name| name == "SourceError"),
        "the scan did not find SourceError: {error_enums:?}"
    );
    ensure!(
        offenders.is_empty(),
        "public error enums without #[non_exhaustive]: {offenders:?}"
    );
    Ok(())
}

/// Returns the name of a public enum whose name ends in `Error`.
fn public_error_enum_name(line: &str) -> Option<&str> {
    let declaration = line.trim_start().strip_prefix("pub enum ")?;
    let enum_name = declaration
        .split(|character: char| !character.is_alphanumeric() && character != '_')
        .next()?;
    enum_name.ends_with("Error").then_some(enum_name)
}

/// Yields the trimmed lines of the attributes and doc comment directly above
/// `line_index`, nearest first, including the lines of a multi-line attribute.
fn attributes_above<'a>(
    lines: &'a [&'a str],
    line_index: usize,
) -> impl Iterator<Item = &'a str> + 'a {
    lines
        .get(..line_index)
        .unwrap_or_default()
        .iter()
        .rev()
        .map(|line| line.trim())
        .take_while(|line| {
            line.starts_with("///")
                || !(line.is_empty() || line.ends_with('}') || line.ends_with(';'))
        })
}

fn rust_source_files(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut source_files = Vec::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(current) = pending.pop() {
        for entry in fs::read_dir(&current)
            .map_err(|error| eyre!("reading {}: {error}", current.display()))?
        {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                source_files.push(path);
            }
        }
    }
    Ok(source_files)
}
