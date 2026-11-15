#![allow(
    missing_docs,
    reason = "Integration test names describe the capability uniqueness contract under test."
)]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use eyre::{Result, ensure, eyre};
use zinder_proto::CAPABILITIES;

const CAPABILITIES_SOURCE_PATH: &str = "crates/zinder-proto/src/capabilities.rs";
const SCANNED_DIRECTORIES: &[&str] = &["crates", "services"];

/// Floor for the Rust sources one scan root must yield. The tree holds several
/// times this many, so only a missing or emptied tree falls below it.
const MINIMUM_SCANNED_FILES_PER_DIRECTORY: usize = 50;

/// Source files allowed to contain capability literal strings.
///
/// The single source of truth is `capabilities.rs`. The doc-mirror test
/// (`capability_docs.rs`) and this file consume the `CAPABILITIES` table
/// directly; they never embed the literal strings themselves, so they are
/// not allow-listed.
const LITERAL_ALLOWLIST: &[&str] = &[CAPABILITIES_SOURCE_PATH];

#[test]
fn capability_literals_are_imported_not_duplicated() -> Result<()> {
    let root = workspace_root()?;
    let allowed: Vec<PathBuf> = LITERAL_ALLOWLIST
        .iter()
        .map(|path| root.join(path))
        .collect();

    let capability_strings: BTreeSet<&str> = CAPABILITIES.iter().map(|spec| spec.string).collect();
    if capability_strings.is_empty() {
        return Err(eyre!(
            "the CAPABILITIES table must contain at least one entry for this guard \
             to mean anything; refusing to silently pass."
        ));
    }

    let mut offenders: Vec<(PathBuf, &str)> = Vec::new();
    for directory in SCANNED_DIRECTORIES {
        for source_path in rust_source_files(&root.join(directory))? {
            if allowed
                .iter()
                .any(|allowed_path| allowed_path == &source_path)
            {
                continue;
            }
            let contents = fs::read_to_string(&source_path)?;
            let code_only = strip_line_comments(&contents);
            for capability in &capability_strings {
                let literal = format!("\"{capability}\"");
                if code_only.contains(&literal) {
                    offenders.push((source_path.clone(), capability));
                }
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "capability literals must be imported from `zinder_proto::capabilities`, \
         never re-declared as inline string literals. Replace each occurrence with \
         the corresponding `pub const` from `{CAPABILITIES_SOURCE_PATH}`:\n  {}",
        offenders
            .iter()
            .map(|(path, capability)| format!("{capability:?} in {}", path.display()))
            .collect::<Vec<_>>()
            .join("\n  "),
    );
    Ok(())
}

/// Returns `source` with whole-line comments removed.
///
/// Strips lines whose first non-whitespace characters are `//`, including doc
/// comments (`///`, `//!`). Block comments are not stripped; capability
/// literals do not appear in `/* ... */` form in this workspace.
fn strip_line_comments(source: &str) -> String {
    source
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Resolves the workspace root from the manifest directory of this test's crate.
///
/// Nextest sets `CARGO_MANIFEST_DIR` at run time, rewritten by
/// `--workspace-remap` when the tests run from an archive. The compile-time
/// value names the build machine's checkout, so it is only the fallback for
/// runners that do not set the variable.
fn workspace_root() -> Result<PathBuf> {
    let manifest_directory = std::env::var_os("CARGO_MANIFEST_DIR")
        .map_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")), PathBuf::from);
    manifest_directory
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| eyre!("CARGO_MANIFEST_DIR has no grandparent: {manifest_directory:?}"))
}

/// Fails when a scan root holds too few Rust sources to be the workspace tree.
///
/// A scan that finds nothing would pass every invariant vacuously, for
/// example when archived tests run without the checkout beside them.
fn ensure_scan_saw_source_tree(directory: &Path, scanned_file_count: usize) -> Result<()> {
    ensure!(
        scanned_file_count >= MINIMUM_SCANNED_FILES_PER_DIRECTORY,
        "scanned {scanned_file_count} Rust sources under {}, expected at least \
         {MINIMUM_SCANNED_FILES_PER_DIRECTORY}; the invariant tests need the workspace source tree \
         at the path nextest reports in CARGO_MANIFEST_DIR",
        directory.display(),
    );
    Ok(())
}

fn rust_source_files(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    collect_rust_source_files(directory, &mut paths)?;
    ensure_scan_saw_source_tree(directory, paths.len())?;
    paths.sort();
    Ok(paths)
}

fn collect_rust_source_files(directory: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            if path
                .file_name()
                .is_some_and(|name| name == "target" || name == ".tmp")
            {
                continue;
            }
            collect_rust_source_files(&path, paths)?;
        } else if file_type.is_file() && path.extension().is_some_and(|extension| extension == "rs")
        {
            paths.push(path);
        }
    }
    Ok(())
}
