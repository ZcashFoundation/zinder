#![allow(
    missing_docs,
    reason = "Integration test names describe the wire-convention invariants under test."
)]

use std::fs;
use std::path::{Path, PathBuf};

use eyre::{Result, eyre};

const WIRE_BRANCH_ID_PATH: &str = "crates/zinder-core/src/wire/branch_id.rs";
const WIRE_BLOCK_HASH_PATH: &str = "crates/zinder-core/src/wire/block_hash.rs";
const WIRE_TRANSACTION_ID_PATH: &str = "crates/zinder-core/src/wire/transaction_id.rs";

const BANNED_BRANCH_ID_FORMAT_FRAGMENT: &str = "format!(\"{:08x}\"";

const SCANNED_DIRECTORIES: &[&str] = &["crates", "services"];

/// Files allowed to contain a `format!("{:08x}", ...)` call.
///
/// Only `wire/branch_id.rs` may host the call: it is the canonical encoder
/// for consensus branch ids, and its `#[cfg(test)] mod tests` legitimately
/// compares against hand-written fixtures.
const BRANCH_ID_FORMAT_ALLOWLIST: &[&str] = &[WIRE_BRANCH_ID_PATH];

/// Files allowed to write `(transaction_id|block_hash).as_bytes().(to_vec|into)`.
///
/// The two wire modules legitimately call `as_bytes()` to implement the
/// canonical encoders. The store-format files encode the same byte form into
/// `RocksDB` key/value bytes; the convention there is internal to the storage
/// layer and not a service boundary.
const TXID_BLOCK_HASH_AS_BYTES_ALLOWLIST: &[&str] = &[
    WIRE_TRANSACTION_ID_PATH,
    WIRE_BLOCK_HASH_PATH,
    "crates/zinder-store/src/format/mempool_event_codec.rs",
    "crates/zinder-store/src/format/stream_cursor.rs",
    "crates/zinder-store/src/address_output.rs",
];

/// Identifier substrings that name a Zcash 32-byte little-endian hash field at a wire boundary.
///
/// The list intentionally over-includes synonyms used in concrete struct
/// fields (`block_hash`, `previous_block_hash`, ...) so a wire boundary that
/// names the field one of these ways still routes through `encode_internal_*`.
/// The chain-view tips (`visible_tip`, `settled_tip`, `indexed_tip`) carry the
/// hash on their `BlockTip.hash` field, encoded through `encode_rpc_block_hash_hex`.
const WIRE_HASH_FIELD_NAMES: &[&str] = &[
    "transaction_id",
    "block_hash",
    "previous_block_hash",
    "completing_block_hash",
    "parent_hash",
    "spending_transaction_id",
];

/// Suffixes that turn a wire-boundary `.as_bytes()` into a proto-field emission.
///
/// Tests and assertions may compare against the slice form (`.as_bytes()`
/// alone, or `&value.as_bytes()`), so the structural guard only flags
/// emissions that allocate a `Vec<u8>` or `bytes::Bytes`.
const WIRE_EMISSION_SUFFIXES: &[&str] = &[".as_bytes().to_vec()", ".as_bytes().into()"];

#[test]
fn branch_id_hex_format_lives_only_in_wire_module() -> Result<()> {
    let root = workspace_root()?;
    let allowlist: Vec<PathBuf> = BRANCH_ID_FORMAT_ALLOWLIST
        .iter()
        .map(|path| root.join(path))
        .collect();

    let mut offenders: Vec<PathBuf> = Vec::new();
    for directory in SCANNED_DIRECTORIES {
        for source_path in rust_source_files(&root.join(directory))? {
            if allowlist.iter().any(|allowed| allowed == &source_path) {
                continue;
            }
            let contents = fs::read_to_string(&source_path)?;
            if code_lines_contain(&contents, BANNED_BRANCH_ID_FORMAT_FRAGMENT) {
                offenders.push(source_path);
            }
        }
    }

    let pattern_for_error = BANNED_BRANCH_ID_FORMAT_FRAGMENT;
    let formatted_offenders = offenders
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join("\n  ");
    assert!(
        offenders.is_empty(),
        "consensus branch ids must use `zinder_core::wire::encode_branch_id_hex`; \
         inline {pattern_for_error} call found in:\n  {formatted_offenders}\n\
         move the conversion into `{WIRE_BRANCH_ID_PATH}` or call the encoder \
         from the offending site.",
    );
    Ok(())
}

/// Returns `true` when `needle` appears on a line that is not a Rust line
/// comment.
///
/// Strips lines whose first non-whitespace characters are `//` (including doc
/// comments `///` and `//!`). Block comments are not stripped; the test
/// invariants this helper supports do not appear in `/* ... */` form anywhere
/// in the workspace.
fn code_lines_contain(source: &str, needle: &str) -> bool {
    code_lines(source).any(|line| line.contains(needle))
}

/// Yields source lines that are not Rust line comments.
fn code_lines(source: &str) -> impl Iterator<Item = &str> {
    source
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
}

/// Walks Rust source files under `directory`, excluding the per-crate
/// `tests/` subtrees.
///
/// Wire-emission guards apply only to production source code: tests routinely
/// build hand-crafted proto fixtures with inline `as_bytes()` to assert
/// round-trip equality, and forcing those through the encoders adds churn
/// without preventing the regression the guard exists to prevent (drift
/// between production call sites and the canonical convention).
fn production_source_files(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    collect_production_source_files(directory, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn collect_production_source_files(directory: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    if !directory.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            if path
                .file_name()
                .is_some_and(|name| name == "target" || name == ".tmp" || name == "tests")
            {
                continue;
            }
            collect_production_source_files(&path, paths)?;
        } else if file_type.is_file() && path.extension().is_some_and(|extension| extension == "rs")
        {
            paths.push(path);
        }
    }
    Ok(())
}

#[test]
fn wire_hash_fields_use_canonical_encoders() -> Result<()> {
    let root = workspace_root()?;
    let allowlist: Vec<PathBuf> = TXID_BLOCK_HASH_AS_BYTES_ALLOWLIST
        .iter()
        .map(|path| root.join(path))
        .collect();

    let mut offenders: Vec<(PathBuf, String)> = Vec::new();
    for directory in SCANNED_DIRECTORIES {
        for source_path in production_source_files(&root.join(directory))? {
            if allowlist.iter().any(|allowed| allowed == &source_path) {
                continue;
            }
            let contents = fs::read_to_string(&source_path)?;
            for line in code_lines(&contents) {
                let has_emission = WIRE_EMISSION_SUFFIXES
                    .iter()
                    .any(|suffix| line.contains(suffix));
                if !has_emission {
                    continue;
                }
                let names_wire_hash = WIRE_HASH_FIELD_NAMES.iter().any(|name| line.contains(name));
                if names_wire_hash {
                    offenders.push((source_path.clone(), line.trim().to_owned()));
                }
            }
        }
    }

    let formatted_offenders = offenders
        .iter()
        .map(|(path, line)| format!("{}: {line}", path.display()))
        .collect::<Vec<_>>()
        .join("\n  ");
    assert!(
        offenders.is_empty(),
        "transaction-id and block-hash wire emissions must use \
         `zinder_core::wire::encode_internal_transaction_id` / \
         `encode_internal_block_hash`. Inline `.as_bytes().to_vec()` or \
         `.as_bytes().into()` found at:\n  {formatted_offenders}",
    );
    Ok(())
}

#[test]
fn network_to_wire_string_match_lives_only_in_wire_module() -> Result<()> {
    const NETWORK_MATCH_PATTERNS: &[&str] = &[
        "Network::ZcashMainnet =>",
        "Network::ZcashTestnet =>",
        "Network::ZcashRegtest =>",
    ];
    const NETWORK_ALLOWLIST: &[&str] = &["crates/zinder-core/src/wire/chain_name.rs"];

    let root = workspace_root()?;
    let allowlist: Vec<PathBuf> = NETWORK_ALLOWLIST
        .iter()
        .map(|path| root.join(path))
        .collect();

    let mut offenders: Vec<(PathBuf, String)> = Vec::new();
    for directory in SCANNED_DIRECTORIES {
        for source_path in production_source_files(&root.join(directory))? {
            if allowlist.iter().any(|allowed| allowed == &source_path) {
                continue;
            }
            let contents = fs::read_to_string(&source_path)?;
            for line in code_lines(&contents) {
                let names_wire_string = line.contains("\"main\"")
                    || line.contains("\"test\"")
                    || line.contains("\"zcash-mainnet\"")
                    || line.contains("\"zcash-testnet\"")
                    || line.contains("\"zcash-regtest\"");
                if !names_wire_string {
                    continue;
                }
                let matches_network_arm =
                    NETWORK_MATCH_PATTERNS.iter().any(|arm| line.contains(arm));
                if matches_network_arm {
                    offenders.push((source_path.clone(), line.trim().to_owned()));
                }
            }
        }
    }

    let formatted_offenders = offenders
        .iter()
        .map(|(path, line)| format!("{}: {line}", path.display()))
        .collect::<Vec<_>>()
        .join("\n  ");
    assert!(
        offenders.is_empty(),
        "Network to wire-string tables must live in \
         `crates/zinder-core/src/wire/chain_name.rs`; call \
         `encode_bip70_chain_name` or `encode_zinder_native_chain_name` \
         instead. Found at:\n  {formatted_offenders}",
    );
    Ok(())
}

#[test]
fn utxo_set_commitment_personal_lives_only_in_wire_module() -> Result<()> {
    const PERSONAL_TAG_LITERAL: &str = "ZinderUtxoSet___";
    const PERSONAL_ALLOWLIST: &[&str] = &["crates/zinder-core/src/wire/utxo_set_commitment.rs"];

    let root = workspace_root()?;
    let allowlist: Vec<PathBuf> = PERSONAL_ALLOWLIST
        .iter()
        .map(|path| root.join(path))
        .collect();

    let mut offenders: Vec<PathBuf> = Vec::new();
    for directory in SCANNED_DIRECTORIES {
        for source_path in production_source_files(&root.join(directory))? {
            if allowlist.iter().any(|allowed| allowed == &source_path) {
                continue;
            }
            let contents = fs::read_to_string(&source_path)?;
            if code_lines_contain(&contents, PERSONAL_TAG_LITERAL) {
                offenders.push(source_path);
            }
        }
    }

    let formatted_offenders = offenders
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join("\n  ");
    assert!(
        offenders.is_empty(),
        "the UTXO-set commitment personalization and element encoding must live in \
         `crates/zinder-core/src/wire/utxo_set_commitment.rs`; call \
         `zinder_core::wire::encode_utxo_set_commitment_element` instead of \
         re-serializing the preimage. Found the personalization literal in:\n  {formatted_offenders}",
    );
    Ok(())
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_directory = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_directory
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| eyre!("CARGO_MANIFEST_DIR has no grandparent: {manifest_directory:?}"))
}

fn rust_source_files(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    collect_rust_source_files(directory, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn collect_rust_source_files(directory: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    if !directory.exists() {
        return Ok(());
    }
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
