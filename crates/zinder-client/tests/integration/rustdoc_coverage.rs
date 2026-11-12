//! Rustdoc-coverage contract test for the public client traits.
//!
//! Every public method declared on `pub trait ChainIndex` and
//! `pub trait EndpointBackedIndex` must carry a `# Examples` rustdoc section.
//! The block is what wallet integrators read first when they open the trait
//! surface in `cargo doc`; missing one is a documentation-contract violation.
//! This test parses `crates/zinder-client/src/chain_index.rs` and asserts each
//! method declaration is preceded by a `/// # Examples` line within its
//! rustdoc block.

use std::{fs, path::PathBuf};

use eyre::{Result, eyre};

const CHAIN_INDEX_DECLARATION: &str = "pub trait ChainIndex";
const ENDPOINT_BACKED_DECLARATION: &str = "pub trait EndpointBackedIndex";
const EXAMPLES_MARKER: &str = "/// # Examples";

#[test]
fn every_public_chain_index_method_has_an_examples_block() -> Result<()> {
    assert_trait_methods_have_examples(CHAIN_INDEX_DECLARATION)
}

#[test]
fn every_public_endpoint_backed_method_has_an_examples_block() -> Result<()> {
    assert_trait_methods_have_examples(ENDPOINT_BACKED_DECLARATION)
}

fn assert_trait_methods_have_examples(trait_declaration: &str) -> Result<()> {
    let chain_index_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/chain_index.rs");
    let source = fs::read_to_string(&chain_index_path)
        .map_err(|error| eyre!("failed to read {}: {error}", chain_index_path.display()))?;

    let trait_body = extract_trait_body(&source, trait_declaration)?;

    let methods = trait_method_declarations(trait_body);
    if methods.is_empty() {
        return Err(eyre!(
            "no `{trait_declaration}` method declarations found in {}; the parser is out of sync \
             with the trait layout",
            chain_index_path.display(),
        ));
    }

    let mut missing_methods: Vec<String> = Vec::new();
    for method in &methods {
        if !rustdoc_block_contains_examples_marker(trait_body, method.declaration_line_index) {
            missing_methods.push(method.name.clone());
        }
    }

    if missing_methods.is_empty() {
        return Ok(());
    }

    Err(eyre!(
        "the following `{trait_declaration}` methods are missing a `/// # Examples` rustdoc block:\n  - {}\n\n\
         Add an `# Examples` section above each declaration in {}.",
        missing_methods.join("\n  - "),
        chain_index_path.display(),
    ))
}

/// One `async fn` or `fn` declaration found inside a trait body.
struct TraitMethod {
    name: String,
    /// Zero-based line index, relative to the trait body, where the declaration starts.
    declaration_line_index: usize,
}

/// Extracts the trait body.
///
/// Returns the slice between the outermost `{` and matching `}` of
/// `trait_declaration`, so method matching cannot consume declarations that
/// sit outside the trait.
fn extract_trait_body<'source>(
    source: &'source str,
    trait_declaration: &str,
) -> Result<&'source str> {
    let trait_offset = source
        .find(trait_declaration)
        .ok_or_else(|| eyre!("`{trait_declaration}` not found in source"))?;
    let open_brace_offset = source[trait_offset..]
        .find('{')
        .map(|relative| trait_offset + relative)
        .ok_or_else(|| eyre!("opening brace for `{trait_declaration}` not found"))?;

    let after_open = open_brace_offset + 1;
    let mut depth: usize = 1;
    for (index, character) in source[after_open..].char_indices() {
        match character {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(&source[after_open..after_open + index]);
                }
            }
            _ => {}
        }
    }
    Err(eyre!("closing brace for `{trait_declaration}` not found"))
}

/// Finds every method declaration at the trait's direct nesting level.
///
/// The trait body uses four-space indentation for its own items;
/// default-impl bodies and inner blocks always sit at six or more spaces of
/// indentation, so anchoring on a four-space prefix excludes them.
fn trait_method_declarations(trait_body: &str) -> Vec<TraitMethod> {
    let mut methods: Vec<TraitMethod> = Vec::new();
    for (line_index, line) in trait_body.lines().enumerate() {
        let Some(trimmed) = line.strip_prefix("    ") else {
            continue;
        };
        if trimmed.starts_with(' ') {
            continue;
        }
        let signature_remainder = trimmed
            .strip_prefix("async fn ")
            .or_else(|| trimmed.strip_prefix("fn "));
        let Some(remainder) = signature_remainder else {
            continue;
        };
        let Some(open_paren_offset) = remainder.find('(') else {
            continue;
        };
        let method_name = remainder[..open_paren_offset].trim();
        if method_name.is_empty() {
            continue;
        }
        methods.push(TraitMethod {
            name: method_name.to_owned(),
            declaration_line_index: line_index,
        });
    }
    methods
}

/// Reports whether the rustdoc block above a declaration contains the
/// `# Examples` marker.
///
/// The walk steps backwards over contiguous doc-comment and blank lines and
/// stops as soon as a non-doc, non-blank line is hit, so the search never
/// crosses an earlier method's body.
fn rustdoc_block_contains_examples_marker(trait_body: &str, declaration_line_index: usize) -> bool {
    let lines: Vec<&str> = trait_body.lines().collect();
    let mut cursor = declaration_line_index;
    while cursor > 0 {
        cursor -= 1;
        let line = lines[cursor].trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("///") || line.starts_with("//!") {
            if line.starts_with(EXAMPLES_MARKER) {
                return true;
            }
            continue;
        }
        if line.starts_with("#[") || line.starts_with("#![") {
            continue;
        }
        break;
    }
    false
}

#[test]
fn extract_chain_index_body_matches_known_method_set() -> Result<()> {
    let must_include = [
        "current_epoch",
        "visible_tip_block",
        "settled_tip_block",
        "block_id_by_selector",
        "block_header_by_selector",
        "compact_block_at",
        "compact_blocks_in_range",
        "tree_state_at",
        "latest_tree_state_checkpoint",
        "subtree_roots_in_range",
        "transaction_by_id",
        "transparent_address_unspent_outputs",
        "transparent_address_tx_ids_in_range",
    ];
    assert_trait_includes_methods(CHAIN_INDEX_DECLARATION, &must_include)
}

#[test]
fn extract_endpoint_backed_body_matches_known_method_set() -> Result<()> {
    let must_include = [
        "server_info",
        "chain_value_pools_at_tip",
        "broadcast_transaction",
        "chain_events",
        "chain_events_for_family",
        "chain_events_with_filter",
        "mempool_snapshot",
        "mempool_events",
        "is_in_mempool",
    ];
    assert_trait_includes_methods(ENDPOINT_BACKED_DECLARATION, &must_include)
}

fn assert_trait_includes_methods(trait_declaration: &str, must_include: &[&str]) -> Result<()> {
    let chain_index_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/chain_index.rs");
    let source = fs::read_to_string(&chain_index_path)
        .map_err(|error| eyre!("failed to read {}: {error}", chain_index_path.display()))?;
    let trait_body = extract_trait_body(&source, trait_declaration)?;
    let methods = trait_method_declarations(trait_body);
    let names: Vec<&str> = methods.iter().map(|method| method.name.as_str()).collect();

    for expected_name in must_include {
        assert!(
            names.contains(expected_name),
            "rustdoc-coverage parser missed expected `{trait_declaration}` method \
             `{expected_name}`; the parser is out of sync with the trait layout"
        );
    }
    Ok(())
}
