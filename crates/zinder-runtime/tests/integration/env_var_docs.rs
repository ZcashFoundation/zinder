//! Drift gate: the `env-var-table:public-interfaces` Markdown block in
//! `docs/reference/configuration.md` must match what
//! [`zinder_runtime::render_environment_variable_table`] emits.
//!
//! The contract is bidirectional. Adding an env var requires extending
//! [`zinder_runtime::ENVIRONMENT_VARIABLES`] and copy-pasting the regenerated
//! table back into `configuration.md`; missing the second step makes
//! this test fail with the exact rendering the doc must contain. Removing
//! an env var works the same way in reverse.

#![allow(
    missing_docs,
    reason = "Integration test names describe the env-var-table mirror contract under test."
)]

use eyre::{Result, eyre};

const CONFIGURATION_DOC: &str = include_str!("../../../../docs/reference/configuration.md");
const START_MARKER: &str = "<!-- env-var-table:public-interfaces:start -->";
const END_MARKER: &str = "<!-- env-var-table:public-interfaces:end -->";

#[test]
fn configuration_env_var_table_mirrors_runtime_constant() -> Result<()> {
    let rendered = zinder_runtime::render_environment_variable_table();
    let block_in_doc = extract_block(CONFIGURATION_DOC)?;
    let expected = format!("\n{rendered}");
    assert_eq!(
        block_in_doc.trim_end(),
        expected.trim_end(),
        "docs/reference/configuration.md env-var table has drifted from \
         zinder_runtime::ENVIRONMENT_VARIABLES. Regenerate via\n  \
         cargo run -p zinder-runtime --example dump_env_var_table\n\
         and paste the output between the start/end markers."
    );
    Ok(())
}

#[test]
fn configuration_block_carries_at_least_one_row() -> Result<()> {
    let block_in_doc = extract_block(CONFIGURATION_DOC)?;
    let row_count = block_in_doc
        .lines()
        .filter(|line| line.starts_with("| `ZINDER_"))
        .count();
    assert!(
        row_count >= 1,
        "env-var-table block must contain at least one ZINDER_* row; the block \
         was found but empty, which is a maintenance bug (no operator surface \
         to document)."
    );
    Ok(())
}

fn extract_block(doc: &str) -> Result<&str> {
    let after_start = doc
        .split_once(START_MARKER)
        .ok_or_else(|| eyre!("missing start marker {START_MARKER}"))?
        .1;
    let block = after_start
        .split_once(END_MARKER)
        .ok_or_else(|| eyre!("missing end marker {END_MARKER}"))?
        .0;
    Ok(block)
}
