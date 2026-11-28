//! Drift gate:
//! 1. Every leaf field in [`zinder_runtime::ZinderConfig`] must have an exact
//!    bijective counterpart in [`zinder_runtime::ENVIRONMENT_VARIABLES`] (and its
//!    canonical `ZINDER_<SECTION>__<FIELD>` name).
//! 2. Every [`zinder_runtime::ENVIRONMENT_VARIABLES`] entry must be accepted by
//!    [`zinder_runtime::ConfigLoader`] when loading [`zinder_runtime::ZinderConfig`].
//! 3. The `env-var-table:public-interfaces` Markdown block in
//!    `docs/reference/configuration.md` must match what
//!    [`zinder_runtime::render_environment_variable_table`] emits.

#![allow(
    missing_docs,
    reason = "Integration test names describe the env-var-table mirror and schema bijection contract."
)]

use std::collections::{BTreeMap, BTreeSet};

use eyre::{Result, eyre};
use zinder_runtime::{
    ConfigError, ConfigLoader, ENVIRONMENT_VARIABLES, ZinderConfig, discover_schema_leaf_paths,
};

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

#[test]
fn schema_leaf_paths_match_documented_env_vars_bijectively() {
    let schema_paths = discover_schema_leaf_paths();
    let mut doc_paths = BTreeSet::new();
    let mut seen_names = BTreeSet::new();
    let mut path_to_name = BTreeMap::new();

    for entry in ENVIRONMENT_VARIABLES {
        assert!(
            seen_names.insert(entry.name),
            "duplicate env var name in ENVIRONMENT_VARIABLES: {}",
            entry.name
        );
        assert!(
            doc_paths.insert(entry.toml_path.to_owned()),
            "duplicate toml_path in ENVIRONMENT_VARIABLES: {}",
            entry.toml_path
        );
        assert_eq!(
            entry.name,
            expected_env_var_name(entry.toml_path),
            "env var name `{}` does not match canonical mapping for `{}`",
            entry.name,
            entry.toml_path
        );
        assert!(
            !entry.used_by.is_empty(),
            "ENVIRONMENT_VARIABLES entry `{}` must list at least one binary",
            entry.name
        );
        let _inserted = path_to_name.insert(entry.toml_path, entry.name);
    }

    let missing_docs: Vec<&String> = schema_paths.difference(&doc_paths).collect();
    let stale_docs: Vec<&String> = doc_paths.difference(&schema_paths).collect();
    assert!(
        missing_docs.is_empty() && stale_docs.is_empty(),
        "ZinderConfig leaf schema and ENVIRONMENT_VARIABLES diverged.\n  Missing from ENVIRONMENT_VARIABLES: {missing_docs:?}\n  Stale in ENVIRONMENT_VARIABLES: {stale_docs:?}\n  Paths: {path_to_name:?}"
    );
}

#[test]
fn every_documented_env_var_name_is_accepted_by_zinder_config() -> Result<()> {
    for entry in ENVIRONMENT_VARIABLES {
        let env_name = entry.name.replace("<N>", "1");
        let dummy_val = dummy_value_for(entry.toml_path);
        let outcome = ConfigLoader::new()
            .with_zinder_env_from_map([(env_name.clone(), dummy_val.to_owned())])?
            .load::<ZinderConfig>();
        if let Err(ConfigError::RejectedEnvVar { .. }) = outcome {
            return Err(eyre!(
                "ZinderConfig rejected documented env var `{env_name}` (toml_path `{}`): {outcome:?}",
                entry.toml_path
            ));
        }
        outcome.map_err(|err| {
            eyre!(
                "ZinderConfig failed loading documented env var `{env_name}` (toml_path `{}`): {err}",
                entry.toml_path
            )
        })?;
    }
    Ok(())
}

fn expected_env_var_name(toml_path: &str) -> String {
    let encoded = toml_path
        .split('.')
        .map(|segment| {
            if segment == "<N>" {
                "<N>".to_owned()
            } else {
                segment.to_ascii_uppercase()
            }
        })
        .collect::<Vec<_>>()
        .join("__");
    format!("ZINDER_{encoded}")
}

fn dummy_value_for(toml_path: &str) -> &'static str {
    match toml_path {
        "observability.otlp_endpoint" => "https://collector.internal:4317",
        "observability.sampling_rate"
        | "node.health.verification_progress_floor"
        | "node_fallbacks.<N>.health.verification_progress_floor" => "0.5",
        "network.name" => "zcash-regtest",
        "ops.listen_addr"
        | "ingest_control.listen_addr"
        | "projector_control.listen_addr"
        | "query.listen_addr"
        | "compat.listen_addr"
        | "explorer.listen_addr"
        | "cipherscan.listen_addr" => "127.0.0.1:9100",
        "security.allow_public_bind"
        | "storage.full_blocks"
        | "ingest.explorer_views"
        | "ingest.tree_state_densification.enabled"
        | "ingest.tree_state_densification.during_construction"
        | "ingest.tree_state_verification.enabled"
        | "query.trust_proxy_client_address"
        | "compat.ping_very_insecure"
        | "compat.trust_proxy_client_address"
        | "explorer.trust_proxy_client_address" => "false",
        "storage.raw_blob_policy" => "none",
        "storage.canonical.rocksdb.statistics_level"
        | "storage.materialized_views.rocksdb.statistics_level"
        | "wallet.rocksdb.statistics_level" => "tickers",
        "ingest.tree_state_densification.selection" => "every-block",
        "ingest.run_overrides.coverage" => "explicit",
        "deployment.profile" | "compat.serving" => "wallet",
        "query.range_admission.client_key" | "compat.range_admission.client_key" => "connection",
        path if path.ends_with(".method") => "none",
        path if path.ends_with(".browser_origins") => "https://wallet.example",
        path if path.ends_with("proxy_protocol_trusted_cidrs") => "127.0.0.1/32",
        path if path.ends_with("_bytes")
            || path.ends_with("_ms")
            || path.ends_with("_secs")
            || path.ends_with("_seconds")
            || path.ends_with("_blocks")
            || path.ends_with("_hours")
            || path.ends_with("_minutes")
            || path.ends_with("_files")
            || path.ends_with("_count")
            || path.ends_with("_jobs")
            || path.ends_with("_concurrency")
            || path.ends_with("_workers")
            || path.ends_with(".workers")
            || path.ends_with("_budget")
            || path.ends_with("_attempts")
            || path.ends_with("_waiters")
            || path.ends_with("_height")
            || path.ends_with("_requests")
            || path.ends_with("_streams")
            || path.ends_with("_per_client")
            || path.ends_with("_per_slot")
            || path.ends_with("_per_second")
            || path.ends_with("_per_step")
            || path.ends_with("_chain_epochs")
            || path.ends_with("_max_concurrent")
            || path.ends_with("_per_sorter") =>
        {
            "1"
        }
        _ => "http://127.0.0.1:8080",
    }
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
