//! Drift gate: every `zinder_` metric a shipped Grafana dashboard queries must
//! be listed in `docs/reference/metrics.md`.
//!
//! A panel that names a metric no runtime emits matches no series and stays
//! empty without any error. The metrics reference lists what the runtimes
//! emit, so a dashboard expression is checked against it.

#![allow(
    missing_docs,
    reason = "Integration test names describe the dashboard metric contract under test."
)]

use std::{collections::BTreeSet, fs, path::Path};

use eyre::{Result, ensure, eyre};
use serde_json::Value;

const METRICS_REFERENCE: &str = include_str!("../../../../docs/reference/metrics.md");
const DASHBOARD_DIRECTORY: &str = "../../observability/grafana/dashboards";
const METRIC_PREFIX: &str = "zinder_";

/// Series suffixes Prometheus adds to a histogram or summary name.
const DERIVED_SERIES_SUFFIXES: &[&str] = &["_bucket", "_sum", "_count"];

#[test]
fn dashboard_expressions_name_only_documented_metrics() -> Result<()> {
    let reference = DocumentedMetrics::parse(METRICS_REFERENCE);
    let dashboards = dashboard_files()?;
    ensure!(
        !dashboards.is_empty(),
        "no dashboards found under {DASHBOARD_DIRECTORY}"
    );
    let mut undocumented = Vec::new();
    for dashboard in &dashboards {
        let parsed: Value = serde_json::from_str(&fs::read_to_string(dashboard)?)?;
        let mut expressions = Vec::new();
        collect_expressions(&parsed, &mut expressions);
        for expression in expressions {
            for metric in metric_names(expression) {
                if !reference.lists(&metric) {
                    undocumented.push(format!(
                        "{}: {metric}",
                        dashboard.file_name().map_or_else(
                            || dashboard.display().to_string(),
                            |name| name.to_string_lossy().into_owned()
                        )
                    ));
                }
            }
        }
    }
    ensure!(
        undocumented.is_empty(),
        "dashboard expressions name metrics that docs/reference/metrics.md does not list; \
         point each panel at an emitted metric:\n  {}",
        undocumented.join("\n  ")
    );
    Ok(())
}

#[test]
fn metric_names_are_read_from_promql_expressions() {
    let names = metric_names(
        "histogram_quantile(0.95, sum by (le) (rate(zinder_query_request_duration_seconds_bucket[5m]))) \
         / zinder_build_info{service=\"zinder-query\"}",
    );

    assert_eq!(
        names,
        BTreeSet::from([
            "zinder_build_info".to_owned(),
            "zinder_query_request_duration_seconds_bucket".to_owned(),
        ])
    );
}

#[test]
fn derived_series_of_documented_metrics_count_as_listed() {
    let reference = DocumentedMetrics::parse(
        "| `zinder_query_request_duration_seconds` | histogram |\n\
         | `zinder_store_rocksdb_property` | gauge |",
    );

    assert!(reference.lists("zinder_query_request_duration_seconds_bucket"));
    assert!(reference.lists("zinder_store_rocksdb_property"));
    assert!(!reference.lists("zinder_query_request_total"));
    assert!(!reference.lists("zinder_compat_lightwalletd_writer_status_total"));
}

#[test]
fn a_dashboard_name_with_a_trailing_underscore_is_checked() {
    let reference = DocumentedMetrics::parse(
        "| `zinder_wallet_serving_pair_publisher_replica_lag_chain_epochs` | gauge |",
    );
    let names = metric_names("max(zinder_wallet_serving_pair_publisher_replica_lag_chain_epochs_)");

    assert_eq!(
        names,
        BTreeSet::from([
            "zinder_wallet_serving_pair_publisher_replica_lag_chain_epochs_".to_owned()
        ])
    );
    assert!(names.iter().all(|name| !reference.lists(name)));
}

#[test]
fn a_family_named_in_prose_lists_none_of_its_members() {
    let reference = DocumentedMetrics::parse(
        "| `zinder_store_rocksdb_property` | gauge |\n\
         The projector exports no `zinder_store_*` metrics, and the `zinder_readiness_*` \
         series carry `service`.",
    );

    assert!(reference.lists("zinder_store_rocksdb_property"));
    assert!(!reference.lists("zinder_store_rocksdb_proprety"));
    assert!(!reference.lists("zinder_readiness_state"));
}

/// Metric names the reference lists in full. A `name_*` family mentioned in
/// prose lists nothing, so a misspelled member still fails the gate.
struct DocumentedMetrics {
    names: BTreeSet<String>,
}

impl DocumentedMetrics {
    fn parse(reference: &str) -> Self {
        Self {
            names: metric_tokens(reference)
                .filter(|(name, followed_by_star)| !followed_by_star && !name.ends_with('_'))
                .map(|(name, _)| name)
                .collect(),
        }
    }

    fn lists(&self, metric: &str) -> bool {
        let base = DERIVED_SERIES_SUFFIXES
            .iter()
            .find_map(|suffix| metric.strip_suffix(suffix))
            .unwrap_or(metric);
        self.names.contains(metric) || self.names.contains(base)
    }
}

fn metric_names(expression: &str) -> BTreeSet<String> {
    metric_tokens(expression).map(|(name, _)| name).collect()
}

/// Every `zinder_` identifier in `text`, and whether a `*` follows it.
fn metric_tokens(text: &str) -> impl Iterator<Item = (String, bool)> + '_ {
    text.match_indices(METRIC_PREFIX)
        .filter_map(move |(start, _)| {
            let preceded_by_identifier = text[..start]
                .chars()
                .next_back()
                .is_some_and(|previous| previous.is_ascii_alphanumeric() || previous == '_');
            if preceded_by_identifier {
                return None;
            }
            let name: String = text[start..]
                .chars()
                .take_while(|character| {
                    character.is_ascii_lowercase()
                        || character.is_ascii_digit()
                        || *character == '_'
                })
                .collect();
            let followed_by_star = text[start + name.len()..].starts_with('*');
            Some((name, followed_by_star))
        })
}

fn collect_expressions<'dashboard>(
    node: &'dashboard Value,
    expressions: &mut Vec<&'dashboard str>,
) {
    match node {
        Value::Object(fields) => {
            for (key, field) in fields {
                match (key.as_str(), field) {
                    ("expr" | "query", Value::String(expression)) => {
                        expressions.push(expression);
                    }
                    _ => collect_expressions(field, expressions),
                }
            }
        }
        Value::Array(entries) => {
            for entry in entries {
                collect_expressions(entry, expressions);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn dashboard_files() -> Result<Vec<std::path::PathBuf>> {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join(DASHBOARD_DIRECTORY);
    let mut files = Vec::new();
    for entry in fs::read_dir(&directory)
        .map_err(|error| eyre!("cannot read {}: {error}", directory.display()))?
    {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}
