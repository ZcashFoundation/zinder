#![allow(
    missing_docs,
    reason = "Integration test names describe the capability-docs contract under test."
)]

use std::collections::BTreeSet;

use eyre::{Result, eyre};
use zinder_proto::capabilities::{CapabilitySurface, capabilities_for_surface};

/// Deployment-wide capability strings the docs mirror.
///
/// The doc marker blocks list the consumer-facing wallet and explorer
/// capabilities; the private `ingest.*` control surface is not part of the
/// published capability list.
fn documented_capabilities() -> BTreeSet<String> {
    capabilities_for_surface(CapabilitySurface::Wallet)
        .chain(capabilities_for_surface(CapabilitySurface::Explorer))
        .map(|spec| spec.string.to_owned())
        .collect()
}

const CAPABILITIES_DOC: &str = include_str!("../../../../docs/reference/capabilities.md");
const TESTING_RUNBOOK_DOC: &str = include_str!("../../../../docs/runbooks/testing.md");
const NEXTEST_TOML: &str = include_str!("../../../../.config/nextest.toml");

const PUBLIC_INTERFACES_START: &str = "<!-- capability-list:public-interfaces:start -->";
const PUBLIC_INTERFACES_END: &str = "<!-- capability-list:public-interfaces:end -->";
const TESTING_RUNBOOK_START: &str = "<!-- capability-list:testing-runbook:start -->";
const TESTING_RUNBOOK_END: &str = "<!-- capability-list:testing-runbook:end -->";

/// `--profile=<name>` invocations the testing runbook quotes.
///
/// Each must resolve to an existing `[profile.<name>]` section in
/// `.config/nextest.toml` so an operator following the runbook hits a
/// real profile.
const RUNBOOK_REFERENCED_PROFILES: &[&str] = &["ci", "ci-perf", "ci-live", "ci-parity"];

#[test]
fn capabilities_reference_list_mirrors_zinder_capabilities() -> Result<()> {
    assert_capability_list_matches(
        "docs/reference/capabilities.md",
        CAPABILITIES_DOC,
        PUBLIC_INTERFACES_START,
        PUBLIC_INTERFACES_END,
    )
}

#[test]
fn testing_runbook_capability_list_mirrors_zinder_capabilities() -> Result<()> {
    assert_capability_list_matches(
        "docs/runbooks/testing.md",
        TESTING_RUNBOOK_DOC,
        TESTING_RUNBOOK_START,
        TESTING_RUNBOOK_END,
    )
}

#[test]
fn testing_runbook_default_filter_mirrors_nextest_toml() -> Result<()> {
    let quoted = TESTING_RUNBOOK_DOC
        .lines()
        .find(|line| line.contains("default-filter = "))
        .ok_or_else(|| eyre!("testing.md must quote the default-filter line"))?
        .trim()
        .trim_start_matches('`')
        .trim_end_matches('`');
    let toml_default_profile = NEXTEST_TOML
        .split("[profile.default]")
        .nth(1)
        .ok_or_else(|| eyre!(".config/nextest.toml must declare [profile.default]"))?
        .split("[profile.")
        .next()
        .ok_or_else(|| eyre!("could not isolate the default profile section"))?;
    let toml_default_filter = toml_default_profile
        .lines()
        .find(|line| line.trim_start().starts_with("default-filter = "))
        .ok_or_else(|| eyre!("default profile must set default-filter"))?
        .trim();
    assert!(
        quoted.contains(toml_default_filter) || toml_default_filter.contains(quoted),
        "default-filter quoted in docs/runbooks/testing.md does not match \
         the value in .config/nextest.toml.\n  runbook: {quoted}\n  nextest: {toml_default_filter}\n\
         When you change the default-filter expression in either place, \
         update the other so operators do not chase a stale invocation."
    );
    Ok(())
}

#[test]
fn testing_runbook_profile_names_exist_in_nextest_toml() {
    for profile in RUNBOOK_REFERENCED_PROFILES {
        let toml_marker = format!("[profile.{profile}]");
        assert!(
            NEXTEST_TOML.contains(&toml_marker),
            "testing.md references `--profile={profile}` but .config/nextest.toml \
             has no {toml_marker} section. Either add the profile or remove the \
             reference from the runbook."
        );
        let runbook_marker = format!("--profile={profile}");
        assert!(
            TESTING_RUNBOOK_DOC.contains(&runbook_marker),
            "expected the testing runbook to mention --profile={profile}; \
             if the profile is intentionally workspace-only and not part of \
             the runbook contract, drop it from RUNBOOK_REFERENCED_PROFILES."
        );
    }
}

fn assert_capability_list_matches(
    doc_path: &str,
    doc: &str,
    start_marker: &str,
    end_marker: &str,
) -> Result<()> {
    let parsed = parse_capability_list(doc, start_marker, end_marker)?;
    let expected = documented_capabilities();

    let missing_from_doc: Vec<&String> = expected.difference(&parsed).collect();
    let extra_in_doc: Vec<&String> = parsed.difference(&expected).collect();

    assert!(
        missing_from_doc.is_empty() && extra_in_doc.is_empty(),
        "{doc_path} capability list has drifted from the CAPABILITIES table.\n\
         Missing from doc: {missing_from_doc:?}\n\
         Extra in doc:    {extra_in_doc:?}\n\
         Update the doc list (or update the CAPABILITIES table if a capability \
         was added/removed) so the two stay in sync."
    );
    Ok(())
}

fn parse_capability_list(
    doc: &str,
    start_marker: &str,
    end_marker: &str,
) -> Result<BTreeSet<String>> {
    let after_start = doc
        .split_once(start_marker)
        .ok_or_else(|| eyre!("missing start marker {start_marker}"))?
        .1;
    let block = after_start
        .split_once(end_marker)
        .ok_or_else(|| eyre!("missing end marker {end_marker}"))?
        .0;
    Ok(block.lines().filter_map(parse_capability_line).collect())
}

fn parse_capability_line(line: &str) -> Option<String> {
    let trimmed = line.trim().trim_start_matches("- ").trim_matches('`');
    if trimmed.is_empty() || trimmed.starts_with("```") {
        return None;
    }
    if !trimmed.contains('.') {
        return None;
    }
    let (_, suffix) = trimmed.rsplit_once("_v")?;
    if suffix.chars().all(|character| character.is_ascii_digit()) {
        Some(trimmed.to_owned())
    } else {
        None
    }
}
