//! Drift probe between the documented `ZINDER_*` environment variables and a
//! service binary's real configuration loader.
//!
//! Binaries read `ZINDER_*` from the process environment and deserialize it
//! into `deny_unknown_fields` section structs. The probe runs the service
//! binary with `--print-config` and an otherwise empty environment, so the
//! verdict comes from the same loader an operator reaches at startup, without
//! mutating the test process environment.
//!
//! Two directions are checked:
//!
//! - [`EnvVarContractProbe::check_documented_variables_are_accepted`] fails on
//!   any documented variable that is not a key the loader accepts.
//! - [`EnvVarContractProbe::check_accepted_variables_are_documented`] fails on
//!   any accepted key without a documented name.
//!
//! The accepted keys come from the loader itself: the probe provokes its
//! unknown-field error at every section and reads the expected-field list.

use std::{collections::BTreeSet, path::PathBuf, process::Command};

use eyre::{Result, WrapErr, bail, eyre};

const ENV_PREFIX: &str = "ZINDER_";
const WALLET_SERVING_ENTRYPOINT: &str =
    include_str!("../../../deploy/wallet-serving-runtime-entrypoint");
const PROBE_SECTION: &str = "CONTRACT_PROBE";
const PROBE_LEAF: &str = "UNUSED";
const PROBE_KEY: &str = "contract_probe";

/// Runs one service binary's `--print-config` as a configuration-loader probe.
#[derive(Clone, Debug)]
pub struct EnvVarContractProbe {
    executable: PathBuf,
}

impl EnvVarContractProbe {
    /// Builds a probe for the binary at `executable`, usually
    /// `env!("CARGO_BIN_EXE_<name>")`.
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
        }
    }

    /// Fails when any of `documented_names` is not a configuration key the
    /// loader accepts.
    pub fn check_documented_variables_are_accepted<'a>(
        &self,
        documented_names: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        let accepted = self.accepted_variables()?;
        let rejected = documented_names
            .into_iter()
            .filter(|name| !accepted.contains(*name))
            .collect::<Vec<_>>();
        if rejected.is_empty() {
            return Ok(());
        }
        bail!(
            "{} documents variables its loader does not accept; fix the row or its `used_by` \
             list: {}",
            self.executable_name(),
            rejected.join(", "),
        );
    }

    /// Fails when the loader accepts a configuration key whose environment
    /// variable is missing from `documented_names`.
    ///
    /// Section structs shared between binaries accept keys that only some
    /// binaries consume. `accepted_but_unconsumed` lists those keys for this
    /// binary; every entry must be accepted by the loader and must not also
    /// be documented for the binary, so the list cannot go stale.
    pub fn check_accepted_variables_are_documented<'a>(
        &self,
        documented_names: impl IntoIterator<Item = &'a str>,
        accepted_but_unconsumed: &[&str],
    ) -> Result<()> {
        let documented = documented_names.into_iter().collect::<BTreeSet<&str>>();
        let accepted = self.accepted_variables()?;

        let mut problems = Vec::new();
        for name in &accepted {
            if !documented.contains(name.as_str())
                && !accepted_but_unconsumed.contains(&name.as_str())
            {
                problems.push(format!(
                    "{name}: accepted but no documented row names this binary"
                ));
            }
        }
        for name in accepted_but_unconsumed {
            if !accepted.contains(*name) {
                problems.push(format!(
                    "{name}: listed as accepted-but-unconsumed but the loader rejects it"
                ));
            } else if documented.contains(name) {
                problems.push(format!(
                    "{name}: listed as accepted-but-unconsumed but also documented for this binary"
                ));
            }
        }
        if problems.is_empty() {
            return Ok(());
        }
        bail!(
            "{} configuration keys and documented variables disagree:\n{}",
            self.executable_name(),
            problems.join("\n"),
        );
    }

    /// Fails unless `listed_sections` names exactly the top-level sections
    /// the loader defines.
    ///
    /// The wallet-serving image entrypoint passes each process only the
    /// `ZINDER_*` sections its binary defines, so a section missing from its
    /// list silently drops every variable of that section.
    pub fn check_top_level_sections<'a>(
        &self,
        listed_sections: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        let defined = self
            .accepted_field_names(&[])?
            .ok_or_else(|| {
                eyre!(
                    "{} did not reject an unknown top-level key",
                    self.executable_name()
                )
            })?
            .into_iter()
            .collect::<BTreeSet<String>>();
        let listed = listed_sections
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<String>>();
        if defined == listed {
            return Ok(());
        }
        bail!(
            "{} defines the sections {defined:?}, but the list names {listed:?}",
            self.executable_name(),
        );
    }

    fn accepted_variables(&self) -> Result<BTreeSet<String>> {
        let mut accepted = BTreeSet::new();
        self.collect_accepted_variables(&[], &mut accepted)?;
        Ok(accepted)
    }

    fn collect_accepted_variables(
        &self,
        section_path: &[String],
        accepted: &mut BTreeSet<String>,
    ) -> Result<()> {
        let Some(field_names) = self.accepted_field_names(section_path)? else {
            if section_path.is_empty() {
                bail!(
                    "{} did not reject an unknown top-level key; its loader must deny unknown \
                     fields for the probe to enumerate accepted variables",
                    self.executable_name(),
                );
            }
            accepted.insert(variable_name(section_path));
            return Ok(());
        };
        for field_name in field_names {
            let mut field_path = section_path.to_vec();
            field_path.push(field_name);
            self.collect_accepted_variables(&field_path, accepted)?;
        }
        Ok(())
    }

    /// Returns the field names the struct at `section_path` accepts, or `None`
    /// when the path is a leaf field rather than a section struct.
    fn accepted_field_names(&self, section_path: &[String]) -> Result<Option<Vec<String>>> {
        let probe_variable = if section_path.is_empty() {
            format!("{ENV_PREFIX}{PROBE_SECTION}__{PROBE_LEAF}")
        } else {
            format!(
                "{}__{PROBE_SECTION}__{PROBE_LEAF}",
                variable_name(section_path)
            )
        };
        let stderr = self.run_with_env(&[(probe_variable.as_str(), "1")])?;
        let marker = format!("unknown field `{PROBE_KEY}`, ");
        let Some((_, expectation)) = stderr.split_once(&marker) else {
            return Ok(None);
        };
        parse_expected_fields(expectation).map(Some).ok_or_else(|| {
            eyre!(
                "unrecognized unknown-field error from {}: {stderr}",
                self.executable_name()
            )
        })
    }

    fn run_with_env(&self, environment: &[(&str, &str)]) -> Result<String> {
        let output = Command::new(&self.executable)
            .arg("--print-config")
            .env_clear()
            .envs(environment.iter().copied())
            .output()
            .wrap_err_with(|| format!("failed to run {}", self.executable.display()))?;
        Ok(String::from_utf8_lossy(&output.stderr).into_owned())
    }

    fn executable_name(&self) -> &str {
        self.executable
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("service binary")
    }
}

/// Returns the sections the wallet-serving image entrypoint passes to one
/// process, from its `<process>_sections="..."` setting, such as
/// `ingest_sections`.
pub fn wallet_serving_entrypoint_sections(process: &str) -> Result<Vec<&'static str>> {
    let setting_prefix = format!("{process}_sections=\"");
    WALLET_SERVING_ENTRYPOINT
        .lines()
        .find_map(|line| line.strip_prefix(setting_prefix.as_str()))
        .and_then(|quoted| quoted.strip_suffix('"'))
        .map(|sections| sections.split_whitespace().collect())
        .ok_or_else(|| {
            eyre!("deploy/wallet-serving-runtime-entrypoint does not set {process}_sections")
        })
}

/// Renders a configuration path such as `["storage", "path"]` as
/// `ZINDER_STORAGE__PATH`.
fn variable_name(path: &[String]) -> String {
    let segments = path
        .iter()
        .map(|segment| segment.to_uppercase())
        .collect::<Vec<_>>();
    format!("{ENV_PREFIX}{}", segments.join("__"))
}

/// Extracts the field list from the tail of a serde unknown-field message:
/// ``expected one of `a`, `b` for key `x` ``, ``expected `a` or `b` ``,
/// ``expected `a` ``, or `there are no fields`.
fn parse_expected_fields(expectation: &str) -> Option<Vec<String>> {
    let expectation = expectation.lines().next().unwrap_or_default();
    if expectation.starts_with("there are no fields") {
        return Some(Vec::new());
    }
    let list = expectation.strip_prefix("expected ")?;
    let list = list.strip_prefix("one of ").unwrap_or(list);
    let list = list
        .split_once(" for key `")
        .map_or(list, |(fields, _)| fields);
    let field_names = list
        .split(", ")
        .flat_map(|segment| segment.split(" or "))
        .map(|quoted| quoted.trim().trim_matches('`').to_owned())
        .collect::<Vec<_>>();
    field_names
        .iter()
        .all(|field_name| !field_name.is_empty() && !field_name.contains('`'))
        .then_some(field_names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multi_field_expectation_with_key_suffix() {
        let parsed = parse_expected_fields("expected one of `path`, `canonical` for key `storage`");
        assert_eq!(
            parsed,
            Some(vec!["path".to_owned(), "canonical".to_owned()])
        );
    }

    #[test]
    fn parses_single_field_expectation() {
        let parsed = parse_expected_fields("expected `rocksdb` for key `storage.canonical`");
        assert_eq!(parsed, Some(vec!["rocksdb".to_owned()]));
    }

    #[test]
    fn parses_top_level_expectation_without_key_suffix() {
        let parsed = parse_expected_fields("expected one of `network`, `ops`");
        assert_eq!(parsed, Some(vec!["network".to_owned(), "ops".to_owned()]));
    }

    #[test]
    fn parses_two_field_expectation() {
        let parsed = parse_expected_fields(
            "expected `poll_interval_ms` or `lag_threshold_blocks` for key `ingest.follow`",
        );
        assert_eq!(
            parsed,
            Some(vec![
                "poll_interval_ms".to_owned(),
                "lag_threshold_blocks".to_owned()
            ])
        );
    }

    #[test]
    fn parses_empty_section() {
        assert_eq!(
            parse_expected_fields("there are no fields"),
            Some(Vec::new())
        );
    }

    #[test]
    fn renders_nested_path_as_double_underscore_name() {
        let path = ["storage".to_owned(), "canonical_path".to_owned()];
        assert_eq!(variable_name(&path), "ZINDER_STORAGE__CANONICAL_PATH");
    }
}
