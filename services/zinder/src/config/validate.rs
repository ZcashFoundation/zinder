use std::path::Path;

use tempfile::TempDir;
use toml::Value as TomlValue;
use zinder_compat_cipherscan::runtime::{
    CipherscanConfigOverrides, cipherscan_config_toml, load_cipherscan_config_with_env,
};
use zinder_compat_lightwalletd::runtime::{
    LightwalletdConfigOverrides, lightwalletd_config_toml, load_history_publisher_config_with_env,
    load_lightwalletd_config_with_env,
};
use zinder_explorer::runtime::{
    ExplorerConfigOverrides, explorer_config_toml, load_explorer_config_with_env,
};
use zinder_ingest::runtime::{
    IngestConfigOverrides, load_ingest_config_with_env, redacted_ingest_config_toml,
};
use zinder_projector::runtime::{
    ProjectorConfigOverrides, load_projector_config_with_env, projector_config_toml,
};
use zinder_query::runtime::{QueryConfigOverrides, load_query_config_with_env, query_config_toml};

use crate::cli::ServiceRole;

use super::{HistoryPublisherPlan, RoleLaunchSpec, SupervisionPlan, UnifiedConfigError};

/// Collected per-role redacted TOML outputs produced during validation.
#[derive(Clone, Debug, Default)]
pub(crate) struct ValidatedRoleRenderings {
    pub(super) ops_listen_addr: String,
    pub(super) role_tomls: Vec<(ServiceRole, String)>,
    pub(super) history_publisher: Option<HistoryPublisherPlan>,
}

pub(super) fn validate_all_roles(
    plan: &SupervisionPlan,
) -> Result<ValidatedRoleRenderings, UnifiedConfigError> {
    let dir = TempDir::new()?;
    let mut role_tomls = Vec::with_capacity(plan.roles.len());

    for (idx, spec) in plan.roles.iter().enumerate() {
        let toml_path = write_validation_toml(dir.path(), idx, spec)?;
        let env_pairs = validation_env_for(spec);
        let rendered = validate_single_role(spec.role, &toml_path, env_pairs)?;
        role_tomls.push((spec.role, rendered));
    }

    Ok(ValidatedRoleRenderings {
        ops_listen_addr: plan
            .ops_listen_addr
            .map_or_else(String::new, |addr| addr.to_string()),
        role_tomls,
        history_publisher: plan.history_publisher.clone(),
    })
}

fn write_validation_toml(
    dir: &Path,
    index: usize,
    spec: &RoleLaunchSpec,
) -> Result<std::path::PathBuf, UnifiedConfigError> {
    let path = dir.join(format!("{index}-{}.toml", spec.role.label()));
    std::fs::write(&path, &spec.filtered_toml)?;
    Ok(path)
}

fn validation_env_for(spec: &RoleLaunchSpec) -> Vec<(String, String)> {
    let mut env_pairs: Vec<(String, String)> = spec
        .filtered_env
        .iter()
        .map(|(key, env_val)| (key.clone(), env_val.clone()))
        .collect();
    // Validation disables child ops listeners so role validation never reflects stale ports.
    env_pairs.push(("ZINDER_OPS__LISTEN_ADDR".to_owned(), String::new()));
    env_pairs
}

fn validate_single_role(
    role: ServiceRole,
    toml_path: &Path,
    env_pairs: Vec<(String, String)>,
) -> Result<String, UnifiedConfigError> {
    let path_opt = Some(toml_path.to_path_buf());
    match role {
        ServiceRole::Ingest => {
            let loaded =
                load_ingest_config_with_env(path_opt, IngestConfigOverrides::default(), env_pairs)
                    .map_err(|err| role_err(role, &err.to_string()))?;
            redacted_ingest_config_toml(&loaded).map_err(|err| role_err(role, &err.to_string()))
        }
        ServiceRole::Projector => {
            let loaded = load_projector_config_with_env(
                path_opt,
                ProjectorConfigOverrides::default(),
                env_pairs,
            )
            .map_err(|err| role_err(role, &err.to_string()))?;
            projector_config_toml(&loaded).map_err(|err| role_err(role, &err.to_string()))
        }
        ServiceRole::Query => {
            let loaded =
                load_query_config_with_env(path_opt, QueryConfigOverrides::default(), env_pairs)
                    .map_err(|err| role_err(role, &err.to_string()))?;
            query_config_toml(&loaded).map_err(|err| role_err(role, &err.to_string()))
        }
        ServiceRole::CompatLightwalletd => {
            let loaded = load_lightwalletd_config_with_env(
                path_opt,
                LightwalletdConfigOverrides::default(),
                env_pairs,
            )
            .map_err(|err| role_err(role, &err.to_string()))?;
            lightwalletd_config_toml(&loaded).map_err(|err| role_err(role, &err.to_string()))
        }
        ServiceRole::Explorer => {
            let loaded = load_explorer_config_with_env(
                path_opt,
                ExplorerConfigOverrides::default(),
                env_pairs,
            )
            .map_err(|err| role_err(role, &err.to_string()))?;
            explorer_config_toml(&loaded).map_err(|err| role_err(role, &err.to_string()))
        }
        ServiceRole::CompatCipherscan => {
            let loaded = load_cipherscan_config_with_env(
                path_opt,
                CipherscanConfigOverrides::default(),
                env_pairs,
            )
            .map_err(|err| role_err(role, &err.to_string()))?;
            cipherscan_config_toml(&loaded).map_err(|err| role_err(role, &err.to_string()))
        }
        ServiceRole::HistoryPublisher => {
            let loaded = load_history_publisher_config_with_env(
                path_opt,
                LightwalletdConfigOverrides::default(),
                env_pairs,
            )
            .map_err(|err| role_err(role, &err.to_string()))?;
            loaded
                .to_toml()
                .map_err(|err| role_err(role, &err.to_string()))
        }
    }
}

fn role_err(role: ServiceRole, detail: &str) -> UnifiedConfigError {
    UnifiedConfigError::RoleValidation {
        role: role.label(),
        detail: detail.to_owned(),
    }
}

/// Renders one redacted combined TOML document from all active validated roles.
#[must_use]
pub fn render_combined_redacted_toml(plan: &SupervisionPlan) -> String {
    let mut combined = toml::Table::new();
    for (role, role_toml_str) in &plan.redacted_sections.role_tomls {
        if let Ok(parsed) = toml::from_str::<toml::Table>(role_toml_str) {
            merge_role_rendered_table(&mut combined, *role, parsed);
        }
    }
    let mut ops_tbl = toml::Table::new();
    ops_tbl.insert(
        "listen_addr".to_owned(),
        TomlValue::String(plan.redacted_sections.ops_listen_addr.clone()),
    );
    combined.insert("ops".to_owned(), TomlValue::Table(ops_tbl));

    if let Some(publisher) = &plan.redacted_sections.history_publisher {
        combined.insert(
            "history_publisher".to_owned(),
            TomlValue::Table(redacted_history_publisher_table(publisher)),
        );
    }

    toml::to_string_pretty(&combined).unwrap_or_default()
}

fn merge_role_rendered_table(combined: &mut toml::Table, role: ServiceRole, parsed: toml::Table) {
    for (section, section_val) in parsed {
        match section.as_str() {
            "ops" => {}
            "deployment" | "network" | "security" | "node" | "node_fallbacks" => {
                combined.entry(section).or_insert(section_val);
            }
            "storage" | "wallet" | "ingest_control" => {
                merge_subtable(combined, &section, section_val);
            }
            "compat" if role == ServiceRole::HistoryPublisher => {}
            _ => {
                combined.insert(section, section_val);
            }
        }
    }
}

fn merge_subtable(combined: &mut toml::Table, section: &str, incoming: TomlValue) {
    let TomlValue::Table(incoming_tbl) = incoming else {
        return;
    };
    let entry = combined
        .entry(section.to_owned())
        .or_insert_with(|| TomlValue::Table(toml::Table::new()));
    let TomlValue::Table(target) = entry else {
        return;
    };
    for (field, field_val) in incoming_tbl {
        target.entry(field).or_insert(field_val);
    }
}

fn redacted_history_publisher_table(publisher: &HistoryPublisherPlan) -> toml::Table {
    let mut tbl = toml::Table::new();
    tbl.insert(
        "directory".to_owned(),
        TomlValue::String(publisher.directory.display().to_string()),
    );
    tbl.insert(
        "signing_key".to_owned(),
        TomlValue::String("[REDACTED]".to_owned()),
    );
    tbl.insert(
        "follow_interval_secs".to_owned(),
        TomlValue::Integer(i64::from(publisher.follow_interval_secs.get())),
    );
    if let Some(height) = publisher.first_height {
        tbl.insert(
            "first_height".to_owned(),
            TomlValue::Integer(i64::from(height)),
        );
    }
    if let Some(segment) = publisher.segment_blocks {
        tbl.insert(
            "segment_blocks".to_owned(),
            TomlValue::Integer(i64::from(segment.get())),
        );
    }
    if let Some(workers) = publisher.workers {
        tbl.insert(
            "workers".to_owned(),
            TomlValue::Integer(i64::try_from(workers.get()).unwrap_or(i64::MAX)),
        );
    }
    tbl
}
