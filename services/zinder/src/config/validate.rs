use toml::Value as TomlValue;
use zinder_compat_cipherscan::runtime::{
    cipherscan_config_toml, resolve_cipherscan_config_from_raw,
};
use zinder_compat_lightwalletd::runtime::{
    lightwalletd_config_toml, resolve_lightwalletd_config_from_raw,
};
use zinder_explorer::runtime::{explorer_config_toml, resolve_explorer_config_from_raw};
use zinder_ingest::runtime::{redacted_ingest_config_toml, resolve_ingest_config_from_raw};
use zinder_projector::runtime::{projector_config_toml, resolve_projector_config_from_raw};
use zinder_query::runtime::{query_config_toml, resolve_query_config_from_raw};
use zinder_runtime::OpsSection;

use crate::cli::ServiceRole;

use super::{RoleLaunchSpec, SupervisionPlan, UnifiedConfigError};

/// Collected per-role redacted TOML outputs produced during validation.
#[derive(Clone, Debug, Default)]
pub(crate) struct ValidatedRoleRenderings {
    pub(super) role_tomls: Vec<(ServiceRole, String)>,
}

pub(super) fn validate_all_roles(
    plan: &SupervisionPlan,
) -> Result<ValidatedRoleRenderings, UnifiedConfigError> {
    let mut role_tomls = Vec::with_capacity(plan.roles.len());
    let has_wallet_query_consumers = plan.roles.iter().any(|spec| {
        matches!(
            spec.role,
            ServiceRole::Explorer | ServiceRole::CompatCipherscan
        )
    });

    for spec in &plan.roles {
        let rendered = validate_single_role(spec, has_wallet_query_consumers)?;
        role_tomls.push((spec.role, rendered));
    }

    Ok(ValidatedRoleRenderings { role_tomls })
}

fn validate_single_role(
    spec: &RoleLaunchSpec,
    has_wallet_query_consumers: bool,
) -> Result<String, UnifiedConfigError> {
    let role = spec.role;
    let raw = spec.validation_config.clone();
    match role {
        ServiceRole::Ingest => {
            let loaded = resolve_ingest_config_from_raw(raw)
                .map_err(|err| role_err(role, &err.to_string()))?;
            redacted_ingest_config_toml(&loaded).map_err(|err| role_err(role, &err.to_string()))
        }
        ServiceRole::Projector => {
            let loaded = resolve_projector_config_from_raw(raw)
                .map_err(|err| role_err(role, &err.to_string()))?;
            projector_config_toml(&loaded).map_err(|err| role_err(role, &err.to_string()))
        }
        ServiceRole::Query => {
            let loaded = resolve_query_config_from_raw(raw)
                .map_err(|err| role_err(role, &err.to_string()))?;
            if has_wallet_query_consumers && loaded.proxy_protocol.is_enabled() {
                return Err(role_err(
                    role,
                    "query.proxy_protocol_trusted_cidrs is incompatible with the combined explorer preset. Remove the query setting and unset ZINDER_QUERY__PROXY_PROTOCOL_TRUSTED_CIDRS, or run the roles separately with both wallet_query_endpoint settings pointing to a proxy that sends a PROXY preface.",
                ));
            }
            query_config_toml(&loaded).map_err(|err| role_err(role, &err.to_string()))
        }
        ServiceRole::CompatLightwalletd => {
            let loaded = resolve_lightwalletd_config_from_raw(raw)
                .map_err(|err| role_err(role, &err.to_string()))?;
            lightwalletd_config_toml(&loaded).map_err(|err| role_err(role, &err.to_string()))
        }
        ServiceRole::Explorer => {
            let loaded = resolve_explorer_config_from_raw(raw)
                .map_err(|err| role_err(role, &err.to_string()))?;
            explorer_config_toml(&loaded).map_err(|err| role_err(role, &err.to_string()))
        }
        ServiceRole::CompatCipherscan => {
            let loaded = resolve_cipherscan_config_from_raw(raw)
                .map_err(|err| role_err(role, &err.to_string()))?;
            cipherscan_config_toml(&loaded).map_err(|err| role_err(role, &err.to_string()))
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
    let ops_section = OpsSection::from_resolved(plan.ops_listen_addr);
    if let Ok(ops_tbl) = toml::Table::try_from(ops_section) {
        combined.insert("ops".to_owned(), TomlValue::Table(ops_tbl));
    }

    toml::to_string_pretty(&combined).unwrap_or_default()
}

fn merge_role_rendered_table(combined: &mut toml::Table, _role: ServiceRole, parsed: toml::Table) {
    for (section, section_val) in parsed {
        match section.as_str() {
            "ops" => {}
            "deployment" | "network" | "security" | "node" | "node_fallbacks" => {
                combined.entry(section).or_insert(section_val);
            }
            "storage" | "wallet" | "ingest_control" => {
                merge_subtable(combined, &section, section_val);
            }
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
