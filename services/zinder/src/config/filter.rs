use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
};

use serde::Deserialize;
use toml::Value as TomlValue;
use zinder_core::Network;
use zinder_runtime::{
    ConfigError, ConfigLoader, DeploymentProfile, DeploymentSection, IngestControlSection,
    NetworkSection, OpsSection, ProjectorControlSection, SecuritySection, derive_canonical_path,
    derive_canonical_secondary_path, derive_checkpoints_path, derive_explorer_secondary_path,
    derive_wallet_path, guard_optional_serving_bind, parse_socket_addr, resolve_allow_public_bind,
    resolve_ops_listen_addr, section_defaults::DEFAULT_DATA_DIR, validate_optional_data_dir,
};

use crate::{
    cli::{Preset, ServiceRole},
    healthcheck::DEFAULT_SUPERVISOR_OPS_LISTEN_ADDR,
};

use super::{
    ConfigScope, RoleLaunchSpec, SupervisionPlan, SupervisorMode, UnifiedConfigError,
    validate::ValidatedRoleRenderings,
};

const DEFAULT_INDEX_OPS_LISTEN_ADDR: &str = "127.0.0.1:9105";
const DEFAULT_INGEST_CONTROL_ADDR: &str = "127.0.0.1:9100";
const DEFAULT_QUERY_ADDR: &str = "127.0.0.1:9102";
const DEFAULT_EXPLORER_ADDR: &str = "127.0.0.1:9068";

const ALLOWED_TOP_LEVEL_SECTIONS: &[&str] = &[
    "deployment",
    "network",
    "ops",
    "security",
    "storage",
    "wallet",
    "ingest_control",
    "projector_control",
    "node",
    "node_fallbacks",
    "ingest",
    "retention",
    "projector",
    "query",
    "compat",
    "explorer",
    "cipherscan",
];

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct CombinedMetaRaw {
    deployment: DeploymentSection,
    network: NetworkSection,
    ops: OpsSection,
    security: SecuritySection,
    storage: CombinedStorageMeta,
    wallet: CombinedWalletMeta,
    ingest_control: IngestControlSection,
    projector_control: ProjectorControlSection,
    query: ListenOnlySection,
    explorer: ExplorerMetaSection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CombinedStorageMeta {
    data_dir: Option<PathBuf>,
    path: Option<PathBuf>,
    canonical_path: Option<PathBuf>,
    canonical_secondary_path: Option<PathBuf>,
    secondary_path: Option<PathBuf>,
    raw_blob_policy: Option<String>,
    full_blocks: Option<bool>,
    secondary_catchup_interval_ms: Option<u64>,
    initial_catchup_timeout_ms: Option<u64>,
    secondary_replica_lag_threshold_chain_epochs: Option<u64>,
    serving_pair_staleness_ceiling_ms: Option<u64>,
    canonical: toml::Table,
    materialized_views: toml::Table,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CombinedWalletMeta {
    path: Option<PathBuf>,
    secondary_path: Option<PathBuf>,
    rocksdb: toml::Table,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ListenOnlySection {
    listen_addr: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ExplorerMetaSection {
    listen_addr: Option<String>,
    wallet_query_endpoint: Option<String>,
}

struct DerivedWiring {
    profile: DeploymentProfile,
    inject_deployment_profile: bool,
    network: Network,
    ops_listen_addr: Option<SocketAddr>,
    full_blocks: bool,
    effective_data_dir: Option<PathBuf>,
    canonical_path: PathBuf,
    wallet_path: Option<PathBuf>,
    secondary_base: Option<PathBuf>,
    checkpoint_staging_root: Option<PathBuf>,
    ingest_control_addr: Option<SocketAddr>,
    ingest_control_url: Option<String>,
    query_listen_addr: Option<SocketAddr>,
    explorer_listen_addr: Option<SocketAddr>,
    generated_read_token: Option<String>,
    generated_checkpoint_token: Option<String>,
    generated_projector_token: Option<String>,
}

pub(super) fn build_supervision_plan(
    scope: &ConfigScope,
    env_entries: &[(String, String)],
) -> Result<SupervisionPlan, UnifiedConfigError> {
    let file_table = load_checked_file_table(scope.config_path.as_deref())?;
    validate_zinder_env_sections(env_entries)?;
    let meta = load_combined_meta(scope, env_entries)?;
    let cli_data_dir = validate_optional_data_dir(scope.data_dir.clone())?;
    let meta_data_dir = validate_optional_data_dir(meta.storage.data_dir.clone())?;
    let effective_data_dir = cli_data_dir.or(meta_data_dir);
    let explicit_profile = scope
        .preset
        .map(Preset::deployment_profile)
        .or(meta.deployment.profile);
    let profile = explicit_profile.unwrap_or(DeploymentProfile::Wallet);
    let active_roles = active_roles_for(scope.mode, profile, scope.readers_only)?;
    let wiring = derive_wiring(
        scope,
        &meta,
        (profile, true),
        effective_data_dir,
        &active_roles,
    )?;

    let mut roles = Vec::with_capacity(active_roles.len());
    for role in active_roles {
        roles.push(build_role_spec(role, &file_table, env_entries, &wiring)?);
    }

    Ok(SupervisionPlan {
        mode: scope.mode,
        profile,
        network: wiring.network,
        ops_listen_addr: wiring.ops_listen_addr,
        full_blocks: wiring.full_blocks,
        canonical_path: wiring.canonical_path,
        wallet_path: wiring.wallet_path,
        ingest_control_addr: wiring.ingest_control_addr,
        query_listen_addr: wiring.query_listen_addr,
        explorer_listen_addr: wiring.explorer_listen_addr,
        roles,
        redacted_sections: ValidatedRoleRenderings::default(),
    })
}

fn load_checked_file_table(config_path: Option<&Path>) -> Result<toml::Table, UnifiedConfigError> {
    let Some(path) = config_path else {
        return Ok(toml::Table::new());
    };
    let text =
        std::fs::read_to_string(path).map_err(|source| UnifiedConfigError::ReadConfigFile {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
    let table = toml::from_str::<toml::Table>(&text).map_err(|source| {
        UnifiedConfigError::ReadConfigFile {
            path: path.display().to_string(),
            detail: source.to_string(),
        }
    })?;
    for section in table.keys() {
        if !ALLOWED_TOP_LEVEL_SECTIONS.contains(&section.as_str()) {
            return Err(ConfigError::invalid(format!(
                "unknown configuration section `{section}` in {}",
                path.display()
            ))
            .into());
        }
    }
    Ok(table)
}

fn validate_zinder_env_sections(env_entries: &[(String, String)]) -> Result<(), ConfigError> {
    for (env_name, _) in env_entries {
        if env_name.starts_with("ZINDER_TEST_") {
            continue;
        }
        let Some(suffix) = env_name.strip_prefix("ZINDER_") else {
            continue;
        };
        let top_level = suffix
            .split_once("__")
            .map_or(suffix, |(section, _)| section)
            .to_ascii_lowercase();
        if !ALLOWED_TOP_LEVEL_SECTIONS.contains(&top_level.as_str()) {
            return Err(ConfigError::invalid(format!(
                "unrecognized Zinder environment variable section `{top_level}` in `{env_name}`"
            )));
        }
    }
    Ok(())
}

fn load_combined_meta(
    scope: &ConfigScope,
    env_entries: &[(String, String)],
) -> Result<CombinedMetaRaw, ConfigError> {
    let default_ops = match scope.mode {
        SupervisorMode::Serve => DEFAULT_SUPERVISOR_OPS_LISTEN_ADDR,
        SupervisorMode::Index => DEFAULT_INDEX_OPS_LISTEN_ADDR,
    };
    let loader = ConfigLoader::new()
        .with_default("ops.listen_addr", default_ops)?
        .with_security_section()?
        .with_file(scope.config_path.clone())
        .with_zinder_env_from_map(env_entries.iter().cloned())?;
    loader.load()
}

fn active_roles_for(
    mode: SupervisorMode,
    profile: DeploymentProfile,
    readers_only: bool,
) -> Result<Vec<ServiceRole>, UnifiedConfigError> {
    if mode == SupervisorMode::Index {
        return match profile {
            DeploymentProfile::LightWallet => Ok(vec![ServiceRole::Ingest]),
            DeploymentProfile::Wallet
            | DeploymentProfile::WalletArchive
            | DeploymentProfile::Explorer => Ok(vec![ServiceRole::Ingest, ServiceRole::Projector]),
            _ => Err(UnifiedConfigError::Invalid(format!(
                "unsupported deployment profile `{}`",
                profile.as_str()
            ))),
        };
    }
    let mut roles = Vec::new();
    match profile {
        DeploymentProfile::LightWallet => {
            if !readers_only {
                roles.push(ServiceRole::Ingest);
            }
            roles.push(ServiceRole::CompatLightwalletd);
        }
        DeploymentProfile::Wallet | DeploymentProfile::WalletArchive => {
            if !readers_only {
                roles.push(ServiceRole::Ingest);
                roles.push(ServiceRole::Projector);
            }
            roles.push(ServiceRole::Query);
            roles.push(ServiceRole::CompatLightwalletd);
        }
        DeploymentProfile::Explorer => {
            if !readers_only {
                roles.push(ServiceRole::Ingest);
                roles.push(ServiceRole::Projector);
            }
            roles.push(ServiceRole::Query);
            roles.push(ServiceRole::CompatLightwalletd);
            roles.push(ServiceRole::Explorer);
            roles.push(ServiceRole::CompatCipherscan);
        }
        _ => {
            return Err(UnifiedConfigError::Invalid(format!(
                "unsupported deployment profile `{}`",
                profile.as_str()
            )));
        }
    }
    Ok(roles)
}

fn derive_wiring(
    scope: &ConfigScope,
    meta: &CombinedMetaRaw,
    profile_mode: (DeploymentProfile, bool),
    effective_data_dir: Option<PathBuf>,
    active_roles: &[ServiceRole],
) -> Result<DerivedWiring, UnifiedConfigError> {
    let (profile, inject_deployment_profile) = profile_mode;
    let network = meta.network.clone().resolve()?;
    let ops_listen_addr = resolve_ops_listen_addr(meta.ops.clone())?;
    let allow_public_bind = resolve_allow_public_bind(meta.security)?;
    guard_optional_serving_bind("ops.listen_addr", ops_listen_addr, allow_public_bind)?;
    let full_blocks = scope.full_blocks
        || meta.storage.full_blocks == Some(true)
        || profile == DeploymentProfile::WalletArchive;
    let has_ingest = active_roles.contains(&ServiceRole::Ingest);
    let has_consumers = active_roles.iter().any(|role| {
        matches!(
            role,
            ServiceRole::Projector | ServiceRole::Query | ServiceRole::CompatLightwalletd
        )
    });
    let canonical_path = resolve_canonical_primary(&meta.storage, effective_data_dir.as_deref());
    let wallet_path = resolve_wallet_primary(profile, &meta.wallet, effective_data_dir.as_deref());
    let secondary_base = resolve_secondary_base(
        &meta.storage,
        effective_data_dir.as_deref(),
        &canonical_path,
    );
    let checkpoint_staging_root =
        resolve_checkpoint_staging_root(meta, effective_data_dir.as_deref());
    let (ingest_control_addr, ingest_control_url) =
        resolve_ingest_control_wiring(meta, has_ingest && has_consumers)?;
    let (generated_read_token, generated_checkpoint_token) =
        generate_internal_tokens(meta, has_ingest && has_consumers)?;
    let generated_projector_token =
        generate_projector_control_token(meta, active_roles.contains(&ServiceRole::Projector))?;
    let (query_listen_addr, explorer_listen_addr) = resolve_explorer_endpoints(meta, active_roles)?;

    Ok(DerivedWiring {
        profile,
        inject_deployment_profile,
        network,
        ops_listen_addr,
        full_blocks,
        effective_data_dir,
        canonical_path,
        wallet_path,
        secondary_base,
        checkpoint_staging_root,
        ingest_control_addr,
        ingest_control_url,
        query_listen_addr,
        explorer_listen_addr,
        generated_read_token,
        generated_checkpoint_token,
        generated_projector_token,
    })
}

fn resolve_canonical_primary(storage: &CombinedStorageMeta, data_dir: Option<&Path>) -> PathBuf {
    storage
        .path
        .clone()
        .or_else(|| storage.canonical_path.clone())
        .unwrap_or_else(|| {
            let root = data_dir.unwrap_or_else(|| Path::new(DEFAULT_DATA_DIR));
            derive_canonical_path(root)
        })
}

fn resolve_wallet_primary(
    profile: DeploymentProfile,
    wallet: &CombinedWalletMeta,
    data_dir: Option<&Path>,
) -> Option<PathBuf> {
    if profile == DeploymentProfile::LightWallet {
        return None;
    }
    Some(wallet.path.clone().unwrap_or_else(|| {
        let root = data_dir.unwrap_or_else(|| Path::new(DEFAULT_DATA_DIR));
        derive_wallet_path(root)
    }))
}

fn resolve_secondary_base(
    storage: &CombinedStorageMeta,
    data_dir: Option<&Path>,
    canonical_path: &Path,
) -> Option<PathBuf> {
    if let Some(dir) = data_dir {
        return Some(dir.to_path_buf());
    }
    if storage.path.is_some() && storage.secondary_path.is_none() {
        let parent = canonical_path.parent().unwrap_or_else(|| Path::new("."));
        return Some(parent.join(".secondaries"));
    }
    storage.secondary_path.clone()
}

fn resolve_checkpoint_staging_root(
    meta: &CombinedMetaRaw,
    effective_data_dir: Option<&Path>,
) -> Option<PathBuf> {
    if meta.ingest_control.checkpoint_staging_root.is_some()
        || meta.projector_control.checkpoint_staging_root.is_some()
    {
        return None;
    }
    effective_data_dir.map(derive_checkpoints_path)
}

fn resolve_ingest_control_wiring(
    meta: &CombinedMetaRaw,
    co_supervised: bool,
) -> Result<(Option<SocketAddr>, Option<String>), UnifiedConfigError> {
    if !co_supervised {
        return Ok((None, None));
    }
    let listen_text = meta
        .ingest_control
        .listen_addr
        .as_deref()
        .unwrap_or(DEFAULT_INGEST_CONTROL_ADDR);
    if listen_text.trim().is_empty() {
        return Err(UnifiedConfigError::Invalid(
            "ingest_control.listen_addr cannot be empty when supervising writer alongside readers"
                .to_owned(),
        ));
    }
    let socket_addr = parse_socket_addr("ingest_control.listen_addr", listen_text)?;
    let dial_url = meta
        .ingest_control
        .addr
        .clone()
        .unwrap_or_else(|| format!("http://{}", loopback_target(socket_addr)));
    Ok((Some(socket_addr), Some(dial_url)))
}

fn generate_internal_tokens(
    meta: &CombinedMetaRaw,
    co_supervised: bool,
) -> Result<(Option<String>, Option<String>), UnifiedConfigError> {
    if !co_supervised {
        return Ok((None, None));
    }
    let operator_configured = meta.ingest_control.bearer_token.is_some()
        || meta.ingest_control.bearer_token_path.is_some()
        || meta.ingest_control.checkpoint_bearer_token.is_some()
        || meta.ingest_control.checkpoint_bearer_token_path.is_some();
    if operator_configured {
        return Ok((None, None));
    }
    Ok((Some(random_hex_token_32()?), Some(random_hex_token_32()?)))
}

fn generate_projector_control_token(
    meta: &CombinedMetaRaw,
    has_projector: bool,
) -> Result<Option<String>, UnifiedConfigError> {
    if !has_projector {
        return Ok(None);
    }
    if meta.projector_control.bearer_token.is_some()
        || meta.projector_control.bearer_token_path.is_some()
    {
        return Ok(None);
    }
    Ok(Some(random_hex_token_32()?))
}

fn random_hex_token_32() -> Result<String, UnifiedConfigError> {
    let mut raw_bytes = [0_u8; 32];
    getrandom::fill(&mut raw_bytes).map_err(|source| {
        UnifiedConfigError::Invalid(format!(
            "failed to generate loopback bearer token: {source}"
        ))
    })?;
    Ok(hex::encode(raw_bytes))
}

fn resolve_explorer_endpoints(
    meta: &CombinedMetaRaw,
    active_roles: &[ServiceRole],
) -> Result<(Option<SocketAddr>, Option<SocketAddr>), UnifiedConfigError> {
    let query_addr = if active_roles.contains(&ServiceRole::Query)
        && active_roles.contains(&ServiceRole::Explorer)
    {
        let raw = meta
            .query
            .listen_addr
            .as_deref()
            .unwrap_or(DEFAULT_QUERY_ADDR);
        Some(parse_socket_addr("query.listen_addr", raw)?)
    } else {
        None
    };
    let explorer_addr = if active_roles.contains(&ServiceRole::Explorer)
        && active_roles.contains(&ServiceRole::CompatCipherscan)
    {
        let raw = meta
            .explorer
            .listen_addr
            .as_deref()
            .unwrap_or(DEFAULT_EXPLORER_ADDR);
        Some(parse_socket_addr("explorer.listen_addr", raw)?)
    } else {
        None
    };
    Ok((query_addr, explorer_addr))
}

const fn loopback_target(addr: SocketAddr) -> SocketAddr {
    match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
        }
        IpAddr::V6(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), addr.port())
        }
        IpAddr::V4(_) | IpAddr::V6(_) => addr,
    }
}

fn build_role_spec(
    role: ServiceRole,
    file_table: &toml::Table,
    env_entries: &[(String, String)],
    wiring: &DerivedWiring,
) -> Result<RoleLaunchSpec, UnifiedConfigError> {
    let mut table = filter_toml_for_role(role, file_table, wiring);
    apply_role_overrides(role, &mut table, wiring)?;
    let filtered_toml = toml::to_string(&table).map_err(|source| {
        UnifiedConfigError::Invalid(format!("TOML serialize failed: {source}"))
    })?;
    let mut filtered_env = filter_env_for_role(role, env_entries, wiring);
    apply_role_env_overrides(role, &mut filtered_env, wiring);
    Ok(RoleLaunchSpec {
        role,
        filtered_toml,
        filtered_env,
        required_for_readyz: true,
    })
}

fn filter_toml_for_role(
    role: ServiceRole,
    source: &toml::Table,
    wiring: &DerivedWiring,
) -> toml::Table {
    let mut out = toml::Table::new();
    for (section, section_val) in source {
        if !role_accepts_section(role, section, wiring.profile) {
            continue;
        }
        let filtered_val = match section.as_str() {
            "storage" => {
                if let TomlValue::Table(tbl) = section_val {
                    TomlValue::Table(filter_storage_table(role, tbl, wiring))
                } else {
                    section_val.clone()
                }
            }
            "wallet" => {
                if let TomlValue::Table(tbl) = section_val {
                    TomlValue::Table(filter_wallet_table(role, tbl))
                } else {
                    section_val.clone()
                }
            }
            "ingest_control" => {
                if let TomlValue::Table(tbl) = section_val {
                    TomlValue::Table(filter_ingest_control_table(role, tbl))
                } else {
                    section_val.clone()
                }
            }
            _ => section_val.clone(),
        };
        out.insert(section.clone(), filtered_val);
    }
    out
}

fn role_accepts_section(role: ServiceRole, section: &str, profile: DeploymentProfile) -> bool {
    match role {
        ServiceRole::Ingest => matches!(
            section,
            "deployment"
                | "network"
                | "ops"
                | "security"
                | "node"
                | "node_fallbacks"
                | "ingest"
                | "ingest_control"
                | "retention"
                | "storage"
        ),
        ServiceRole::Projector => matches!(
            section,
            "deployment"
                | "network"
                | "ops"
                | "security"
                | "projector"
                | "projector_control"
                | "ingest_control"
                | "storage"
                | "wallet"
        ),
        ServiceRole::Query => matches!(
            section,
            "deployment"
                | "network"
                | "ops"
                | "security"
                | "node"
                | "node_fallbacks"
                | "query"
                | "ingest_control"
                | "storage"
                | "wallet"
        ),
        ServiceRole::CompatLightwalletd => {
            if section == "wallet" && profile == DeploymentProfile::LightWallet {
                return false;
            }
            matches!(
                section,
                "deployment"
                    | "network"
                    | "ops"
                    | "security"
                    | "node"
                    | "node_fallbacks"
                    | "compat"
                    | "ingest_control"
                    | "storage"
                    | "wallet"
            )
        }
        ServiceRole::Explorer => matches!(
            section,
            "network" | "ops" | "security" | "node" | "node_fallbacks" | "explorer" | "storage"
        ),
        ServiceRole::CompatCipherscan => {
            matches!(section, "network" | "ops" | "security" | "cipherscan")
        }
    }
}

fn filter_storage_table(
    role: ServiceRole,
    table: &toml::Table,
    wiring: &DerivedWiring,
) -> toml::Table {
    let allowed: &[&str] = match role {
        ServiceRole::Ingest => &[
            "data_dir",
            "path",
            "canonical",
            "materialized_views",
            "raw_blob_policy",
            "full_blocks",
        ],
        ServiceRole::Projector => &[
            "data_dir",
            "canonical_path",
            "canonical_secondary_path",
            "raw_blob_policy",
            "full_blocks",
            "canonical",
        ],
        ServiceRole::Query | ServiceRole::CompatLightwalletd => &[
            "data_dir",
            "path",
            "secondary_path",
            "raw_blob_policy",
            "full_blocks",
            "secondary_catchup_interval_ms",
            "initial_catchup_timeout_ms",
            "secondary_replica_lag_threshold_chain_epochs",
            "serving_pair_staleness_ceiling_ms",
            "canonical",
        ],
        ServiceRole::Explorer => &[
            "data_dir",
            "path",
            "secondary_path",
            "full_blocks",
            "secondary_catchup_interval_ms",
            "initial_catchup_timeout_ms",
            "secondary_replica_lag_threshold_chain_epochs",
            "canonical",
            "materialized_views",
        ],
        ServiceRole::CompatCipherscan => &[],
    };
    let mut filtered = toml::Table::new();
    for (field, val) in table {
        if allowed.contains(&field.as_str()) {
            filtered.insert(field.clone(), val.clone());
        }
    }
    if role == ServiceRole::Ingest
        && !filtered.contains_key("path")
        && let Some(canon) = table.get("canonical_path")
    {
        filtered.insert("path".to_owned(), canon.clone());
    }
    if role == ServiceRole::Projector
        && !filtered.contains_key("canonical_path")
        && let Some(canon) = table.get("path")
    {
        filtered.insert("canonical_path".to_owned(), canon.clone());
    }
    if role == ServiceRole::Projector
        && !filtered.contains_key("canonical_secondary_path")
        && wiring.effective_data_dir.is_none()
        && let Some(base) = &wiring.secondary_base
    {
        filtered.insert(
            "canonical_secondary_path".to_owned(),
            TomlValue::String(base.join("projector").display().to_string()),
        );
    }
    filtered
}

fn filter_wallet_table(role: ServiceRole, table: &toml::Table) -> toml::Table {
    let mut filtered = toml::Table::new();
    for (field, val) in table {
        if role == ServiceRole::Projector && field == "secondary_path" {
            continue;
        }
        filtered.insert(field.clone(), val.clone());
    }
    filtered
}

fn filter_ingest_control_table(role: ServiceRole, table: &toml::Table) -> toml::Table {
    let mut filtered = toml::Table::new();
    let reader_compat = matches!(role, ServiceRole::Query | ServiceRole::CompatLightwalletd);
    for (field, val) in table {
        if reader_compat
            && matches!(
                field.as_str(),
                "checkpoint_bearer_token" | "checkpoint_bearer_token_path"
            )
        {
            continue;
        }
        filtered.insert(field.clone(), val.clone());
    }
    filtered
}

fn apply_role_overrides(
    role: ServiceRole,
    table: &mut toml::Table,
    wiring: &DerivedWiring,
) -> Result<(), UnifiedConfigError> {
    if wiring.inject_deployment_profile
        && matches!(
            role,
            ServiceRole::Ingest
                | ServiceRole::Projector
                | ServiceRole::Query
                | ServiceRole::CompatLightwalletd
        )
    {
        with_subtable(table, "deployment", |deploy| {
            deploy.insert(
                "profile".to_owned(),
                TomlValue::String(wiring.profile.as_str().to_owned()),
            );
        });
    } else if !wiring.inject_deployment_profile {
        apply_unprofiled_transaction_defaults(role, table);
    }
    if role == ServiceRole::Ingest && wiring.profile == DeploymentProfile::Explorer {
        let mut rejected_explorer_views = false;
        with_subtable(table, "ingest", |ingest_tbl| {
            if ingest_tbl
                .get("explorer_views")
                .and_then(TomlValue::as_bool)
                == Some(false)
            {
                rejected_explorer_views = true;
            } else {
                ingest_tbl.insert("explorer_views".to_owned(), TomlValue::Boolean(true));
            }
        });
        if rejected_explorer_views {
            return Err(UnifiedConfigError::Invalid(
                "explorer preset requires ingest.explorer_views = true".to_owned(),
            ));
        }
    }
    if role == ServiceRole::Projector {
        with_subtable(table, "projector", |projector_tbl| {
            projector_tbl
                .entry("lease_duration_seconds")
                .or_insert_with(|| TomlValue::Integer(14_400));
        });
    }
    apply_storage_overrides(role, table, wiring);
    apply_control_and_endpoint_overrides(role, table, wiring);
    Ok(())
}

fn apply_storage_overrides(role: ServiceRole, table: &mut toml::Table, wiring: &DerivedWiring) {
    if role == ServiceRole::CompatCipherscan {
        return;
    }
    with_subtable(table, "storage", |storage_tbl| {
        if wiring.full_blocks {
            storage_tbl.insert("full_blocks".to_owned(), TomlValue::Boolean(true));
        }
        if let Some(data_dir) = &wiring.effective_data_dir {
            storage_tbl.insert(
                "data_dir".to_owned(),
                TomlValue::String(data_dir.display().to_string()),
            );
        }
        apply_secondary_storage_paths(role, storage_tbl, wiring);
    });
    apply_wallet_secondary_overrides(role, table, wiring);
}

fn apply_secondary_storage_paths(
    role: ServiceRole,
    storage_tbl: &mut toml::Table,
    wiring: &DerivedWiring,
) {
    match role {
        ServiceRole::Query | ServiceRole::CompatLightwalletd | ServiceRole::Explorer => {
            let role_slug = match role {
                ServiceRole::Query => "query",
                ServiceRole::CompatLightwalletd => "compat-lightwalletd",
                ServiceRole::Explorer => "explorer",
                ServiceRole::Ingest | ServiceRole::Projector | ServiceRole::CompatCipherscan => "",
            };
            if let Some(sec_val) = storage_tbl
                .get("secondary_path")
                .and_then(TomlValue::as_str)
            {
                let scoped = Path::new(sec_val).join(role_slug);
                storage_tbl.insert(
                    "secondary_path".to_owned(),
                    TomlValue::String(scoped.display().to_string()),
                );
            } else if wiring.effective_data_dir.is_none()
                && let Some(base) = &wiring.secondary_base
            {
                let derived = if role == ServiceRole::Explorer {
                    derive_explorer_secondary_path(base)
                } else {
                    derive_canonical_secondary_path(base, role_slug)
                };
                storage_tbl.insert(
                    "secondary_path".to_owned(),
                    TomlValue::String(derived.display().to_string()),
                );
            }
        }
        ServiceRole::Ingest | ServiceRole::Projector | ServiceRole::CompatCipherscan => {}
    }
}

fn apply_wallet_secondary_overrides(
    role: ServiceRole,
    table: &mut toml::Table,
    wiring: &DerivedWiring,
) {
    if !matches!(role, ServiceRole::Query | ServiceRole::CompatLightwalletd)
        || wiring.profile == DeploymentProfile::LightWallet
    {
        return;
    }
    let slug = match role {
        ServiceRole::Query => "query",
        ServiceRole::CompatLightwalletd => "compat-lightwalletd",
        ServiceRole::Ingest
        | ServiceRole::Projector
        | ServiceRole::Explorer
        | ServiceRole::CompatCipherscan => "",
    };
    if let Some(TomlValue::Table(wallet_tbl)) = table.get_mut("wallet")
        && let Some(sec) = wallet_tbl.get("secondary_path").and_then(TomlValue::as_str)
    {
        wallet_tbl.insert(
            "secondary_path".to_owned(),
            TomlValue::String(Path::new(sec).join(slug).display().to_string()),
        );
    }
}

fn apply_control_and_endpoint_overrides(
    role: ServiceRole,
    table: &mut toml::Table,
    wiring: &DerivedWiring,
) {
    if let Some(staging) = &wiring.checkpoint_staging_root {
        if role == ServiceRole::Ingest {
            with_subtable(table, "ingest_control", |control| {
                control.insert(
                    "checkpoint_staging_root".to_owned(),
                    TomlValue::String(staging.display().to_string()),
                );
            });
        } else if role == ServiceRole::Projector {
            with_subtable(table, "projector_control", |control| {
                control.insert(
                    "checkpoint_staging_root".to_owned(),
                    TomlValue::String(staging.display().to_string()),
                );
            });
        }
    }
    if matches!(
        role,
        ServiceRole::Projector | ServiceRole::Query | ServiceRole::CompatLightwalletd
    ) && let Some(url) = &wiring.ingest_control_url
    {
        with_subtable(table, "ingest_control", |control_tbl| {
            control_tbl
                .entry("addr".to_owned())
                .or_insert_with(|| TomlValue::String(url.clone()));
        });
    }
    if role == ServiceRole::Explorer
        && let Some(query_addr) = wiring.query_listen_addr
    {
        with_subtable(table, "explorer", |exp_tbl| {
            exp_tbl
                .entry("wallet_query_endpoint".to_owned())
                .or_insert_with(|| {
                    TomlValue::String(format!("http://{}", loopback_target(query_addr)))
                });
        });
    }
    if role == ServiceRole::CompatCipherscan {
        with_subtable(table, "cipherscan", |cs_tbl| {
            if let Some(exp_addr) = wiring.explorer_listen_addr {
                cs_tbl
                    .entry("explorer_query_endpoint".to_owned())
                    .or_insert_with(|| {
                        TomlValue::String(format!("http://{}", loopback_target(exp_addr)))
                    });
            }
            if let Some(query_addr) = wiring.query_listen_addr {
                cs_tbl
                    .entry("wallet_query_endpoint".to_owned())
                    .or_insert_with(|| {
                        TomlValue::String(format!("http://{}", loopback_target(query_addr)))
                    });
            }
        });
    }
}

fn apply_unprofiled_transaction_defaults(role: ServiceRole, table: &mut toml::Table) {
    match role {
        ServiceRole::Ingest => with_subtable(table, "ingest", |ingest_tbl| {
            with_subtable(ingest_tbl, "run_overrides", |run_overrides| {
                run_overrides
                    .entry("coverage".to_owned())
                    .or_insert_with(|| TomlValue::String("wallet-serving".to_owned()));
            });
        }),
        ServiceRole::CompatLightwalletd => with_subtable(table, "compat", |compat_tbl| {
            compat_tbl
                .entry("serving".to_owned())
                .or_insert_with(|| TomlValue::String("wallet".to_owned()));
        }),
        ServiceRole::Projector
        | ServiceRole::Query
        | ServiceRole::Explorer
        | ServiceRole::CompatCipherscan => {}
    }
}

fn with_subtable(table: &mut toml::Table, key: &str, update: impl FnOnce(&mut toml::Table)) {
    let mut sub = match table.remove(key) {
        Some(TomlValue::Table(existing)) => existing,
        None
        | Some(
            TomlValue::String(_)
            | TomlValue::Integer(_)
            | TomlValue::Float(_)
            | TomlValue::Boolean(_)
            | TomlValue::Datetime(_)
            | TomlValue::Array(_),
        ) => toml::Table::new(),
    };
    update(&mut sub);
    table.insert(key.to_owned(), TomlValue::Table(sub));
}

fn filter_env_for_role(
    role: ServiceRole,
    env_entries: &[(String, String)],
    wiring: &DerivedWiring,
) -> BTreeMap<String, String> {
    let mut filtered = BTreeMap::new();
    for (env_name, env_val) in env_entries {
        if !env_name.starts_with("ZINDER_") || env_name.starts_with("ZINDER_TEST_") {
            continue;
        }
        let suffix = &env_name["ZINDER_".len()..];
        let lower = suffix.to_ascii_lowercase();
        let (section, field_path) = lower.split_once("__").unwrap_or((lower.as_str(), ""));
        if !role_accepts_section(role, section, wiring.profile) {
            continue;
        }
        if !role_accepts_env_subfield(role, section, field_path) {
            continue;
        }
        if section == "ops" && field_path == "listen_addr" {
            continue;
        }
        if (wiring.full_blocks && section == "storage" && field_path == "full_blocks")
            || (wiring.effective_data_dir.is_some()
                && section == "storage"
                && field_path == "data_dir")
        {
            continue;
        }
        if section == "storage"
            && field_path == "secondary_path"
            && matches!(
                role,
                ServiceRole::Query | ServiceRole::CompatLightwalletd | ServiceRole::Explorer
            )
        {
            continue;
        }
        if section == "wallet"
            && field_path == "secondary_path"
            && matches!(role, ServiceRole::Query | ServiceRole::CompatLightwalletd)
        {
            continue;
        }
        filtered.insert(env_name.clone(), env_val.clone());
    }
    filtered
}

fn role_accepts_env_subfield(role: ServiceRole, section: &str, field_path: &str) -> bool {
    let top_field = field_path.split("__").next().unwrap_or("");
    match section {
        "storage" => match role {
            ServiceRole::Ingest => matches!(
                top_field,
                "data_dir"
                    | "path"
                    | "canonical"
                    | "materialized_views"
                    | "raw_blob_policy"
                    | "full_blocks"
            ),
            ServiceRole::Projector => matches!(
                top_field,
                "data_dir"
                    | "canonical_path"
                    | "canonical_secondary_path"
                    | "raw_blob_policy"
                    | "full_blocks"
                    | "canonical"
            ),
            ServiceRole::Query | ServiceRole::CompatLightwalletd => matches!(
                top_field,
                "data_dir"
                    | "path"
                    | "secondary_path"
                    | "raw_blob_policy"
                    | "full_blocks"
                    | "secondary_catchup_interval_ms"
                    | "initial_catchup_timeout_ms"
                    | "secondary_replica_lag_threshold_chain_epochs"
                    | "serving_pair_staleness_ceiling_ms"
                    | "canonical"
            ),
            ServiceRole::Explorer => matches!(
                top_field,
                "data_dir"
                    | "path"
                    | "secondary_path"
                    | "full_blocks"
                    | "secondary_catchup_interval_ms"
                    | "initial_catchup_timeout_ms"
                    | "secondary_replica_lag_threshold_chain_epochs"
                    | "canonical"
                    | "materialized_views"
            ),
            ServiceRole::CompatCipherscan => false,
        },
        "wallet" => !(role == ServiceRole::Projector && top_field == "secondary_path"),
        "ingest_control" => {
            let is_reader = matches!(role, ServiceRole::Query | ServiceRole::CompatLightwalletd);
            !(is_reader
                && matches!(
                    top_field,
                    "checkpoint_bearer_token" | "checkpoint_bearer_token_path"
                ))
        }
        _ => true,
    }
}

fn apply_role_env_overrides(
    role: ServiceRole,
    env: &mut BTreeMap<String, String>,
    wiring: &DerivedWiring,
) {
    if wiring.inject_deployment_profile
        && matches!(
            role,
            ServiceRole::Ingest
                | ServiceRole::Projector
                | ServiceRole::Query
                | ServiceRole::CompatLightwalletd
        )
    {
        env.insert(
            "ZINDER_DEPLOYMENT__PROFILE".to_owned(),
            wiring.profile.as_str().to_owned(),
        );
    }
    if let Some(read_token) = &wiring.generated_read_token
        && matches!(
            role,
            ServiceRole::Ingest
                | ServiceRole::Projector
                | ServiceRole::Query
                | ServiceRole::CompatLightwalletd
        )
    {
        env.insert(
            "ZINDER_INGEST_CONTROL__BEARER_TOKEN".to_owned(),
            read_token.clone(),
        );
    }
    if let Some(checkpoint_token) = &wiring.generated_checkpoint_token
        && matches!(role, ServiceRole::Ingest | ServiceRole::Projector)
    {
        env.insert(
            "ZINDER_INGEST_CONTROL__CHECKPOINT_BEARER_TOKEN".to_owned(),
            checkpoint_token.clone(),
        );
    }
    if let Some(projector_token) = &wiring.generated_projector_token
        && role == ServiceRole::Projector
    {
        env.insert(
            "ZINDER_PROJECTOR_CONTROL__BEARER_TOKEN".to_owned(),
            projector_token.clone(),
        );
    }
}
