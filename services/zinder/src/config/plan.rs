use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
};

use toml::Value as TomlValue;
use zinder_core::Network;
use zinder_runtime::{
    CipherscanSection, CompatSection, ConfigError, ConfigLoader, DeploymentProfile,
    DeploymentSection, ExplorerSection, IngestControlSection, IngestSection,
    ProjectorControlSection, ProjectorSection, QuerySection, RedactedUrl, RetentionSection,
    SecretInput, StorageRoleSection, StorageSection, Toggle, WalletSection, ZinderConfig,
    derive_canonical_path, derive_canonical_secondary_path, derive_checkpoints_path,
    derive_explorer_secondary_path, derive_wallet_path, guard_optional_serving_bind,
    parse_socket_addr, resolve_allow_public_bind, resolve_ops_listen_addr,
    section_defaults::DEFAULT_DATA_DIR, validate_optional_data_dir,
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
const DEFAULT_CO_SUPERVISED_LEASE_DURATION_SECONDS: u64 = 14_400;

struct DerivedWiring {
    profile: DeploymentProfile,
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
    let base_config = load_combined_config(scope, env_entries)?;
    if scope.readers_only {
        base_config.reject_writer_only_secrets()?;
    }
    let cli_data_dir = validate_optional_data_dir(scope.data_dir.clone())?;
    let config_data_dir = validate_optional_data_dir(base_config.storage.data_dir.clone())?;
    let effective_data_dir = cli_data_dir.or(config_data_dir);
    let profile = scope
        .preset
        .map(Preset::deployment_profile)
        .or(base_config.deployment.profile)
        .unwrap_or(DeploymentProfile::Wallet);
    let active_roles = active_roles_for(scope.mode, profile, scope.readers_only)?;
    let wiring = derive_wiring(
        scope,
        &base_config,
        profile,
        effective_data_dir,
        &active_roles,
    )?;

    let mut roles = Vec::with_capacity(active_roles.len());
    for role in active_roles {
        roles.push(build_role_spec(role, &base_config, &wiring)?);
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

fn load_combined_config(
    scope: &ConfigScope,
    env_entries: &[(String, String)],
) -> Result<ZinderConfig, UnifiedConfigError> {
    let default_ops = match scope.mode {
        SupervisorMode::Serve => DEFAULT_SUPERVISOR_OPS_LISTEN_ADDR,
        SupervisorMode::Index => DEFAULT_INDEX_OPS_LISTEN_ADDR,
    };
    ConfigLoader::new()
        .with_default("ops.listen_addr", default_ops)?
        .with_security_section()?
        .with_file(scope.config_path.clone())
        .with_zinder_env_from_map(env_entries.iter().cloned())?
        .load()
        .map_err(UnifiedConfigError::from)
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
    base: &ZinderConfig,
    profile: DeploymentProfile,
    effective_data_dir: Option<PathBuf>,
    active_roles: &[ServiceRole],
) -> Result<DerivedWiring, UnifiedConfigError> {
    let network = base.network.clone().resolve()?;
    let ops_listen_addr = resolve_ops_listen_addr(base.ops)?;
    let allow_public_bind = resolve_allow_public_bind(base.security)?;
    guard_optional_serving_bind("ops.listen_addr", ops_listen_addr, allow_public_bind)?;
    let full_blocks = scope.full_blocks
        || base.storage.full_blocks == Some(true)
        || profile == DeploymentProfile::WalletArchive;
    let has_ingest = active_roles.contains(&ServiceRole::Ingest);
    let has_consumers = active_roles.iter().any(|role| {
        matches!(
            role,
            ServiceRole::Projector | ServiceRole::Query | ServiceRole::CompatLightwalletd
        )
    });
    let canonical_path = resolve_canonical_primary(&base.storage, effective_data_dir.as_deref());
    let wallet_path = resolve_wallet_primary(profile, &base.wallet, effective_data_dir.as_deref());
    let secondary_base = resolve_secondary_base(
        &base.storage,
        effective_data_dir.as_deref(),
        &canonical_path,
    );
    let checkpoint_staging_root =
        resolve_checkpoint_staging_root(base, effective_data_dir.as_deref());
    let (ingest_control_addr, ingest_control_url) =
        resolve_ingest_control_wiring(base, has_ingest && has_consumers)?;
    let (generated_read_token, generated_checkpoint_token) =
        generate_internal_tokens(base, has_ingest && has_consumers)?;
    let generated_projector_token =
        generate_projector_control_token(base, active_roles.contains(&ServiceRole::Projector))?;
    let (query_listen_addr, explorer_listen_addr) = resolve_explorer_endpoints(base, active_roles)?;

    Ok(DerivedWiring {
        profile,
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

fn resolve_canonical_primary(storage: &StorageSection, data_dir: Option<&Path>) -> PathBuf {
    storage.path.clone().unwrap_or_else(|| {
        let root = data_dir.unwrap_or_else(|| Path::new(DEFAULT_DATA_DIR));
        derive_canonical_path(root)
    })
}

fn resolve_wallet_primary(
    profile: DeploymentProfile,
    wallet: &WalletSection,
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
    storage: &StorageSection,
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
    base: &ZinderConfig,
    effective_data_dir: Option<&Path>,
) -> Option<PathBuf> {
    if base.ingest_control.checkpoint_staging_root.is_some()
        || base.projector_control.checkpoint_staging_root.is_some()
    {
        return None;
    }
    effective_data_dir.map(derive_checkpoints_path)
}

fn resolve_ingest_control_wiring(
    base: &ZinderConfig,
    co_supervised: bool,
) -> Result<(Option<SocketAddr>, Option<String>), UnifiedConfigError> {
    if !co_supervised {
        return Ok((None, None));
    }
    let socket_addr = match base.ingest_control.listen_addr {
        Toggle::Disabled => {
            return Err(UnifiedConfigError::Invalid(
                "ingest_control.listen_addr cannot be disabled (`none`) when supervising writer alongside readers"
                    .to_owned(),
            ));
        }
        Toggle::Defaulted => {
            parse_socket_addr("ingest_control.listen_addr", DEFAULT_INGEST_CONTROL_ADDR)?
        }
        Toggle::Enabled(addr) => addr,
    };
    let dial_url = base.ingest_control.addr.as_ref().map_or_else(
        || format!("http://{}", loopback_target(socket_addr)),
        |url| url.as_str().to_owned(),
    );
    Ok((Some(socket_addr), Some(dial_url)))
}

fn generate_internal_tokens(
    base: &ZinderConfig,
    co_supervised: bool,
) -> Result<(Option<String>, Option<String>), UnifiedConfigError> {
    if !co_supervised {
        return Ok((None, None));
    }
    let operator_configured = base.ingest_control.bearer_token.is_some()
        || base.ingest_control.bearer_token_path.is_some()
        || base.ingest_control.checkpoint_bearer_token.is_some()
        || base.ingest_control.checkpoint_bearer_token_path.is_some();
    if operator_configured {
        return Ok((None, None));
    }
    Ok((Some(random_hex_token_32()?), Some(random_hex_token_32()?)))
}

fn generate_projector_control_token(
    base: &ZinderConfig,
    has_projector: bool,
) -> Result<Option<String>, UnifiedConfigError> {
    if !has_projector {
        return Ok(None);
    }
    if base.projector_control.bearer_token.is_some()
        || base.projector_control.bearer_token_path.is_some()
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
    base: &ZinderConfig,
    active_roles: &[ServiceRole],
) -> Result<(Option<SocketAddr>, Option<SocketAddr>), UnifiedConfigError> {
    let query_addr = if active_roles.contains(&ServiceRole::Query)
        && active_roles.contains(&ServiceRole::Explorer)
    {
        let raw = base
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
        let raw = base
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
    base: &ZinderConfig,
    wiring: &DerivedWiring,
) -> Result<RoleLaunchSpec, UnifiedConfigError> {
    let mut role_cfg = base.clone();
    role_cfg.ops.listen_addr = Toggle::Disabled;
    scrub_non_role_fields(role, &mut role_cfg, wiring.profile);
    apply_role_overrides(role, &mut role_cfg, wiring)?;

    let validation_config = role_cfg.clone();
    let (non_secret, secret_env) = role_cfg.extract_secrets_for_launch();
    let non_secret_toml = render_non_secret_toml(&non_secret)?;

    Ok(RoleLaunchSpec {
        role,
        non_secret_toml,
        secret_env,
        required_for_readyz: true,
        validation_config,
    })
}

fn render_non_secret_toml(non_secret: &ZinderConfig) -> Result<String, UnifiedConfigError> {
    let mut table = toml::Table::try_from(non_secret).map_err(|source| {
        UnifiedConfigError::Invalid(format!("TOML serialize failed: {source}"))
    })?;
    prune_empty_tables(&mut table);
    toml::to_string(&table)
        .map_err(|source| UnifiedConfigError::Invalid(format!("TOML serialize failed: {source}")))
}

fn prune_empty_tables(table: &mut toml::Table) {
    table.retain(|_, entry| {
        if let TomlValue::Table(sub) = entry {
            prune_empty_tables(sub);
            !sub.is_empty()
        } else {
            true
        }
    });
}

fn scrub_non_role_fields(
    role: ServiceRole,
    role_cfg: &mut ZinderConfig,
    profile: DeploymentProfile,
) {
    scrub_unrelated_role_sections(role, role_cfg);
    scrub_shared_infrastructure_sections(role, role_cfg, profile);
    scrub_storage_subfields(role, role_cfg);
}

fn scrub_unrelated_role_sections(role: ServiceRole, role_cfg: &mut ZinderConfig) {
    if role != ServiceRole::Ingest {
        role_cfg.ingest = IngestSection::default();
        role_cfg.retention = RetentionSection::default();
    }
    if role != ServiceRole::Projector {
        role_cfg.projector = ProjectorSection::default();
        role_cfg.projector_control = ProjectorControlSection::default();
    }
    if role != ServiceRole::Query {
        role_cfg.query = QuerySection::default();
    }
    if role != ServiceRole::CompatLightwalletd {
        role_cfg.compat = CompatSection::default();
    }
    if role != ServiceRole::Explorer {
        role_cfg.explorer = ExplorerSection::default();
    }
    if role != ServiceRole::CompatCipherscan {
        role_cfg.cipherscan = CipherscanSection::default();
    }
}

fn scrub_shared_infrastructure_sections(
    role: ServiceRole,
    role_cfg: &mut ZinderConfig,
    profile: DeploymentProfile,
) {
    if matches!(role, ServiceRole::Explorer | ServiceRole::CompatCipherscan) {
        role_cfg.deployment = DeploymentSection::default();
        role_cfg.ingest_control = IngestControlSection::default();
    }
    if matches!(role, ServiceRole::Projector | ServiceRole::CompatCipherscan) {
        let _cleared_node = std::mem::take(&mut role_cfg.node);
        let _cleared_fallbacks = std::mem::take(&mut role_cfg.node_fallbacks);
    } else if role != ServiceRole::Ingest {
        role_cfg.node.indexer_grpc_addr = None;
        let _cleared_tls = std::mem::take(&mut role_cfg.node.indexer_tls);
        for (_, fallback) in role_cfg.node_fallbacks.iter_mut() {
            fallback.indexer_grpc_addr = None;
            let _cleared_fb_tls = std::mem::take(&mut fallback.indexer_tls);
        }
    }
    if matches!(
        role,
        ServiceRole::Ingest | ServiceRole::Explorer | ServiceRole::CompatCipherscan
    ) || (role == ServiceRole::CompatLightwalletd && profile == DeploymentProfile::LightWallet)
    {
        role_cfg.wallet = WalletSection::default();
    }
    if role == ServiceRole::Projector {
        role_cfg.wallet.secondary_path = None;
    }
    if role == ServiceRole::Ingest {
        role_cfg.ingest_control.addr = None;
    }
    if matches!(
        role,
        ServiceRole::Projector | ServiceRole::Query | ServiceRole::CompatLightwalletd
    ) {
        role_cfg.ingest_control.listen_addr = Toggle::Defaulted;
    }
    if matches!(role, ServiceRole::Query | ServiceRole::CompatLightwalletd) {
        role_cfg.ingest_control.checkpoint_bearer_token = None;
        role_cfg.ingest_control.checkpoint_bearer_token_path = None;
    }
}

fn scrub_storage_subfields(role: ServiceRole, role_cfg: &mut ZinderConfig) {
    match role {
        ServiceRole::Ingest => {
            role_cfg.storage.secondary_path = None;
            clear_reader_catchup_fields(&mut role_cfg.storage);
        }
        ServiceRole::Projector => {
            role_cfg.storage.materialized_views = StorageRoleSection::default();
            clear_reader_catchup_fields(&mut role_cfg.storage);
        }
        ServiceRole::Query | ServiceRole::CompatLightwalletd => {
            role_cfg.storage.materialized_views = StorageRoleSection::default();
        }
        ServiceRole::Explorer => {
            role_cfg.storage.raw_blob_policy = None;
            role_cfg.storage.serving_pair_staleness_ceiling_ms = None;
        }
        ServiceRole::CompatCipherscan => {
            role_cfg.storage = StorageSection::default();
        }
    }
}

fn clear_reader_catchup_fields(storage: &mut StorageSection) {
    storage.secondary_catchup_interval_ms = None;
    storage.initial_catchup_timeout_ms = None;
    storage.secondary_replica_lag_threshold_chain_epochs = None;
    storage.serving_pair_staleness_ceiling_ms = None;
}

fn apply_role_overrides(
    role: ServiceRole,
    role_cfg: &mut ZinderConfig,
    wiring: &DerivedWiring,
) -> Result<(), UnifiedConfigError> {
    if matches!(
        role,
        ServiceRole::Ingest
            | ServiceRole::Projector
            | ServiceRole::Query
            | ServiceRole::CompatLightwalletd
    ) {
        role_cfg.deployment.profile = Some(wiring.profile);
    }
    if role == ServiceRole::Ingest && wiring.profile == DeploymentProfile::Explorer {
        if role_cfg.ingest.explorer_views == Some(false) {
            return Err(UnifiedConfigError::Invalid(
                "explorer preset requires ingest.explorer_views = true".to_owned(),
            ));
        }
        role_cfg.ingest.explorer_views = Some(true);
    }
    if role == ServiceRole::Projector {
        role_cfg
            .projector
            .lease_duration_seconds
            .get_or_insert(DEFAULT_CO_SUPERVISED_LEASE_DURATION_SECONDS);
    }
    apply_storage_overrides(role, role_cfg, wiring);
    apply_control_and_endpoint_overrides(role, role_cfg, wiring)?;
    apply_generated_token_overrides(role, role_cfg, wiring)
}

fn apply_storage_overrides(role: ServiceRole, role_cfg: &mut ZinderConfig, wiring: &DerivedWiring) {
    if role == ServiceRole::CompatCipherscan {
        return;
    }
    if wiring.full_blocks {
        role_cfg.storage.full_blocks = Some(true);
    }
    if let Some(dir) = &wiring.effective_data_dir {
        role_cfg.storage.data_dir = Some(dir.clone());
    }
    apply_secondary_storage_paths(role, &mut role_cfg.storage, wiring);
    if matches!(role, ServiceRole::Query | ServiceRole::CompatLightwalletd)
        && wiring.profile != DeploymentProfile::LightWallet
        && let Some(role_slug) = reader_role_slug(role)
        && let Some(sec_path) = role_cfg.wallet.secondary_path.as_mut()
    {
        *sec_path = sec_path.join(role_slug);
    }
}

fn apply_secondary_storage_paths(
    role: ServiceRole,
    storage: &mut StorageSection,
    wiring: &DerivedWiring,
) {
    let Some(role_slug) = reader_role_slug(role) else {
        return;
    };
    if let Some(sec_path) = storage.secondary_path.as_mut() {
        *sec_path = sec_path.join(role_slug);
    } else if wiring.effective_data_dir.is_none()
        && let Some(base) = &wiring.secondary_base
    {
        let derived = match role {
            ServiceRole::Projector => base.join("projector"),
            ServiceRole::Explorer => derive_explorer_secondary_path(base),
            ServiceRole::Query | ServiceRole::CompatLightwalletd => {
                derive_canonical_secondary_path(base, role_slug)
            }
            ServiceRole::Ingest | ServiceRole::CompatCipherscan => return,
        };
        storage.secondary_path = Some(derived);
    }
}

const fn reader_role_slug(role: ServiceRole) -> Option<&'static str> {
    match role {
        ServiceRole::Projector => Some("projector"),
        ServiceRole::Query => Some("query"),
        ServiceRole::CompatLightwalletd => Some("compat-lightwalletd"),
        ServiceRole::Explorer => Some("explorer"),
        ServiceRole::Ingest | ServiceRole::CompatCipherscan => None,
    }
}

fn apply_control_and_endpoint_overrides(
    role: ServiceRole,
    role_cfg: &mut ZinderConfig,
    wiring: &DerivedWiring,
) -> Result<(), UnifiedConfigError> {
    if let Some(staging) = &wiring.checkpoint_staging_root {
        match role {
            ServiceRole::Ingest => {
                role_cfg.ingest_control.checkpoint_staging_root = Some(staging.clone());
            }
            ServiceRole::Projector => {
                role_cfg.projector_control.checkpoint_staging_root = Some(staging.clone());
            }
            ServiceRole::Query
            | ServiceRole::CompatLightwalletd
            | ServiceRole::Explorer
            | ServiceRole::CompatCipherscan => {}
        }
    }
    if matches!(
        role,
        ServiceRole::Projector | ServiceRole::Query | ServiceRole::CompatLightwalletd
    ) && let Some(url) = &wiring.ingest_control_url
        && role_cfg.ingest_control.addr.is_none()
    {
        role_cfg.ingest_control.addr = Some(wrap_url(url.clone())?);
    }
    if role == ServiceRole::Explorer
        && let Some(query_addr) = wiring.query_listen_addr
        && matches!(role_cfg.explorer.wallet_query_endpoint, Toggle::Defaulted)
    {
        let url = format!("http://{}", loopback_target(query_addr));
        role_cfg.explorer.wallet_query_endpoint = Toggle::Enabled(wrap_url(url)?);
    }
    if role == ServiceRole::CompatCipherscan {
        if let Some(exp_addr) = wiring.explorer_listen_addr
            && role_cfg.cipherscan.explorer_query_endpoint.is_none()
        {
            role_cfg.cipherscan.explorer_query_endpoint =
                Some(wrap_url(format!("http://{}", loopback_target(exp_addr)))?);
        }
        if let Some(query_addr) = wiring.query_listen_addr
            && role_cfg.cipherscan.wallet_query_endpoint.is_none()
        {
            role_cfg.cipherscan.wallet_query_endpoint =
                Some(wrap_url(format!("http://{}", loopback_target(query_addr)))?);
        }
    }
    Ok(())
}

fn apply_generated_token_overrides(
    role: ServiceRole,
    role_cfg: &mut ZinderConfig,
    wiring: &DerivedWiring,
) -> Result<(), UnifiedConfigError> {
    if let Some(read_token) = &wiring.generated_read_token
        && matches!(
            role,
            ServiceRole::Ingest
                | ServiceRole::Projector
                | ServiceRole::Query
                | ServiceRole::CompatLightwalletd
        )
    {
        role_cfg.ingest_control.bearer_token = Some(wrap_secret(read_token.clone())?);
    }
    if let Some(checkpoint_token) = &wiring.generated_checkpoint_token
        && matches!(role, ServiceRole::Ingest | ServiceRole::Projector)
    {
        role_cfg.ingest_control.checkpoint_bearer_token =
            Some(wrap_secret(checkpoint_token.clone())?);
    }
    if let Some(projector_token) = &wiring.generated_projector_token
        && role == ServiceRole::Projector
    {
        role_cfg.projector_control.bearer_token = Some(wrap_secret(projector_token.clone())?);
    }
    Ok(())
}

fn wrap_url(raw: String) -> Result<RedactedUrl, UnifiedConfigError> {
    RedactedUrl::new(raw)
        .map_err(ConfigError::invalid)
        .map_err(UnifiedConfigError::from)
}

fn wrap_secret(raw: String) -> Result<SecretInput, UnifiedConfigError> {
    SecretInput::new(raw)
        .map_err(ConfigError::invalid)
        .map_err(UnifiedConfigError::from)
}
