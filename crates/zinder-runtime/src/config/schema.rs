//! Unified typed configuration root [`ZinderConfig`] and Serde schema introspection.

use std::collections::{BTreeMap, BTreeSet};

use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use zinder_source::{DISABLED_SENTINEL, NodeFallbackSections, NodeSection, SecretNodeInput};

use crate::{
    ConfigError, ObservabilityConfig,
    config::{NetworkSection, RedactedCidrs, RedactedUrl, SecretInput, Toggle},
    sections::{
        CipherscanSection, CompatSection, DeploymentSection, ExplorerSection, IngestControlSection,
        IngestSection, OpsSection, ProjectorControlSection, ProjectorSection, QuerySection,
        RetentionSection, SecuritySection, StorageSection, WalletSection,
    },
};

const SCHEMA_PROBE_SENTINEL: &str = "__zinder_schema_probe__";

/// Unified configuration schema shared across all Zinder binaries and the
/// combined `zinder` supervisor.
///
/// Unknown keys anywhere in any section fail closed during deserialization.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ZinderConfig {
    /// Coordinated deployment preset (`[deployment]`).
    #[serde(default, skip_serializing_if = "DeploymentSection::is_empty")]
    pub deployment: DeploymentSection,
    /// OpenTelemetry tracing settings (`[observability]`).
    #[serde(default, skip_serializing_if = "ObservabilityConfig::is_empty")]
    pub observability: ObservabilityConfig,
    /// Zcash network selection (`[network]`).
    #[serde(default, skip_serializing_if = "NetworkSection::is_empty")]
    pub network: NetworkSection,
    /// Operational HTTP health/metrics endpoint (`[ops]`).
    #[serde(default, skip_serializing_if = "OpsSection::is_empty")]
    pub ops: OpsSection,
    /// Serving bind safety posture (`[security]`).
    #[serde(default, skip_serializing_if = "SecuritySection::is_empty")]
    pub security: SecuritySection,
    /// Canonical and materialized-view storage settings (`[storage]`).
    #[serde(default, skip_serializing_if = "StorageSection::is_empty")]
    pub storage: StorageSection,
    /// Wallet projection storage settings (`[wallet]`).
    #[serde(default, skip_serializing_if = "WalletSection::is_empty")]
    pub wallet: WalletSection,
    /// Primary upstream Zebra node settings (`[node]`).
    #[serde(default, skip_serializing_if = "NodeSection::is_empty")]
    pub node: NodeSection,
    /// Ordered fallback Zebra nodes (`[node_fallbacks.*]`).
    #[serde(default, skip_serializing_if = "NodeFallbackSections::is_empty")]
    pub node_fallbacks: NodeFallbackSections,
    /// Durable retention policies (`[retention]`).
    #[serde(default, skip_serializing_if = "RetentionSection::is_empty")]
    pub retention: RetentionSection,
    /// Canonical-writer control endpoint settings (`[ingest_control]`).
    #[serde(default, skip_serializing_if = "IngestControlSection::is_empty")]
    pub ingest_control: IngestControlSection,
    /// Projector control endpoint settings (`[projector_control]`).
    #[serde(default, skip_serializing_if = "ProjectorControlSection::is_empty")]
    pub projector_control: ProjectorControlSection,
    /// Canonical ingest writer settings (`[ingest]`).
    #[serde(default, skip_serializing_if = "IngestSection::is_empty")]
    pub ingest: IngestSection,
    /// Wallet projection builder/follower settings (`[projector]`).
    #[serde(default, skip_serializing_if = "ProjectorSection::is_empty")]
    pub projector: ProjectorSection,
    /// `WalletQuery` server settings (`[query]`).
    #[serde(default, skip_serializing_if = "QuerySection::is_empty")]
    pub query: QuerySection,
    /// `lightwalletd`-compatible gRPC/gRPC-Web server settings (`[compat]`).
    #[serde(default, skip_serializing_if = "CompatSection::is_empty")]
    pub compat: CompatSection,
    /// `ExplorerQuery` server settings (`[explorer]`).
    #[serde(default, skip_serializing_if = "ExplorerSection::is_empty")]
    pub explorer: ExplorerSection,
    /// Cipherscan HTTP REST / WebSocket adapter settings (`[cipherscan]`).
    #[serde(default, skip_serializing_if = "CipherscanSection::is_empty")]
    pub cipherscan: CipherscanSection,
}

impl ZinderConfig {
    /// Fails closed when writer-only secret material is supplied to a reader-only
    /// supervision mode such as `zinder serve --readers-only`.
    pub fn reject_writer_only_secrets(&self) -> Result<(), ConfigError> {
        let mut forbidden: Vec<String> = Vec::new();
        if self.ingest_control.checkpoint_bearer_token.is_some() {
            forbidden.push("ingest_control.checkpoint_bearer_token".to_owned());
        }
        if self.ingest_control.checkpoint_bearer_token_path.is_some() {
            forbidden.push("ingest_control.checkpoint_bearer_token_path".to_owned());
        }
        if self.projector_control.bearer_token.is_some() {
            forbidden.push("projector_control.bearer_token".to_owned());
        }
        if self.projector_control.bearer_token_path.is_some() {
            forbidden.push("projector_control.bearer_token_path".to_owned());
        }
        if self.node.indexer_tls.client_key_pem.is_some() {
            forbidden.push("node.indexer_tls.client_key_pem".to_owned());
        }
        if self.node.indexer_tls.client_key_path.is_some() {
            forbidden.push("node.indexer_tls.client_key_path".to_owned());
        }
        for (position, fallback) in self.node_fallbacks.iter() {
            if fallback.indexer_tls.client_key_pem.is_some() {
                forbidden.push(format!(
                    "node_fallbacks.{position}.indexer_tls.client_key_pem"
                ));
            }
            if fallback.indexer_tls.client_key_path.is_some() {
                forbidden.push(format!(
                    "node_fallbacks.{position}.indexer_tls.client_key_path"
                ));
            }
        }
        if forbidden.is_empty() {
            return Ok(());
        }
        Err(ConfigError::invalid(format!(
            "writer-only secret fields cannot be supplied in `--readers-only` mode: {}",
            forbidden.join(", ")
        )))
    }

    /// Splits a role configuration into a non-secret [`ZinderConfig`] safe to
    /// serialize to disk and a `ZINDER_*` environment map carrying every
    /// credential, bearer token, private key, and endpoint URL.
    #[must_use]
    pub fn extract_secrets_for_launch(&self) -> (Self, BTreeMap<String, SecretString>) {
        let mut non_secret = self.clone();
        let mut secret_env = BTreeMap::new();

        if let Some(otlp_uri) = non_secret.observability.otlp_endpoint.take() {
            record_secret(
                &mut secret_env,
                "ZINDER_OBSERVABILITY__OTLP_ENDPOINT",
                otlp_uri.to_string(),
            );
        }
        extract_node_secrets("ZINDER_NODE__", &mut non_secret.node, &mut secret_env);
        for (position, fallback) in non_secret.node_fallbacks.iter_mut() {
            let prefix = format!("ZINDER_NODE_FALLBACKS__{position}__");
            extract_node_secrets(&prefix, fallback, &mut secret_env);
        }
        extract_control_secrets(&mut non_secret, &mut secret_env);
        extract_role_secrets(&mut non_secret, &mut secret_env);
        (non_secret, secret_env)
    }
}

fn record_secret(
    secret_env: &mut BTreeMap<String, SecretString>,
    key: impl Into<String>,
    secret_text: String,
) {
    let _previous = secret_env.insert(key.into(), SecretString::from(secret_text));
}

fn extract_node_secrets(
    prefix: &str,
    section: &mut NodeSection,
    secret_env: &mut BTreeMap<String, SecretString>,
) {
    if let Some(addr) = section.json_rpc_addr.take() {
        record_secret(secret_env, format!("{prefix}JSON_RPC_ADDR"), addr);
    }
    extract_toggle_string(
        &mut section.indexer_grpc_addr,
        format!("{prefix}INDEXER_GRPC_ADDR"),
        secret_env,
    );
    extract_toggle_string(
        &mut section.health.addr,
        format!("{prefix}HEALTH__ADDR"),
        secret_env,
    );
    extract_secret_node_input(
        &mut section.auth.password,
        format!("{prefix}AUTH__PASSWORD"),
        secret_env,
    );
    extract_secret_node_input(
        &mut section.auth.cookie,
        format!("{prefix}AUTH__COOKIE"),
        secret_env,
    );
    extract_utf8_path(
        &mut section.auth.path,
        format!("{prefix}AUTH__PATH"),
        secret_env,
    );
    extract_secret_node_input(
        &mut section.indexer_tls.ca_cert_pem,
        format!("{prefix}INDEXER_TLS__CA_CERT_PEM"),
        secret_env,
    );
    extract_secret_node_input(
        &mut section.indexer_tls.client_cert_pem,
        format!("{prefix}INDEXER_TLS__CLIENT_CERT_PEM"),
        secret_env,
    );
    extract_secret_node_input(
        &mut section.indexer_tls.client_key_pem,
        format!("{prefix}INDEXER_TLS__CLIENT_KEY_PEM"),
        secret_env,
    );
    extract_utf8_path(
        &mut section.indexer_tls.client_key_path,
        format!("{prefix}INDEXER_TLS__CLIENT_KEY_PATH"),
        secret_env,
    );
}

fn extract_toggle_string(
    slot: &mut Option<String>,
    env_key: String,
    secret_env: &mut BTreeMap<String, SecretString>,
) {
    if slot
        .as_deref()
        .is_some_and(|raw| raw.trim() != DISABLED_SENTINEL)
        && let Some(raw) = slot.take()
    {
        record_secret(secret_env, env_key, raw);
    }
}

fn extract_secret_node_input(
    slot: &mut Option<SecretNodeInput>,
    env_key: String,
    secret_env: &mut BTreeMap<String, SecretString>,
) {
    if let Some(secret) = slot.take() {
        record_secret(secret_env, env_key, secret.expose_secret().to_owned());
    }
}

fn extract_utf8_path(
    slot: &mut Option<std::path::PathBuf>,
    env_key: String,
    secret_env: &mut BTreeMap<String, SecretString>,
) {
    if slot.as_ref().and_then(|path| path.to_str()).is_some()
        && let Some(path) = slot.take()
        && let Ok(text) = path.into_os_string().into_string()
    {
        record_secret(secret_env, env_key, text);
    }
}

fn extract_control_secrets(
    non_secret: &mut ZinderConfig,
    secret_env: &mut BTreeMap<String, SecretString>,
) {
    if let Some(url) = non_secret.ingest_control.addr.take() {
        record_secret(secret_env, "ZINDER_INGEST_CONTROL__ADDR", url.into_inner());
    }
    extract_secret_input(
        &mut non_secret.ingest_control.bearer_token,
        "ZINDER_INGEST_CONTROL__BEARER_TOKEN",
        secret_env,
    );
    extract_secret_input(
        &mut non_secret.ingest_control.checkpoint_bearer_token,
        "ZINDER_INGEST_CONTROL__CHECKPOINT_BEARER_TOKEN",
        secret_env,
    );
    extract_secret_input(
        &mut non_secret.projector_control.bearer_token,
        "ZINDER_PROJECTOR_CONTROL__BEARER_TOKEN",
        secret_env,
    );
}

fn extract_role_secrets(
    non_secret: &mut ZinderConfig,
    secret_env: &mut BTreeMap<String, SecretString>,
) {
    extract_cidrs(
        &mut non_secret.query.proxy_protocol_trusted_cidrs,
        "ZINDER_QUERY__PROXY_PROTOCOL_TRUSTED_CIDRS",
        secret_env,
    );
    extract_cidrs(
        &mut non_secret.compat.proxy_protocol_trusted_cidrs,
        "ZINDER_COMPAT__PROXY_PROTOCOL_TRUSTED_CIDRS",
        secret_env,
    );
    extract_secret_input(
        &mut non_secret.explorer.bearer_token,
        "ZINDER_EXPLORER__BEARER_TOKEN",
        secret_env,
    );
    if matches!(
        non_secret.explorer.wallet_query_endpoint,
        Toggle::Enabled(_)
    ) && let Toggle::Enabled(url) =
        std::mem::take(&mut non_secret.explorer.wallet_query_endpoint)
    {
        record_secret(
            secret_env,
            "ZINDER_EXPLORER__WALLET_QUERY_ENDPOINT",
            url.into_inner(),
        );
    }
    extract_secret_input(
        &mut non_secret.cipherscan.bearer_token,
        "ZINDER_CIPHERSCAN__BEARER_TOKEN",
        secret_env,
    );
    extract_redacted_url(
        &mut non_secret.cipherscan.explorer_query_endpoint,
        "ZINDER_CIPHERSCAN__EXPLORER_QUERY_ENDPOINT",
        secret_env,
    );
    extract_redacted_url(
        &mut non_secret.cipherscan.wallet_query_endpoint,
        "ZINDER_CIPHERSCAN__WALLET_QUERY_ENDPOINT",
        secret_env,
    );
    extract_redacted_url(
        &mut non_secret.cipherscan.current_price_endpoint,
        "ZINDER_CIPHERSCAN__CURRENT_PRICE_ENDPOINT",
        secret_env,
    );
    extract_redacted_url(
        &mut non_secret.cipherscan.historical_price_endpoint_template,
        "ZINDER_CIPHERSCAN__HISTORICAL_PRICE_ENDPOINT_TEMPLATE",
        secret_env,
    );
}

fn extract_secret_input(
    slot: &mut Option<SecretInput>,
    env_key: &str,
    secret_env: &mut BTreeMap<String, SecretString>,
) {
    if let Some(secret) = slot.take() {
        record_secret(secret_env, env_key, secret.expose_secret().to_owned());
    }
}

fn extract_cidrs(
    slot: &mut RedactedCidrs,
    env_key: &str,
    secret_env: &mut BTreeMap<String, SecretString>,
) {
    if matches!(slot, RedactedCidrs::Configured(_))
        && let Some(raw) = std::mem::take(slot).as_deref()
    {
        record_secret(secret_env, env_key, raw.to_owned());
    }
}

fn extract_redacted_url(
    slot: &mut Option<RedactedUrl>,
    env_key: &str,
    secret_env: &mut BTreeMap<String, SecretString>,
) {
    if let Some(url) = slot.take() {
        record_secret(secret_env, env_key, url.into_inner());
    }
}

/// Discovers every leaf TOML path accepted by [`ZinderConfig`] by probing Serde
/// struct fields recursively in-process.
///
/// Fallback nodes under `node_fallbacks` are normalized to `node_fallbacks.<N>.*`.
#[must_use]
pub fn discover_schema_leaf_paths() -> BTreeSet<String> {
    let mut leaves = BTreeSet::new();
    let mut prefix_segments = Vec::new();
    collect_struct_paths(&mut prefix_segments, &mut leaves);
    leaves
}

fn collect_struct_paths(prefix_segments: &mut Vec<String>, leaves: &mut BTreeSet<String>) {
    let Some(fields) = probe_expected_fields(prefix_segments) else {
        if !prefix_segments.is_empty() {
            let _inserted = leaves.insert(normalize_discovered_path(prefix_segments));
        }
        return;
    };

    for field in fields {
        prefix_segments.push(field);
        if prefix_segments.as_slice() == ["node_fallbacks"] {
            prefix_segments.push("1".to_owned());
            collect_struct_paths(prefix_segments, leaves);
            let _popped = prefix_segments.pop();
        } else {
            collect_struct_paths(prefix_segments, leaves);
        }
        let _popped = prefix_segments.pop();
    }
}

fn normalize_discovered_path(segments: &[String]) -> String {
    if segments.len() >= 2 && segments[0] == "node_fallbacks" && segments[1] == "1" {
        let mut normalized = Vec::with_capacity(segments.len());
        normalized.push("node_fallbacks".to_owned());
        normalized.push("<N>".to_owned());
        normalized.extend_from_slice(&segments[2..]);
        normalized.join(".")
    } else {
        segments.join(".")
    }
}

fn probe_expected_fields(prefix_segments: &[String]) -> Option<Vec<String>> {
    let mut inner = toml::Table::new();
    let _inserted = inner.insert(SCHEMA_PROBE_SENTINEL.to_owned(), toml::Value::Boolean(true));
    let mut probe = toml::Value::Table(inner);
    for segment in prefix_segments.iter().rev() {
        let mut parent = toml::Table::new();
        let _inserted = parent.insert(segment.clone(), probe);
        probe = toml::Value::Table(parent);
    }

    let outcome = probe.try_into::<ZinderConfig>();
    let Err(err) = outcome else {
        return None;
    };
    parse_expected_fields_from_serde_error(&err.to_string())
}

fn parse_expected_fields_from_serde_error(message: &str) -> Option<Vec<String>> {
    let marker = format!("unknown field `{SCHEMA_PROBE_SENTINEL}`");
    let (_, after_marker) = message.split_once(&marker)?;
    let (_, after_expected) = after_marker.split_once("expected ")?;
    let first_line = after_expected.lines().next().unwrap_or(after_expected);
    let list_part = first_line.split(" for key ").next().unwrap_or(first_line);

    let mut fields = Vec::new();
    let mut cursor = list_part;
    while let Some((_, after_open)) = cursor.split_once('`') {
        let Some((field, rest)) = after_open.split_once('`') else {
            break;
        };
        fields.push(field.to_owned());
        cursor = rest;
    }
    (!fields.is_empty()).then_some(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConfigLoader, REDACTED_PLACEHOLDER};

    fn sample_full_node(label: &str) -> NodeSection {
        NodeSection {
            json_rpc_addr: Some(format!("http://rpc-user:rpc-{label}@127.0.0.1:8232")),
            indexer_grpc_addr: Some(format!("https://idx-user:idx-{label}@127.0.0.1:8233")),
            ..Default::default()
        }
    }

    #[test]
    fn reject_writer_only_secrets_catches_every_writer_secret_field() -> Result<(), ConfigError> {
        type SecretFieldCase = (&'static str, fn(&mut ZinderConfig));
        let cases: [SecretFieldCase; 8] = [
            ("ingest_control.checkpoint_bearer_token", |cfg| {
                cfg.ingest_control.checkpoint_bearer_token =
                    SecretInput::new("secret-cp-token").ok();
            }),
            ("ingest_control.checkpoint_bearer_token_path", |cfg| {
                cfg.ingest_control.checkpoint_bearer_token_path =
                    Some(std::path::PathBuf::from("/etc/zinder/cp.token"));
            }),
            ("projector_control.bearer_token", |cfg| {
                cfg.projector_control.bearer_token = SecretInput::new("secret-proj-token").ok();
            }),
            ("projector_control.bearer_token_path", |cfg| {
                cfg.projector_control.bearer_token_path =
                    Some(std::path::PathBuf::from("/etc/zinder/proj.token"));
            }),
            ("node.indexer_tls.client_key_pem", |cfg| {
                cfg.node.indexer_tls.client_key_pem = SecretNodeInput::new("pem-key").ok();
            }),
            ("node.indexer_tls.client_key_path", |cfg| {
                cfg.node.indexer_tls.client_key_path =
                    Some(std::path::PathBuf::from("/etc/zinder/client.key"));
            }),
            ("node_fallbacks.1.indexer_tls.client_key_pem", |cfg| {
                let mut fb = NodeSection::default();
                fb.indexer_tls.client_key_pem = SecretNodeInput::new("fb-pem-key").ok();
                cfg.node_fallbacks = NodeFallbackSections::from_positions([(1, fb)].into());
            }),
            ("node_fallbacks.1.indexer_tls.client_key_path", |cfg| {
                let mut fb = NodeSection::default();
                fb.indexer_tls.client_key_path =
                    Some(std::path::PathBuf::from("/etc/zinder/fb-client.key"));
                cfg.node_fallbacks = NodeFallbackSections::from_positions([(1, fb)].into());
            }),
        ];

        for (expected_field, mutate) in cases {
            let mut cfg = ZinderConfig::default();
            mutate(&mut cfg);
            let Err(err) = cfg.reject_writer_only_secrets() else {
                return Err(ConfigError::invalid(format!(
                    "expected writer-only secret rejection for {expected_field}"
                )));
            };
            let msg = err.to_string();
            assert!(
                msg.contains(expected_field),
                "missing {expected_field} in {msg}"
            );
        }
        Ok(())
    }

    #[test]
    fn extract_secrets_for_launch_scrubs_toml_and_round_trips()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut cfg = ZinderConfig::default();
        cfg.network.name = Some("zcash-regtest".to_owned());
        cfg.observability.otlp_endpoint = Some("https://collector.internal:4317".parse()?);
        cfg.observability.sampling_rate = Some(0.25);
        cfg.node = sample_full_node("primary");
        cfg.node.health.addr = Some("https://hz:secret@127.0.0.1:8080/ready".to_owned());
        cfg.node.auth.password = Some(SecretNodeInput::new("pw-secret")?);
        cfg.node.auth.path = Some(std::path::PathBuf::from("/run/zebra/.cookie"));
        cfg.node.indexer_tls.client_key_pem = Some(SecretNodeInput::new("key-pem-secret")?);
        cfg.node.indexer_tls.client_key_path =
            Some(std::path::PathBuf::from("/etc/zinder/key.pem"));
        let mut fallbacks = BTreeMap::new();
        let _inserted = fallbacks.insert(1_u16, sample_full_node("fb1"));
        cfg.node_fallbacks = NodeFallbackSections::from_positions(fallbacks);
        cfg.ingest_control.addr = Some(RedactedUrl::new("https://ctrl:secret@127.0.0.1:9001")?);
        cfg.ingest_control.bearer_token = Some(SecretInput::new("ic-bearer-secret")?);
        cfg.ingest_control.checkpoint_bearer_token = Some(SecretInput::new("ic-cp-secret")?);
        cfg.projector_control.bearer_token = Some(SecretInput::new("pc-bearer-secret")?);
        cfg.query.proxy_protocol_trusted_cidrs =
            toml::from_str::<QuerySection>("proxy_protocol_trusted_cidrs = \"10.0.0.0/8\"")?
                .proxy_protocol_trusted_cidrs;
        cfg.compat.browser_origins = crate::BrowserOrigins::default();
        cfg.compat.proxy_protocol_trusted_cidrs =
            toml::from_str::<CompatSection>("proxy_protocol_trusted_cidrs = \"192.168.0.0/16\"")?
                .proxy_protocol_trusted_cidrs;
        cfg.explorer.bearer_token = Some(SecretInput::new("exp-bearer-secret")?);
        cfg.explorer.wallet_query_endpoint =
            Toggle::Enabled(RedactedUrl::new("https://wq:secret@127.0.0.1:9052")?);
        cfg.cipherscan.bearer_token = Some(SecretInput::new("cs-bearer-secret")?);
        cfg.cipherscan.explorer_query_endpoint =
            Some(RedactedUrl::new("https://eq:secret@127.0.0.1:9060")?);
        cfg.cipherscan.wallet_query_endpoint =
            Some(RedactedUrl::new("https://cwq:secret@127.0.0.1:9052")?);
        cfg.cipherscan.current_price_endpoint =
            Some(RedactedUrl::new("https://price:secret@api.example/now")?);
        cfg.cipherscan.historical_price_endpoint_template =
            Some(RedactedUrl::new("https://hist:secret@api.example/{date}")?);
        let (non_secret, secret_env) = cfg.extract_secrets_for_launch();
        let toml_text = toml::to_string_pretty(&non_secret)?;
        assert!(!toml_text.contains(REDACTED_PLACEHOLDER), "{toml_text}");
        for forbidden in [
            "secret",
            "collector.internal",
            "pw-secret",
            "key-pem-secret",
            "ic-bearer-secret",
            "ic-cp-secret",
            "pc-bearer-secret",
            "exp-bearer-secret",
            "cs-bearer-secret",
            "10.0.0.0/8",
            "192.168.0.0/16",
            "/run/zebra/.cookie",
            "/etc/zinder/key.pem",
        ] {
            assert!(
                !toml_text.contains(forbidden),
                "unexpected secret/path `{forbidden}` in non-secret TOML:\n{toml_text}"
            );
        }

        let dir = tempfile::TempDir::new()?;
        let cfg_file = dir.path().join("non_secret.toml");
        std::fs::write(&cfg_file, &toml_text)?;
        let env_pairs: Vec<(String, String)> = secret_env
            .into_iter()
            .map(|(key, secret)| {
                (
                    key,
                    secrecy::ExposeSecret::expose_secret(&secret).to_owned(),
                )
            })
            .collect();
        let reloaded: ZinderConfig = ConfigLoader::new()
            .with_file(Some(cfg_file))
            .with_zinder_env_from_map(env_pairs)?
            .load()?;
        assert_eq!(reloaded, cfg);
        Ok(())
    }

    #[test]
    fn discover_schema_leaf_paths_covers_nested_and_fallback_paths() {
        let paths = discover_schema_leaf_paths();
        assert!(paths.contains("deployment.profile"), "{paths:?}");
        assert!(paths.contains("observability.otlp_endpoint"), "{paths:?}");
        assert!(paths.contains("observability.sampling_rate"), "{paths:?}");
        assert!(paths.contains("storage.path"), "{paths:?}");
        assert!(
            paths.contains("wallet.rocksdb.block_cache_bytes"),
            "{paths:?}"
        );
        assert!(
            paths.contains("node_fallbacks.<N>.json_rpc_addr"),
            "{paths:?}"
        );
        assert!(
            paths.contains("node_fallbacks.<N>.indexer_tls.client_key_pem"),
            "{paths:?}"
        );
        assert!(
            paths.contains("compat.range_admission.max_streams_per_client"),
            "{paths:?}"
        );
    }
}
