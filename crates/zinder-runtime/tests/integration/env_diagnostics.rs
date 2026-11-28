//! End-to-end coverage of the type-directed configuration loader,
//! strict parsing, secret string fidelity, and env-var provenance diagnostics.

#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use eyre::{Result, eyre};
use serde::Deserialize;
use zinder_core::Network;
use zinder_runtime::{ConfigError, ConfigLoader, ObservabilityConfig};
use zinder_source::{NodeAuth, NodeFallbackSections, NodeSection, NodeTarget};

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ToyConfig {
    ops: OpsSection,
    typed: TypedSection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct OpsSection {
    listen_addr: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TypedSection {
    secret: Option<String>,
    required_secret: String,
    flag: Option<bool>,
    count_u64: Option<u64>,
    timeout_secs: Option<u32>,
    tags: Vec<String>,
}

fn load_toy_env(env_pair: (&str, &str)) -> Result<ToyConfig, ConfigError> {
    ConfigLoader::new()
        .with_zinder_env_from_map([(env_pair.0.to_owned(), env_pair.1.to_owned())])?
        .load::<ToyConfig>()
}

#[test]
fn single_underscore_env_var_is_attributed_to_its_zinder_name() -> Result<()> {
    let outcome = load_toy_env(("ZINDER_OPS_LISTEN_ADDR", "0.0.0.0:9069"));
    let Err(error) = outcome else {
        return Err(eyre!(
            "expected single-underscore env var to be rejected, got Ok"
        ));
    };
    let ConfigError::RejectedEnvVar {
        original_name,
        rejected_key,
        suggested_name,
        ..
    } = error
    else {
        return Err(eyre!("expected ConfigError::RejectedEnvVar, got {error:?}"));
    };
    assert_eq!(original_name, "ZINDER_OPS_LISTEN_ADDR");
    assert_eq!(rejected_key, "ops_listen_addr");
    assert_eq!(suggested_name.as_deref(), Some("ZINDER_OPS__LISTEN_ADDR"));
    Ok(())
}

#[test]
fn rejected_env_var_display_carries_the_suggestion() -> Result<()> {
    let Err(error) = load_toy_env(("ZINDER_OPS_LISTEN_ADDR", "0.0.0.0:9069")) else {
        return Err(eyre!("expected loader to reject single-underscore env var"));
    };
    let rendered = error.to_string();
    assert!(
        rendered.contains("ZINDER_OPS_LISTEN_ADDR"),
        "rendered error must name the original env var:\n{rendered}",
    );
    assert!(
        rendered.contains("Try `ZINDER_OPS__LISTEN_ADDR` instead."),
        "rendered error must suggest the corrected env var name:\n{rendered}",
    );
    Ok(())
}

#[test]
fn double_underscore_env_var_loads_successfully() -> Result<()> {
    let config = load_toy_env(("ZINDER_OPS__LISTEN_ADDR", "0.0.0.0:9069"))?;
    assert_eq!(config.ops.listen_addr.as_deref(), Some("0.0.0.0:9069"));
    Ok(())
}

#[test]
fn env_var_outside_zinder_prefix_is_ignored() -> Result<()> {
    let config = load_toy_env(("UNRELATED_OPS_LISTEN_ADDR", "0.0.0.0:9069"))?;
    assert!(config.ops.listen_addr.is_none());
    Ok(())
}

#[test]
fn test_only_env_var_is_stripped_before_deserialization() -> Result<()> {
    let config = load_toy_env(("ZINDER_TEST_LIVE", "1"))?;
    assert!(config.ops.listen_addr.is_none());
    Ok(())
}

#[test]
fn string_and_secret_env_values_preserve_exact_bytes() -> Result<()> {
    for literal in [
        "0123456",
        "000123",
        "1e5",
        "TRUE",
        "False",
        "none",
        "p$ssw0rd!#%",
        "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
    ] {
        let loaded: ToyConfig = ConfigLoader::new()
            .with_zinder_env_from_map([
                ("ZINDER_TYPED__SECRET".to_owned(), literal.to_owned()),
                (
                    "ZINDER_TYPED__REQUIRED_SECRET".to_owned(),
                    literal.to_owned(),
                ),
            ])?
            .load()?;
        assert_eq!(loaded.typed.secret.as_deref(), Some(literal));
        assert_eq!(loaded.typed.required_secret, literal);
    }
    Ok(())
}

#[test]
fn env_booleans_and_integers_are_strict_and_preserve_u64_precision() -> Result<()> {
    let max_u64 = u64::MAX.to_string();
    let valid: ToyConfig = ConfigLoader::new()
        .with_zinder_env_from_map([
            ("ZINDER_TYPED__FLAG".to_owned(), "true".to_owned()),
            ("ZINDER_TYPED__COUNT_U64".to_owned(), max_u64),
            ("ZINDER_TYPED__TIMEOUT_SECS".to_owned(), "42".to_owned()),
        ])?
        .load()?;
    assert_eq!(valid.typed.flag, Some(true));
    assert_eq!(valid.typed.count_u64, Some(u64::MAX));
    assert_eq!(valid.typed.timeout_secs, Some(42));

    let false_cfg = load_toy_env(("ZINDER_TYPED__FLAG", "false"))?;
    assert_eq!(false_cfg.typed.flag, Some(false));

    for bad_bool in ["1", "0", "yes", "on", "2", "", "TRUE"] {
        let outcome = load_toy_env(("ZINDER_TYPED__FLAG", bad_bool));
        let Err(error) = outcome else {
            return Err(eyre!("expected bool `{bad_bool}` to be rejected"));
        };
        assert!(
            matches!(
                error,
                ConfigError::InvalidEnvValue {
                    ref original_name,
                    ref key,
                    ..
                } if original_name == "ZINDER_TYPED__FLAG" && key == "typed.flag"
            ),
            "unexpected error for `{bad_bool}`: {error:?}"
        );
    }

    for bad_int in ["on", "yes", "3.7", "", "18446744073709551616", "secret-val"] {
        let outcome = load_toy_env(("ZINDER_TYPED__COUNT_U64", bad_int));
        let Err(error) = outcome else {
            return Err(eyre!("expected integer `{bad_int}` to be rejected"));
        };
        assert!(
            matches!(
                error,
                ConfigError::InvalidEnvValue {
                    ref original_name,
                    ref key,
                    ..
                } if original_name == "ZINDER_TYPED__COUNT_U64" && key == "typed.count_u64"
            ),
            "unexpected error for `{bad_int}`: {error:?}"
        );
        assert!(
            !error.to_string().contains("secret-val"),
            "error must not echo raw env value: {error}"
        );
    }
    Ok(())
}

#[test]
fn toml_scalars_are_strict_without_coercion() -> Result<()> {
    let dir = tempfile::tempdir()?;
    for bad_toml in [
        "[typed]\nflag = 2\n",
        "[typed]\nflag = \"on\"\n",
        "[typed]\nflag = \"true\"\n",
        "[typed]\ntimeout_secs = \"on\"\n",
        "[typed]\ntimeout_secs = 3.7\n",
        "[typed]\ntimeout_secs = true\n",
    ] {
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, bad_toml)?;
        let outcome = ConfigLoader::new()
            .with_file(Some(path))
            .load::<ToyConfig>();
        assert!(
            matches!(outcome, Err(ConfigError::Deserialize { .. })),
            "expected Deserialize failure for `{bad_toml}`, got {outcome:?}"
        );
    }
    Ok(())
}

#[test]
fn env_csv_and_toml_arrays_deserialize_sequences() -> Result<()> {
    let from_env = load_toy_env(("ZINDER_TYPED__TAGS", "alpha, beta , gamma"))?;
    assert_eq!(from_env.typed.tags, vec!["alpha", "beta", "gamma"]);

    let from_empty_env = load_toy_env(("ZINDER_TYPED__TAGS", ""))?;
    assert_eq!(from_empty_env.typed.tags, Vec::<String>::new());

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("tags.toml");
    std::fs::write(&path, "[typed]\ntags = [\"one\", \"two\"]\n")?;
    let from_toml: ToyConfig = ConfigLoader::new().with_file(Some(path)).load()?;
    assert_eq!(from_toml.typed.tags, vec!["one", "two"]);
    Ok(())
}

#[test]
fn nested_env_typos_malformed_keys_and_collisions_are_rejected() -> Result<()> {
    let nested_typo = load_toy_env(("ZINDER_OPS__LISTEN_ADRR", "127.0.0.1:9069"));
    assert!(matches!(
        nested_typo,
        Err(ConfigError::RejectedEnvVar {
            ref original_name,
            ref rejected_key,
            ref suggested_name,
            ..
        }) if original_name == "ZINDER_OPS__LISTEN_ADRR"
            && rejected_key == "ops.listen_adrr"
            && suggested_name.as_deref() == Some("ZINDER_OPS__LISTEN_ADDR")
    ));

    for malformed in [
        "ZINDER_",
        "ZINDER___FOO",
        "ZINDER_OPS__",
        "ZINDER_OPS____ADDR",
    ] {
        let outcome = load_toy_env((malformed, "x"));
        assert!(
            matches!(
                outcome,
                Err(ConfigError::RejectedEnvVar {
                    ref original_name,
                    suggested_name: None,
                    ..
                }) if original_name == malformed
            ),
            "expected RejectedEnvVar for `{malformed}`, got {outcome:?}"
        );
    }

    let case_collision = ConfigLoader::new()
        .with_zinder_env_from_map([
            (
                "ZINDER_OPS__LISTEN_ADDR".to_owned(),
                "127.0.0.1:1".to_owned(),
            ),
            (
                "ZINDER_OPS__listen_addr".to_owned(),
                "127.0.0.1:2".to_owned(),
            ),
        ])?
        .load::<ToyConfig>();
    assert!(matches!(case_collision, Err(ConfigError::Invalid { .. })));

    let table_leaf_collision = ConfigLoader::new()
        .with_default("ops.listen_addr", "127.0.0.1:9069")?
        .with_zinder_env_from_map([("ZINDER_OPS".to_owned(), "scalar-secret".to_owned())])?
        .load::<ToyConfig>();
    assert!(
        matches!(table_leaf_collision, Err(ConfigError::Invalid { ref reason }) if reason.contains("ZINDER_OPS") && !reason.contains("scalar-secret")),
        "unexpected outcome: {table_leaf_collision:?}"
    );
    Ok(())
}

#[test]
fn node_type_errors_and_fallback_merges_preserve_provenance_and_precedence() -> Result<()> {
    #[derive(Debug, Default, Deserialize)]
    #[serde(default, deny_unknown_fields)]
    struct NodeProbe {
        node: NodeSection,
        node_fallbacks: NodeFallbackSections,
        ops: OpsSection,
    }

    let type_err = ConfigLoader::new()
        .with_zinder_env_from_map([(
            "ZINDER_NODE__REQUEST_TIMEOUT_SECS".to_owned(),
            "30s".to_owned(),
        )])?
        .load::<NodeProbe>();
    assert!(
        matches!(
            type_err,
            Err(ConfigError::InvalidEnvValue {
                ref original_name,
                ref key,
                ..
            }) if original_name == "ZINDER_NODE__REQUEST_TIMEOUT_SECS"
                && key == "node.request_timeout_secs"
        ),
        "unexpected outcome: {type_err:?}"
    );

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("precedence.toml");
    std::fs::write(
        &path,
        concat!(
            "[ops]\nlisten_addr = \"127.0.0.1:2000\"\n",
            "[node]\njson_rpc_addr = \"http://127.0.0.1:8232\"\n",
            "[node_fallbacks.1]\njson_rpc_addr = \"http://127.0.0.1:18232\"\n",
            "[node_fallbacks.1.auth]\nmethod = \"basic\"\nusername = \"file-user\"\npassword = \"from-file\"\n",
        ),
    )?;

    let loaded: NodeProbe = ConfigLoader::new()
        .with_override_if("ops.listen_addr", Some("127.0.0.1:4000"))?
        .with_default("ops.listen_addr", "127.0.0.1:1000")?
        .with_file(Some(path))
        .with_zinder_env_from_map([
            (
                "ZINDER_OPS__LISTEN_ADDR".to_owned(),
                "127.0.0.1:3000".to_owned(),
            ),
            (
                "ZINDER_NODE_FALLBACKS__1__AUTH__PASSWORD".to_owned(),
                "0123456".to_owned(),
            ),
        ])?
        .load()?;
    assert_eq!(loaded.ops.listen_addr.as_deref(), Some("127.0.0.1:4000"));
    let fallbacks = NodeTarget::resolve_fallbacks(Network::ZcashRegtest, loaded.node_fallbacks)?;
    assert_eq!(fallbacks.len(), 1);
    assert!(matches!(
        fallbacks[0].node_auth,
        NodeAuth::Basic { ref username, .. } if username == "file-user"
    ));
    let rendered = toml::to_string(&zinder_runtime::NodeToml::from_node_target(&fallbacks[0]))?;
    assert!(rendered.contains("password = \"[REDACTED]\""), "{rendered}");
    assert!(!rendered.contains("0123456"), "{rendered}");
    Ok(())
}

#[test]
fn section_validation_attributes_failing_leaf_without_blaming_siblings() -> Result<()> {
    #[derive(Debug, Default, Deserialize)]
    #[serde(default, deny_unknown_fields)]
    struct ObservabilityProbe {
        ops: OpsSection,
        observability: ObservabilityConfig,
    }

    let invalid_rate = ConfigLoader::new()
        .with_zinder_env_from_map([
            (
                "ZINDER_OBSERVABILITY__OTLP_ENDPOINT".to_owned(),
                "http://127.0.0.1:4317".to_owned(),
            ),
            (
                "ZINDER_OBSERVABILITY__SAMPLING_RATE".to_owned(),
                "2".to_owned(),
            ),
            (
                "ZINDER_OPS__LISTEN_ADDR".to_owned(),
                "127.0.0.1:9105".to_owned(),
            ),
        ])?
        .load::<ObservabilityProbe>();
    assert!(
        matches!(
            invalid_rate,
            Err(ConfigError::InvalidEnvValue {
                ref original_name,
                ref key,
                ..
            }) if original_name == "ZINDER_OBSERVABILITY__SAMPLING_RATE"
                && key == "observability.sampling_rate"
        ),
        "unexpected outcome for env sampling rate: {invalid_rate:?}"
    );

    let dir = tempfile::tempdir()?;
    let toml_path = dir.path().join("obs.toml");
    std::fs::write(&toml_path, "[observability]\nsampling_rate = 2.0\n")?;
    let toml_rate = ConfigLoader::new()
        .with_file(Some(toml_path))
        .with_zinder_env_from_map([(
            "ZINDER_OBSERVABILITY__OTLP_ENDPOINT".to_owned(),
            "http://127.0.0.1:4317".to_owned(),
        )])?
        .load::<ObservabilityProbe>();
    assert!(
        matches!(
            toml_rate,
            Err(ConfigError::Deserialize {
                path: Some(ref failing_path),
                ..
            }) if failing_path == "observability.sampling_rate"
        ),
        "TOML validation failure must not blame sibling env var: {toml_rate:?}"
    );

    let invalid_endpoint = ConfigLoader::new()
        .with_zinder_env_from_map([
            (
                "ZINDER_OBSERVABILITY__OTLP_ENDPOINT".to_owned(),
                "not-a-uri".to_owned(),
            ),
            (
                "ZINDER_OBSERVABILITY__SAMPLING_RATE".to_owned(),
                "0.5".to_owned(),
            ),
        ])?
        .load::<ObservabilityProbe>();
    assert!(
        matches!(
            invalid_endpoint,
            Err(ConfigError::InvalidEnvValue {
                ref original_name,
                ref key,
                ..
            }) if original_name == "ZINDER_OBSERVABILITY__OTLP_ENDPOINT"
                && key == "observability.otlp_endpoint"
        ),
        "unexpected outcome for env otlp endpoint: {invalid_endpoint:?}"
    );
    Ok(())
}
