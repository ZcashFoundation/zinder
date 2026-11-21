//! Operator-facing environment-variable reference for Zinder service binaries.
//!
//! The `ZINDER_*` env-var schema is part of Zinder's public interface (see
//! [Configuration §Environment variables]). The entries in
//! [`ENVIRONMENT_VARIABLES`] are the hand-maintained description of that
//! surface and feed the Markdown table embedded in
//! `docs/reference/configuration.md`.
//!
//! [`render_environment_variable_table`] returns the Markdown the doc block
//! must contain. The doc-mirror integration test in this crate asserts exact
//! equality and prints the regeneration command on failure.
//!
//! Two kinds of drift are caught, in different places:
//!
//! - Doc against table: the doc-mirror test above.
//! - Table against loaders: each service's `env_var_contract` integration
//!   test runs the binary's real configuration loader. Every row naming the
//!   binary must be accepted, and every key the loader accepts must have a
//!   row naming the binary, except keys the test lists as accepted only
//!   because the section type is shared.
//!
//! A row lists a binary when the binary reads the variable into its
//! configuration. A description says when a listed binary does not apply the
//! value, as for `RocksDB` background job caps on secondary opens.
//!
//! Neither check validates descriptions, defaults, requirement, sensitivity,
//! or whether a binary acts on a variable it accepts. Adding a config field
//! means adding a row here and regenerating the doc block.
//!
//! [Configuration §Environment variables]:
//!     ../../docs/reference/configuration.md#environment-variables

use std::fmt::Write as _;

/// One operator-facing environment variable.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EnvVarDoc {
    /// Variable name in the canonical `ZINDER_<SECTION>__<FIELD>` form.
    pub name: &'static str,
    /// The TOML config field the variable resolves to. Mirrors the inverse
    /// mapping from the env-var name back to its config struct's field path.
    pub toml_path: &'static str,
    /// Which Zinder binaries consume this variable. Used for the "Used by"
    /// column so operators can scan the table for one runtime at a time.
    pub used_by: &'static [&'static str],
    /// Whether the field is required, optional, or required-when-conditioned
    /// (string descriptors keep the column compact).
    pub requirement: Requirement,
    /// Whether the value is sensitive (redacted in `--print-config`, structured
    /// logs, and `Debug` impls).
    pub sensitive: bool,
    /// One-line operator-facing description.
    pub description: &'static str,
}

impl EnvVarDoc {
    /// Returns whether `binary` consumes this variable.
    #[must_use]
    pub fn is_used_by(&self, binary: &str) -> bool {
        self.used_by.contains(&binary)
    }
}

/// Whether an env var is required, optional, or conditional.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Requirement {
    /// Binary fails to start without this variable (or its config-file
    /// equivalent).
    Required,
    /// Variable is optional; a default applies when unset.
    Optional,
    /// Required only when the named precondition is met (e.g. another env
    /// var has a specific value).
    ConditionalOn(&'static str),
}

impl Requirement {
    fn label(self) -> String {
        match self {
            Self::Required => "Required".to_owned(),
            Self::Optional => "Optional".to_owned(),
            Self::ConditionalOn(condition) => format!("When `{condition}`"),
        }
    }
}

/// Operator-facing `ZINDER_*` environment variables advertised by Zinder
/// service binaries.
///
/// Mirrored into `docs/reference/configuration.md` between the
/// `<!-- env-var-table:public-interfaces:start -->` and corresponding `:end`
/// markers; the doc-mirror test in
/// `crates/zinder-runtime/tests/integration/env_var_docs.rs` fails when the
/// rendered table and the doc block diverge. Each service's
/// `tests/integration/env_var_contract.rs` checks the rows against that
/// binary's loader.
///
/// `ZINDER_TEST_*` variables are intentionally absent: they are stripped at
/// the runtime config loader so production binaries never observe them.
/// Live-test environment knobs are documented next to the affected tests in
/// `CLAUDE.md` and `docs/runbooks/testing.md`.
pub const ENVIRONMENT_VARIABLES: &[EnvVarDoc] = &[
    EnvVarDoc {
        name: "ZINDER_NETWORK__NAME",
        toml_path: "network.name",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
            "zinder-compat-cipherscan",
        ],
        requirement: Requirement::Required,
        sensitive: false,
        description: "Network identifier: `zcash-mainnet`, `zcash-testnet`, or `zcash-regtest`. \
                      Note: live-test gating reads the bare `ZINDER_NETWORK` env var directly \
                      and never reaches the config loader, so test runbooks still quote that form.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__JSON_RPC_ADDR",
        toml_path: "node.json_rpc_addr",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::ConditionalOn(
            "running zinder-ingest, or broadcasting transactions or reading the recommended fee from a reader",
        ),
        sensitive: false,
        description: "Upstream Zebra JSON-RPC URL. `zinder-ingest` requires it. `zinder-query` \
                      and wallet-mode `zinder-compat-lightwalletd` broadcast transactions \
                      through it, compatibility also reads the recommended fee for \
                      `GetStandardFee` through it, and both start without contacting it. \
                      Without it they serve \
                      storage only and advertise no broadcast, and compatibility answers \
                      `GetStandardFee` with `UNIMPLEMENTED`. Compact-blocks compatibility \
                      rejects every `ZINDER_NODE__*` variable. Optional for \
                      `zinder-explorer`: without it the upstream-observation probe stays \
                      off and `ExplorerFreshness.chain_view.upstream_tip` is always unset.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__INDEXER_GRPC_ADDR",
        toml_path: "node.indexer_grpc_addr",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Optional Zebra indexer gRPC endpoint enabling the streaming mempool source. \
                      Falls back to JSON-RPC mempool polling when unset or empty. An `https://` \
                      URL negotiates TLS, configured by the `ZINDER_NODE__INDEXER_TLS__*` \
                      variables; without them the node is verified against the system roots.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH",
        toml_path: "node.indexer_tls.ca_cert_path",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "PEM CA bundle that verifies the indexer's server certificate instead of the \
                      system roots. Requires an `https://` `ZINDER_NODE__INDEXER_GRPC_ADDR`. Read \
                      at startup and whenever an indexer stream reconnects.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH",
        toml_path: "node.indexer_tls.client_cert_path",
        used_by: &["zinder-ingest"],
        requirement: Requirement::ConditionalOn("ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH is set"),
        sensitive: false,
        description: "PEM client certificate chain presented to an indexer that requires mutual \
                      TLS. Set together with `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH",
        toml_path: "node.indexer_tls.client_key_path",
        used_by: &["zinder-ingest"],
        requirement: Requirement::ConditionalOn(
            "ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH is set",
        ),
        sensitive: false,
        description: "Path to the PEM private key for the indexer client certificate. The path is \
                      shown in `--print-config`; the key is read at startup and whenever an \
                      indexer stream reconnects. The `*_PEM` variables that the single-volume \
                      image entrypoints convert into these files are refused by the binaries.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__INDEXER_TLS__SERVER_NAME",
        toml_path: "node.indexer_tls.server_name",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Name verified against the indexer's certificate when it differs from the \
                      host in `ZINDER_NODE__INDEXER_GRPC_ADDR`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__AUTH__METHOD",
        toml_path: "node.auth.method",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Upstream-node auth shape: `basic`, `cookie`, or unset for no auth.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__AUTH__USERNAME",
        toml_path: "node.auth.username",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::ConditionalOn("ZINDER_NODE__AUTH__METHOD=basic"),
        sensitive: false,
        description: "Basic-auth username. Paired with `ZINDER_NODE__AUTH__PASSWORD`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__AUTH__PASSWORD",
        toml_path: "node.auth.password",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::ConditionalOn("ZINDER_NODE__AUTH__METHOD=basic"),
        sensitive: true,
        description: "Basic-auth password. Redacted in `--print-config` and structured logs.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__AUTH__PATH",
        toml_path: "node.auth.path",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::ConditionalOn("ZINDER_NODE__AUTH__METHOD=cookie"),
        sensitive: false,
        description: "Path to a cookie file. Mutually exclusive with `ZINDER_NODE__AUTH__COOKIE`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__AUTH__COOKIE",
        toml_path: "node.auth.cookie",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::ConditionalOn("ZINDER_NODE__AUTH__METHOD=cookie"),
        sensitive: true,
        description: "Inline cookie credentials (`username:password`). Mutually exclusive with \
                      `ZINDER_NODE__AUTH__PATH`. Accepted for PaaS environments without \
                      persistent disks.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__REQUEST_TIMEOUT_SECS",
        toml_path: "node.request_timeout_secs",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Upstream-node JSON-RPC request timeout in seconds. Defaults to 30.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__MAX_RESPONSE_BYTES",
        toml_path: "node.max_response_bytes",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum JSON-RPC response body size (bytes) accepted from the node. \
                      Defaults to 67108864 (64 MiB). This is a ceiling on what Zinder accepts, \
                      not a request size: Zebra 7.1.0 and Zakura 1.8.6 refuse responses above \
                      50 MiB by default, and ingest splits a source segment the node refuses.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__BROADCAST_TIMEOUT_SECS",
        toml_path: "node.broadcast_timeout_secs",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Per-call timeout (seconds) applied only to `sendrawtransaction`. When \
                      unset, the global `request_timeout_secs` applies instead. Recommended: 7. \
                      With fallback endpoints, a broadcast moves to the next endpoint only after \
                      the endpoint's own timeout, so keep it well below wallet request \
                      deadlines. Fallback endpoints do not inherit it; set \
                      `ZINDER_NODE_FALLBACKS__<N>__BROADCAST_TIMEOUT_SECS` for each.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__HEALTH__ADDR",
        toml_path: "node.health.addr",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "URL of the upstream's HTTP `/ready` endpoint. When set, ingest, the \
                      wallet-serving readers, and `zinder-explorer` poll it as the primary \
                      upstream-sync signal; when unset, they fall back \
                      to `getblockchaininfo.verificationprogress`/`estimatedheight`. \
                      Compatibility with `compat.serving = \"compact-blocks\"` rejects \
                      every node setting. See \
                      [ADR-0009](../adrs/0009-ingest-lifecycle-and-construction.md).",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__HEALTH__POLL_INTERVAL_MS",
        toml_path: "node.health.poll_interval_ms",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Cadence of the upstream-health probe in milliseconds. Defaults to 30000. \
                      Must be greater than zero. Query and wallet-serving compatibility use \
                      it to verify every broadcast endpoint, `[node]` and each fallback, on \
                      first contact and then refresh the node dependency their readiness \
                      reports; compact-blocks \
                      compatibility rejects it; `zinder-explorer` reuses the same cadence for \
                      its upstream-observation probe (the one that populates \
                      `ExplorerFreshness.chain_view.upstream_tip`).",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__HEALTH__VERIFICATION_PROGRESS_FLOOR",
        toml_path: "node.health.verification_progress_floor",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Lower bound on `getblockchaininfo.verificationprogress` below which the \
                      fallback path reports `upstream_not_ready`. Defaults to 0.999. Must be in \
                      `(0.0, 1.0)`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__HEALTH__ESTIMATED_GAP_FLOOR_BLOCKS",
        toml_path: "node.health.estimated_gap_floor_blocks",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Block gap between `estimatedheight` and the local tip above which the \
                      fallback path reports `upstream_not_ready`. Defaults to 10.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__JSON_RPC_ADDR",
        toml_path: "node_fallbacks.<N>.json_rpc_addr",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::ConditionalOn("any ZINDER_NODE_FALLBACKS__<N>__* is set"),
        sensitive: false,
        description: "JSON-RPC URL of fallback node endpoint `<N>`. Endpoints form one ordered list: \
                      `[node]` is position 0 and `[node_fallbacks.N]` is position `N`, starting at 1 \
                      with no gap. Every `ZINDER_NODE__*` field has a `ZINDER_NODE_FALLBACKS__<N>__*` \
                      counterpart. `zinder-ingest` switches to a fallback when its active endpoint \
                      fails. `zinder-query` and wallet-mode `zinder-compat-lightwalletd` broadcast, \
                      and compatibility reads the recommended fee, through the next verified \
                      endpoint when one is unreachable. `zinder-explorer` validates the list and \
                      reads only `[node]`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__INDEXER_GRPC_ADDR",
        toml_path: "node_fallbacks.<N>.indexer_grpc_addr",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Same as `ZINDER_NODE__INDEXER_GRPC_ADDR`, for fallback endpoint `<N>`. After a switch, ingest follows the mempool of the new endpoint through it.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__INDEXER_TLS__CA_CERT_PATH",
        toml_path: "node_fallbacks.<N>.indexer_tls.ca_cert_path",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Same as `ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH`, for fallback endpoint `<N>`. The single-volume image entrypoints also accept the `_PEM` form of this file.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__INDEXER_TLS__CLIENT_CERT_PATH",
        toml_path: "node_fallbacks.<N>.indexer_tls.client_cert_path",
        used_by: &["zinder-ingest"],
        requirement: Requirement::ConditionalOn(
            "ZINDER_NODE_FALLBACKS__<N>__INDEXER_TLS__CLIENT_KEY_PATH is set",
        ),
        sensitive: false,
        description: "Same as `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH`, for fallback endpoint `<N>`. The single-volume image entrypoints also accept the `_PEM` form of this file.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__INDEXER_TLS__CLIENT_KEY_PATH",
        toml_path: "node_fallbacks.<N>.indexer_tls.client_key_path",
        used_by: &["zinder-ingest"],
        requirement: Requirement::ConditionalOn(
            "ZINDER_NODE_FALLBACKS__<N>__INDEXER_TLS__CLIENT_CERT_PATH is set",
        ),
        sensitive: false,
        description: "Same as `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH`, for fallback endpoint `<N>`. The single-volume image entrypoints also accept the `_PEM` form of this file.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__INDEXER_TLS__SERVER_NAME",
        toml_path: "node_fallbacks.<N>.indexer_tls.server_name",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Same as `ZINDER_NODE__INDEXER_TLS__SERVER_NAME`, for fallback endpoint `<N>`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__AUTH__METHOD",
        toml_path: "node_fallbacks.<N>.auth.method",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Same as `ZINDER_NODE__AUTH__METHOD`, for fallback endpoint `<N>`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__AUTH__USERNAME",
        toml_path: "node_fallbacks.<N>.auth.username",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::ConditionalOn("ZINDER_NODE_FALLBACKS__<N>__AUTH__METHOD=basic"),
        sensitive: false,
        description: "Same as `ZINDER_NODE__AUTH__USERNAME`, for fallback endpoint `<N>`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__AUTH__PASSWORD",
        toml_path: "node_fallbacks.<N>.auth.password",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::ConditionalOn("ZINDER_NODE_FALLBACKS__<N>__AUTH__METHOD=basic"),
        sensitive: true,
        description: "Same as `ZINDER_NODE__AUTH__PASSWORD`, for fallback endpoint `<N>`. Redacted in `--print-config` and structured logs.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__AUTH__PATH",
        toml_path: "node_fallbacks.<N>.auth.path",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::ConditionalOn("ZINDER_NODE_FALLBACKS__<N>__AUTH__METHOD=cookie"),
        sensitive: false,
        description: "Same as `ZINDER_NODE__AUTH__PATH`, for fallback endpoint `<N>`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__AUTH__COOKIE",
        toml_path: "node_fallbacks.<N>.auth.cookie",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::ConditionalOn("ZINDER_NODE_FALLBACKS__<N>__AUTH__METHOD=cookie"),
        sensitive: true,
        description: "Same as `ZINDER_NODE__AUTH__COOKIE`, for fallback endpoint `<N>`. Redacted in `--print-config` and structured logs.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__REQUEST_TIMEOUT_SECS",
        toml_path: "node_fallbacks.<N>.request_timeout_secs",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Same as `ZINDER_NODE__REQUEST_TIMEOUT_SECS`, for fallback endpoint `<N>`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__MAX_RESPONSE_BYTES",
        toml_path: "node_fallbacks.<N>.max_response_bytes",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Same as `ZINDER_NODE__MAX_RESPONSE_BYTES`, for fallback endpoint `<N>`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__BROADCAST_TIMEOUT_SECS",
        toml_path: "node_fallbacks.<N>.broadcast_timeout_secs",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Same as `ZINDER_NODE__BROADCAST_TIMEOUT_SECS`, for fallback endpoint \
                      `<N>`. It is not inherited from `[node]`: when unset, this endpoint's \
                      own `request_timeout_secs` applies. Recommended: 7.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__HEALTH__ADDR",
        toml_path: "node_fallbacks.<N>.health.addr",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Same as `ZINDER_NODE__HEALTH__ADDR`, for fallback endpoint `<N>`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__HEALTH__POLL_INTERVAL_MS",
        toml_path: "node_fallbacks.<N>.health.poll_interval_ms",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Same as `ZINDER_NODE__HEALTH__POLL_INTERVAL_MS`, for fallback endpoint \
                      `<N>`, in ingest. Query and wallet-serving compatibility ignore it and \
                      verify and observe every endpoint on \
                      `ZINDER_NODE__HEALTH__POLL_INTERVAL_MS`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__HEALTH__VERIFICATION_PROGRESS_FLOOR",
        toml_path: "node_fallbacks.<N>.health.verification_progress_floor",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Same as `ZINDER_NODE__HEALTH__VERIFICATION_PROGRESS_FLOOR`, for fallback endpoint `<N>`.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE_FALLBACKS__<N>__HEALTH__ESTIMATED_GAP_FLOOR_BLOCKS",
        toml_path: "node_fallbacks.<N>.health.estimated_gap_floor_blocks",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Same as `ZINDER_NODE__HEALTH__ESTIMATED_GAP_FLOOR_BLOCKS`, for fallback endpoint `<N>`.",
    },
    EnvVarDoc {
        name: "ZINDER_OPS__LISTEN_ADDR",
        toml_path: "ops.listen_addr",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
            "zinder-compat-cipherscan",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Listen address for the operational HTTP endpoint \
                      (`/healthz`, `/readyz`, `/metrics`). Defaults to a per-service \
                      loopback address (`127.0.0.1:9105` ingest, `9110` projector, `9106` query, \
                      `9107` compat, `9069` explorer, `9108` cipherscan). Set to an empty string to \
                      disable the endpoint entirely.",
    },
    EnvVarDoc {
        name: "ZINDER_SECURITY__ALLOW_PUBLIC_BIND",
        toml_path: "security.allow_public_bind",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
            "zinder-compat-cipherscan",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Opts a binary in to binding its plaintext serving and operational \
                      surfaces to a public or unspecified (`0.0.0.0`, `::`) address. Defaults \
                      to `false`: a loopback or private-range bind is always allowed, but a \
                      public or unspecified bind is refused at startup unless this is `true`. \
                      Zinder ships no server TLS (ADR-0011); set this only when a reverse \
                      proxy terminates TLS and authorization in front of the listener.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST_CONTROL__LISTEN_ADDR",
        toml_path: "ingest_control.listen_addr",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Listen address of the private IngestControl gRPC endpoint. Localhost-only \
                      by default; cross-host deployments must add bearer-token auth per ADR-0011. \
                      Set to an empty string to disable the endpoint for diagnostic one-shot runs \
                      (such as `--target-height` pre-seed).",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST_CONTROL__ADDR",
        toml_path: "ingest_control.addr",
        used_by: &[
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "URL of the colocated IngestControl writer (`http://host:port`). Readers \
                      use it for tip-change subscriptions, mempool reads, and writer-status \
                      lookups. Defaults to `http://127.0.0.1:9100`.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST_CONTROL__BEARER_TOKEN_PATH",
        toml_path: "ingest_control.bearer_token_path",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
        ],
        requirement: Requirement::ConditionalOn("ingest enforces auth"),
        sensitive: false,
        description: "Path to the shared-secret bearer token the IngestControl endpoint enforces \
                      on every request (ADR-0011). The writer reads it to verify; the readers \
                      read the same file to present. File-only by policy; inline secrets are \
                      rejected at config load.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST_CONTROL__CHECKPOINT_STAGING_ROOT",
        toml_path: "ingest_control.checkpoint_staging_root",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Directory containing freshly prepared state-bundle candidate directories. \
                      CanonicalControl accepts only an opaque candidate id and creates its \
                      canonical checkpoint at `<root>/<candidate-id>/canonical.rocksdb`; \
                      production mounts this path from a dedicated staging volume into ingest \
                      and projector only, never query or compatibility. Defaults to \
                      `/var/lib/zinder/checkpoints`.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST_CONTROL__CHECKPOINT_BEARER_TOKEN_PATH",
        toml_path: "ingest_control.checkpoint_bearer_token_path",
        used_by: &["zinder-ingest"],
        requirement: Requirement::ConditionalOn("canonical checkpoint capture is enabled"),
        sensitive: false,
        description: "Path to the separate method-level token required by \
                      CanonicalControl.CreateOwnerCheckpoint and ReadmitOwnerCheckpoint. Mount \
                      this file only into ingest and projector; query and compatibility must not \
                      receive it.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR_CONTROL__LISTEN_ADDR",
        toml_path: "projector_control.listen_addr",
        used_by: &["zinder-projector"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Loopback-only private ProjectorControl gRPC endpoint for coherent capture. \
                      Empty or unset disables it; an enabled endpoint requires \
                      projector_control.bearer_token_path.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR_CONTROL__BEARER_TOKEN_PATH",
        toml_path: "projector_control.bearer_token_path",
        used_by: &["zinder-projector"],
        requirement: Requirement::ConditionalOn("projector control is enabled"),
        sensitive: false,
        description: "Path to the token required by ProjectorControl and presented as the \
                      canonical checkpoint capability. Mount it only into projector and ingest; \
                      query and compatibility never read it.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR_CONTROL__CHECKPOINT_STAGING_ROOT",
        toml_path: "projector_control.checkpoint_staging_root",
        used_by: &["zinder-projector"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Shared candidate root whose realpath must match \
                      ingest_control.checkpoint_staging_root. The projector sends only a SHA-256 \
                      root binding to canonical control, never a path.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__PATH",
        toml_path: "storage.path",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::ConditionalOn(
            "running zinder-query or zinder-compat-lightwalletd",
        ),
        sensitive: false,
        description: "Canonical RocksDB store directory. Defaults to `/var/lib/zinder/canonical` \
                      for ingest and Explorer. Ingest writes the primary store here. Explorer \
                      locates its materialized-view store beneath this directory. Query and \
                      lightwalletd open the canonical store as a secondary.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__SECONDARY_PATH",
        toml_path: "storage.secondary_path",
        used_by: &[
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Required,
        sensitive: false,
        description: "Process-unique RocksDB secondary metadata directory. Never share this path \
                      across reader processes.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__INITIAL_CATCHUP_TIMEOUT_MS",
        toml_path: "storage.initial_catchup_timeout_ms",
        used_by: &["zinder-query", "zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum startup RocksDB secondary catchup duration before a reader starts \
                      with the opened secondary and lets /readyz report replica lag. Defaults to \
                      30000.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__SECONDARY_CATCHUP_INTERVAL_MS",
        toml_path: "storage.secondary_catchup_interval_ms",
        used_by: &["zinder-query", "zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Cadence of RocksDB secondary catchup ticks in milliseconds. Defaults to \
                      1000. Must be greater than zero.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__SECONDARY_REPLICA_LAG_THRESHOLD_CHAIN_EPOCHS",
        toml_path: "storage.secondary_replica_lag_threshold_chain_epochs",
        used_by: &["zinder-query", "zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Chain epochs the secondary may trail the primary before `/readyz` reports \
                      replica lag. Defaults to 4.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__SERVING_PAIR_STALENESS_CEILING_MS",
        toml_path: "storage.serving_pair_staleness_ceiling_ms",
        used_by: &["zinder-query", "zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Longest a wallet-serving pair keeps admitting traffic, in milliseconds, \
                      after the last writer attestation. The reader refreshes at least every \
                      half ceiling, even if `storage.secondary_catchup_interval_ms` is longer, \
                      so a writer that answers attests it again before the ceiling passes. \
                      Traffic stops when the ceiling passes, even while a refresh is still \
                      waiting for the writer. Defaults to 300000. Must be greater than zero.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__CANONICAL_PATH",
        toml_path: "storage.canonical_path",
        used_by: &["zinder-projector"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Canonical primary RocksDB path the projector opens as a read-only \
                      secondary. Defaults to `/var/lib/zinder/canonical`.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__CANONICAL_SECONDARY_PATH",
        toml_path: "storage.canonical_secondary_path",
        used_by: &["zinder-projector"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Projector-local RocksDB secondary metadata directory for canonical \
                      reads. Defaults to `/var/lib/zinder/projector/canonical-secondary`; never \
                      share it with another process.",
    },
    EnvVarDoc {
        name: "ZINDER_WALLET__PATH",
        toml_path: "wallet.path",
        used_by: &[
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
        ],
        requirement: Requirement::ConditionalOn("running a wallet-serving reader"),
        sensitive: false,
        description: "Wallet-projection RocksDB primary path. The projector owns it as the \
                      primary writer and defaults to `/var/lib/zinder/wallet`; both serving \
                      runtimes open it as a read-only secondary and require an explicit path.",
    },
    EnvVarDoc {
        name: "ZINDER_WALLET__SECONDARY_PATH",
        toml_path: "wallet.secondary_path",
        used_by: &["zinder-query", "zinder-compat-lightwalletd"],
        requirement: Requirement::Required,
        sensitive: false,
        description: "Wallet-serving reader root for immutable wallet-secondary generations. \
                      Must be distinct from every primary and canonical-secondary path.",
    },
    EnvVarDoc {
        name: "ZINDER_WALLET__ROCKSDB__BLOCK_CACHE_BYTES",
        toml_path: "wallet.rocksdb.block_cache_bytes",
        used_by: &[
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Wallet-projection RocksDB block cache budget in bytes. Defaults to 268435456 \
                      for the writer and 67108864 for the wallet-serving reader. In zinder-query \
                      and zinder-compat-lightwalletd, one cache of this size serves every wallet \
                      secondary generation of the process.",
    },
    EnvVarDoc {
        name: "ZINDER_WALLET__ROCKSDB__MAX_WAL_BYTES",
        toml_path: "wallet.rocksdb.max_wal_bytes",
        used_by: &[
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Wallet-projection RocksDB live WAL ceiling in bytes. Defaults to 268435456 \
                      for the writer and 16777216 for the wallet-serving reader.",
    },
    EnvVarDoc {
        name: "ZINDER_WALLET__ROCKSDB__MAX_OPEN_FILES",
        toml_path: "wallet.rocksdb.max_open_files",
        used_by: &[
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Wallet-projection RocksDB open SST file cap. Defaults to 512 for the writer \
                      and 64 for the wallet-serving reader.",
    },
    EnvVarDoc {
        name: "ZINDER_WALLET__ROCKSDB__WRITE_BUFFER_BYTES",
        toml_path: "wallet.rocksdb.write_buffer_bytes",
        used_by: &[
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Wallet-projection RocksDB per-column-family write buffer size. Defaults to \
                      16777216 for the writer and 4194304 for the wallet-serving reader.",
    },
    EnvVarDoc {
        name: "ZINDER_WALLET__ROCKSDB__MAX_WRITE_BUFFER_COUNT",
        toml_path: "wallet.rocksdb.max_write_buffer_count",
        used_by: &[
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Wallet-projection RocksDB mutable plus immutable write buffer count. Defaults \
                      to 4 for the writer and 2 for the wallet-serving reader.",
    },
    EnvVarDoc {
        name: "ZINDER_WALLET__ROCKSDB__MAX_BACKGROUND_JOBS",
        toml_path: "wallet.rocksdb.max_background_jobs",
        used_by: &[
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Wallet-projection primary-writer RocksDB background job cap shared by flush \
                      and compaction work. Defaults to 2 and is not applied to secondary opens, \
                      including wallet-serving readers.",
    },
    EnvVarDoc {
        name: "ZINDER_WALLET__ROCKSDB__MEMTABLE_BUDGET_BYTES",
        toml_path: "wallet.rocksdb.memtable_budget_bytes",
        used_by: &[
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Wallet-projection RocksDB total memtable budget across column families. \
                      Defaults to 536870912 for the writer and 16777216 for a wallet-serving \
                      reader.",
    },
    EnvVarDoc {
        name: "ZINDER_WALLET__ROCKSDB__STATISTICS_LEVEL",
        toml_path: "wallet.rocksdb.statistics_level",
        used_by: &[
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Wallet-projection RocksDB statistics collection gate: `off`, `tickers`, or \
                      `full`. Defaults to `tickers`.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__CANONICAL__ROCKSDB__BLOCK_CACHE_BYTES",
        toml_path: "storage.canonical.rocksdb.block_cache_bytes",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Canonical-store RocksDB block cache budget in bytes. Defaults to 536870912 \
                      for writers. Readers default to one eighth of the detected cgroup memory \
                      limit, clamped between 134217728 and 536870912, and to 134217728 without \
                      a limit. In zinder-query and zinder-compat-lightwalletd, one cache of this \
                      size serves every canonical secondary generation of the process.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_WAL_BYTES",
        toml_path: "storage.canonical.rocksdb.max_wal_bytes",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Canonical-store RocksDB live WAL ceiling in bytes. Defaults to 268435456 \
                      for writers and 33554432 for readers.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES",
        toml_path: "storage.canonical.rocksdb.max_open_files",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Canonical-store RocksDB open SST file cap. Defaults to 512 for writers. \
                      Readers default to 128 to 256 based on the detected container memory limit, \
                      or 128 without a limit.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__CANONICAL__ROCKSDB__WRITE_BUFFER_BYTES",
        toml_path: "storage.canonical.rocksdb.write_buffer_bytes",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Canonical-store per-column-family RocksDB write buffer size. Defaults to \
                      16777216 for writers and 8388608 for readers.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_WRITE_BUFFER_COUNT",
        toml_path: "storage.canonical.rocksdb.max_write_buffer_count",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Canonical-store per-column-family mutable plus immutable RocksDB write \
                      buffer count. Defaults to 2.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_BACKGROUND_JOBS",
        toml_path: "storage.canonical.rocksdb.max_background_jobs",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Canonical-store primary-writer RocksDB background job cap shared by flush \
                      and compaction work. Defaults to 2 and is not applied to secondary opens.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__CANONICAL__ROCKSDB__MEMTABLE_BUDGET_BYTES",
        toml_path: "storage.canonical.rocksdb.memtable_budget_bytes",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Canonical-store total RocksDB memtable budget across column families. \
                      Defaults to 268435456 for writers and 16777216 for readers.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__CANONICAL__ROCKSDB__STATISTICS_LEVEL",
        toml_path: "storage.canonical.rocksdb.statistics_level",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Canonical-store RocksDB statistics collection gate: `off`, `tickers`, or \
                      `full`. Defaults to `tickers`.",
    },
    EnvVarDoc {
        name: "ZINDER_QUERY__LISTEN_ADDR",
        toml_path: "query.listen_addr",
        used_by: &["zinder-query"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Listen address for the native WalletQuery gRPC endpoint. Defaults to \
                      `127.0.0.1:9102`.",
    },
    EnvVarDoc {
        name: "ZINDER_QUERY__REORG_WINDOW_BLOCKS",
        toml_path: "query.reorg_window_blocks",
        used_by: &["zinder-query"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Exact canonical replacement-depth identity expected by native query. Must \
                      be greater than zero and match the canonical writer. Defaults to 1000.",
    },
    EnvVarDoc {
        name: "ZINDER_QUERY__PAIR_CONVERGENCE_ATTEMPTS",
        toml_path: "query.pair_convergence_attempts",
        used_by: &["zinder-query"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum bounded attempts to converge and admit native query's canonical \
                      and wallet secondary pair. Must be in 1..=64; defaults to 12.",
    },
    EnvVarDoc {
        name: "ZINDER_QUERY__TREE_STATE_REPLAY_WAIT_MS",
        toml_path: "query.tree_state_replay_wait_ms",
        used_by: &["zinder-query"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Milliseconds a tree-state rebuild waits for a rebuild slot before it is \
                      refused with `TREE_STATE_REPLAY_LIMIT_REACHED`. Must be in 0..=5000; 0 \
                      refuses at once; defaults to 2000.",
    },
    EnvVarDoc {
        name: "ZINDER_QUERY__TREE_STATE_REPLAY_WAITERS_PER_SLOT",
        toml_path: "query.tree_state_replay_waiters_per_slot",
        used_by: &["zinder-query"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Tree-state rebuilds that may wait for each rebuild slot; a rebuild that \
                      finds every waiting place taken is refused at once with \
                      `TREE_STATE_REPLAY_LIMIT_REACHED`. Must be in 1..=256; defaults to 64, \
                      enough for one slot to serve its waiters at a 30 ms rebuild within the \
                      default 2-second wait.",
    },
    EnvVarDoc {
        name: "ZINDER_QUERY__TRUST_PROXY_CLIENT_ADDRESS",
        toml_path: "query.trust_proxy_client_address",
        used_by: &["zinder-query"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Counts each WalletQuery request against the client connection that a \
                      reverse proxy names in the `x-zinder-client-address` header, instead of \
                      the proxy's own connection, when the receive limit picks which client to \
                      refuse. Set it only when clients reach the listener solely through a \
                      proxy that replaces the header. Defaults to false.",
    },
    EnvVarDoc {
        name: "ZINDER_COMPAT__LISTEN_ADDR",
        toml_path: "compat.listen_addr",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Listen address for the lightwalletd-compatible gRPC endpoint. Defaults to \
                      `127.0.0.1:9067`.",
    },
    EnvVarDoc {
        name: "ZINDER_COMPAT__BROWSER_ORIGINS",
        toml_path: "compat.browser_origins",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Enables gRPC-Web for the listed browser origins. Use comma-separated HTTP \
                      or HTTPS origins in the environment, or an array in TOML. An empty list \
                      disables browser access (the default). Use `*` alone to allow every origin.",
    },
    EnvVarDoc {
        name: "ZINDER_COMPAT__SERVING",
        toml_path: "compat.serving",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Runtime serving selection: exactly `wallet` or `compact-blocks`; defaults \
                      to `wallet`. `compact-blocks` rejects wallet and node settings, and no \
                      canonical or legacy aliases are accepted.",
    },
    EnvVarDoc {
        name: "ZINDER_COMPAT__REORG_WINDOW_BLOCKS",
        toml_path: "compat.reorg_window_blocks",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Exact canonical replacement-depth identity expected by compatibility. Must \
                      be greater than zero and match the canonical writer. Defaults to 1000.",
    },
    EnvVarDoc {
        name: "ZINDER_COMPAT__PAIR_CONVERGENCE_ATTEMPTS",
        toml_path: "compat.pair_convergence_attempts",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum bounded attempts to converge and admit compatibility's \
                      wallet-serving pair or compact-serving canonical secondary. Must be in \
                      1..=64; defaults to 12.",
    },
    EnvVarDoc {
        name: "ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS",
        toml_path: "compat.max_concurrent_block_range_streams",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum concurrent `GetBlockRange` and `GetBlockRangeNullifiers` \
                      streams the compatibility server serves. A valid request past the limit \
                      waits up to 10 seconds for a slot, then fails with `UNAVAILABLE`. Must \
                      be in 1..=65536; defaults to 100.",
    },
    EnvVarDoc {
        name: "ZINDER_COMPAT__TREE_STATE_REPLAY_WAIT_MS",
        toml_path: "compat.tree_state_replay_wait_ms",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Milliseconds a lightwalletd `GetTreeState` rebuild waits for a rebuild slot \
                      before it is refused with `UNAVAILABLE`. Wallet serving only; \
                      compact-blocks serving rejects it. Must be in 0..=5000; 0 refuses at \
                      once; defaults to 2000.",
    },
    EnvVarDoc {
        name: "ZINDER_COMPAT__TREE_STATE_REPLAY_WAITERS_PER_SLOT",
        toml_path: "compat.tree_state_replay_waiters_per_slot",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "lightwalletd `GetTreeState` rebuilds that may wait for each rebuild slot; \
                      a rebuild that finds every waiting place taken fails at once with \
                      `UNAVAILABLE`. Wallet serving only; compact-blocks serving rejects it. \
                      Must be in 1..=256; defaults to 64.",
    },
    EnvVarDoc {
        name: "ZINDER_COMPAT__STALE_TIP_LIMIT_MS",
        toml_path: "compat.stale_tip_limit_ms",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Milliseconds the served tip may age, counted from its block time or \
                      from when the process first served it, whichever is earlier, while the \
                      writer cannot vouch for it because its node is unreachable, behind the \
                      network, or ahead of the writer, \
                      before lightwalletd \
                      `GetLatestBlock` and `GetLightdInfo` fail with `UNAVAILABLE`, so wallets \
                      stop building transactions on a stale tip. A tip the node still reports \
                      is never refused. 0 disables it; otherwise at least 300000. Defaults to \
                      1500000 (25 minutes).",
    },
    EnvVarDoc {
        name: "ZINDER_COMPAT__PING_VERY_INSECURE",
        toml_path: "compat.ping_very_insecure",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Answers the lightwalletd `Ping` load-testing RPC, as \
                      `lightwalletd --ping-very-insecure` does. Each call holds a server task \
                      for as long as the client asks, so enable it only on a server you are \
                      load testing. Defaults to false, which refuses `Ping` with \
                      `FAILED_PRECONDITION`.",
    },
    EnvVarDoc {
        name: "ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS",
        toml_path: "compat.trust_proxy_client_address",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Counts each lightwalletd-compatible request against the client connection that a \
                      reverse proxy names in the `x-zinder-client-address` header, instead of \
                      the proxy's own connection, when the receive limit picks which client to \
                      refuse. Set it only when clients reach the listener solely through a \
                      proxy that replaces the header. Defaults to false.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__BLOCK_CACHE_BYTES",
        toml_path: "storage.materialized_views.rocksdb.block_cache_bytes",
        used_by: &["zinder-ingest", "zinder-explorer"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Materialized-view store RocksDB block cache budget in bytes. Defaults to \
                      268435456 for writers and 67108864 for readers.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_WAL_BYTES",
        toml_path: "storage.materialized_views.rocksdb.max_wal_bytes",
        used_by: &["zinder-ingest", "zinder-explorer"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Materialized-view store RocksDB live WAL ceiling in bytes. Defaults to \
                      268435456 for writers and 16777216 for readers.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_OPEN_FILES",
        toml_path: "storage.materialized_views.rocksdb.max_open_files",
        used_by: &["zinder-ingest", "zinder-explorer"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Materialized-view store RocksDB open SST file cap. Defaults to 512 for \
                      writers and 64 for readers.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__WRITE_BUFFER_BYTES",
        toml_path: "storage.materialized_views.rocksdb.write_buffer_bytes",
        used_by: &["zinder-ingest", "zinder-explorer"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Materialized-view store per-column-family RocksDB write buffer size. \
                      Defaults to 16777216 for writers and 4194304 for readers.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_WRITE_BUFFER_COUNT",
        toml_path: "storage.materialized_views.rocksdb.max_write_buffer_count",
        used_by: &["zinder-ingest", "zinder-explorer"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Materialized-view store per-column-family mutable plus immutable RocksDB write \
                      buffer count. Defaults to 4 for writers and 2 for readers.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_BACKGROUND_JOBS",
        toml_path: "storage.materialized_views.rocksdb.max_background_jobs",
        used_by: &["zinder-ingest", "zinder-explorer"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Materialized-view store primary-writer RocksDB background job cap shared by \
                      flush and compaction work. Defaults to 2 and is not applied to secondary \
                      opens, including `zinder-explorer`.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MEMTABLE_BUDGET_BYTES",
        toml_path: "storage.materialized_views.rocksdb.memtable_budget_bytes",
        used_by: &["zinder-ingest", "zinder-explorer"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Materialized-view store total RocksDB memtable budget across column families. \
                      Defaults to 536870912 for writers and 16777216 for readers.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__STATISTICS_LEVEL",
        toml_path: "storage.materialized_views.rocksdb.statistics_level",
        used_by: &["zinder-ingest", "zinder-explorer"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Materialized-view store RocksDB statistics collection gate: `off`, `tickers`, or `full`. \
                      Defaults to `tickers`.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__SOURCE",
        toml_path: "ingest.source",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Required,
        sensitive: false,
        description: "Source-adapter selector. Lives on `[ingest]` (not `[node]`) because the \
                      choice is a writer-private implementation decision: `[node]` describes the \
                      upstream node itself, `[ingest].source` describes which adapter ingest \
                      uses to talk to it. See [ADR-0006](../adrs/0006-node-source-boundary.md).",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__EXPLORER_VIEWS",
        toml_path: "ingest.explorer_views",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Boolean request for the complete bundled Explorer materialized-view \
                      workload. Defaults to `false` for every coverage. `true` requires \
                      genesis-complete canonical history or the authenticated \
                      `checkpoint_height = 0`; checkpoint heights above zero are rejected before \
                      storage mutation. The request appears in `--print-config`; admission \
                      evidence appears only after the Explorer store opens successfully.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__EXPLORER_VIEWS_MIN_FREE_BYTES",
        toml_path: "ingest.explorer_views_min_free_bytes",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Available bytes on the volume holding `storage.path` below which Explorer \
                      view replay, the transparent-address ranking build, and historical view \
                      backfills pause, so the views cannot fill the volume canonical ingest and \
                      wallet serving need. Replay resumes once at least a tenth more than this \
                      is available. Defaults to 10737418240 (10 GiB); `0` disables the guard.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__RAW_BLOB_POLICY",
        toml_path: "storage.raw_blob_policy",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Immutable raw-blob retention contract: `none`, `transactions`, or `all`. \
                      Defaults to `none` for explicit coverage so canonical indexing does not write \
                      raw block or transaction blobs unless a deployment explicitly needs raw export. \
                      Wallet-serving coverage defaults to `transactions` and rejects `none`, because \
                      native and lightwalletd-compatible transaction and transparent-history methods \
                      require retained bytes. \
                      Projector, query, and compatibility readers use the same value as an exact \
                      admission expectation; Zallet full-block serving requires `all`. The first \
                      canonical commit fixes historical coverage; changing a non-empty store requires \
                      a rebuild and blue-green cutover.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__REORG_WINDOW_BLOCKS",
        toml_path: "ingest.reorg_window_blocks",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Chain-truth invariant: how deep the live reorg window extends. Bounds \
                      settlement, classifier default, and replacement traversal. Must be \
                      greater than zero. Defaults to 1000, Zebra's maximum finalization depth. A \
                      larger value raises an existing store's window in place; a smaller one is \
                      refused.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__MEMPOOL__MAX_TRANSACTION_COUNT",
        toml_path: "ingest.mempool.max_transaction_count",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum number of transactions admitted into one coherent live mempool. \
                      Exceeding the bound withdraws the serving generation and retries source \
                      hydration. Must be greater than zero. Defaults to 8000.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__MEMPOOL__MAX_TOTAL_RAW_TRANSACTION_BYTES",
        toml_path: "ingest.mempool.max_total_raw_transaction_bytes",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum cumulative raw transaction bytes admitted into one coherent live \
                      mempool. Exceeding the bound withdraws the serving generation and retries \
                      source hydration. Must be greater than zero. Defaults to 80000000.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__MEMPOOL__RECONCILIATION_BATCH_TARGET_RAW_TRANSACTION_BYTES",
        toml_path: "ingest.mempool.reconciliation_batch_target_raw_transaction_bytes",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Target raw transaction bytes for one durable mempool reconciliation \
                      write. A single protocol-valid transaction above the target is written \
                      alone so reconciliation can make progress. Must be greater than zero. \
                      Defaults to 16000000.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR__REORG_WINDOW_BLOCKS",
        toml_path: "projector.reorg_window_blocks",
        used_by: &["zinder-projector"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Wallet undo suffix depth and expected canonical replacement policy. Must \
                      match the canonical writer. Defaults to 1000.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR__BUILD_OWNER_HEX",
        toml_path: "projector.build_owner_hex",
        used_by: &["zinder-projector"],
        requirement: Requirement::Required,
        sensitive: false,
        description: "Stable 16-byte wallet-build lease owner encoded as exactly 32 hexadecimal \
                      characters. A restart with the same value reclaims its own construction \
                      leases immediately. Use a distinct value for each concurrently provisioned \
                      lane.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR__LEASE_DURATION_SECONDS",
        toml_path: "projector.lease_duration_seconds",
        used_by: &["zinder-projector"],
        requirement: Requirement::Required,
        sensitive: false,
        description: "Wallet-build and canonical-retention lease duration in seconds. Must be at \
                      least 14400 so a durable construction phase cannot outlive its lease.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR__FENCE_CONVERGENCE_TIMEOUT_SECONDS",
        toml_path: "projector.fence_convergence_timeout_seconds",
        used_by: &["zinder-projector"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Deadline for each attempt of the projector's canonical secondary to \
                      reach the writer's fence. A projector with a wallet open, including one \
                      that resumes after a restart, stops waiting once its secondary holds a \
                      block the wallet has not applied, even while the writer is ahead; a \
                      fresh wallet build waits for the writer to attest the secondary's exact \
                      fence. An attempt that passes the deadline logs \
                      `canonical_secondary_behind_writer`, reports `replica_lagging`, or \
                      `writer_status_unavailable` when the writer did not answer, until the \
                      secondary gives the wallet a block to apply, and starts another attempt; \
                      the projector does not exit. A same-sequence mismatch or a secondary \
                      ahead of the writer fails immediately. Readiness while the wallet trails \
                      is described in [Readiness](readiness.md#projector). \
                      Must be greater than zero. Defaults to 30.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR__BUILD__MAX_OUTPOINT_SORT_MEMORY_BYTES",
        toml_path: "projector.build.max_outpoint_sort_memory_bytes",
        used_by: &["zinder-projector"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Memory ceiling for the wallet builder's outpoint sorter. Defaults to \
                      4294967296.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR__BUILD__MAX_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER",
        toml_path: "projector.build.max_secondary_sort_memory_bytes_per_sorter",
        used_by: &["zinder-projector"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Memory ceiling for each wallet secondary-index sorter. Defaults to \
                      1073741824.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR__BUILD__MAX_TEMPORARY_FILE_BYTES_PER_SORTER",
        toml_path: "projector.build.max_temporary_file_bytes_per_sorter",
        used_by: &["zinder-projector"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Temporary spill-file ceiling for each wallet builder sorter. Defaults to \
                      206158430208.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR__BUILD__SST_TARGET_LOGICAL_BYTES",
        toml_path: "projector.build.sst_target_logical_bytes",
        used_by: &["zinder-projector"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Target logical payload per externally built wallet SST file. Defaults to \
                      134217728.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR__BUILD__MAX_ACCOUNTED_REORG_UNDO_BYTES",
        toml_path: "projector.build.max_accounted_reorg_undo_bytes",
        used_by: &["zinder-projector"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum logical wallet undo bytes admitted during fixed-tip construction. \
                      Defaults to 536870912.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR__FOLLOW__MAX_TRANSITION_LOGICAL_BYTES",
        toml_path: "projector.follow.max_transition_logical_bytes",
        used_by: &["zinder-projector"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum logical bytes one atomic wallet following transition may plan. \
                      Each distinct wallet row the transition touches counts twice, as its key \
                      and final value in the plan and again in the write batch, and the \
                      control record counts once. Must be greater than zero and at most \
                      1073741824. Defaults to 536870912. A transition that needs more leaves \
                      the wallet unchanged and parks the projector, not ready, with \
                      `wallet_transition_limit_exceeded` until it restarts with a larger \
                      value; see [Wallet transition exceeds its logical-byte \
                      limit](../runbooks/deploying-single-volume.md#wallet-transition-exceeds-its-logical-byte-limit).",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__PHASE_CLASSIFICATION__CATCHUP_THRESHOLD_BLOCKS",
        toml_path: "ingest.phase_classification.catchup_threshold_blocks",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Gap (in blocks) above which `zinder-ingest probe` reports `BulkCatchup` \
                      instead of `FollowingTip`. Defaults to `ingest.reorg_window_blocks`. See \
                      [ADR-0009](../adrs/0009-ingest-lifecycle-and-construction.md).",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__CONSTRUCTION__SOURCE_SEGMENT_MAX_BLOCKS",
        toml_path: "ingest.construction.source_segment_max_blocks",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Diagnostic override for the hard ceiling on connected blocks requested \
                      from the source in one segment. The resource-resolved default is 64. \
                      Values above 1000 are rejected at startup, because each block is one \
                      JSON-RPC batch entry and Zakura refuses batches above 1,000 entries.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__CONSTRUCTION__SOURCE_SEGMENT_TARGET_RESPONSE_BYTES",
        toml_path: "ingest.construction.source_segment_target_response_bytes",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Diagnostic override for adaptive response sizing. The default is \
                      `min(node.max_response_bytes, 33554432)`; with `[node_fallbacks.N]` \
                      endpoints, the smallest `max_response_bytes` in the list replaces \
                      `node.max_response_bytes`.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__CONSTRUCTION__SOURCE_FETCH_MAX_IN_FLIGHT_REQUESTS",
        toml_path: "ingest.construction.source_fetch_max_in_flight_requests",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum concurrent source segment requests. Defaults to 12.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__CONSTRUCTION__SOURCE_FETCH_MAX_IN_FLIGHT_BYTES",
        toml_path: "ingest.construction.source_fetch_max_in_flight_bytes",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Diagnostic override for predicted active source responses plus measured \
                      completed reassembly. The default is \
                      `max(node.max_response_bytes, clamp(container_memory / 64, 134217728, \
                      402653184))`; with `[node_fallbacks.N]` endpoints, the largest \
                      `max_response_bytes` in the list replaces `node.max_response_bytes`, \
                      and an override must be at least that large.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__CONSTRUCTION__BLOCK_PREPARE_CONCURRENCY",
        toml_path: "ingest.construction.block_prepare_concurrency",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Diagnostic override for parallel canonical block-prepare slots. The \
                      default is `min(available_parallelism(), 16)`.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__CONSTRUCTION__BLOCK_PREPARE_MEMORY_WATERMARK_BYTES",
        toml_path: "ingest.construction.block_prepare_memory_watermark_bytes",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Diagnostic override for the prepare and resident-handoff admission \
                      watermark. The default is \
                      `clamp(container_memory / 64, 134217728, 536870912)`.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__FOLLOW__POLL_INTERVAL_MS",
        toml_path: "ingest.follow.poll_interval_ms",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Canonical follower poll cadence in milliseconds. Must be greater than zero. \
                      Defaults to 1000.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__FOLLOW__LAG_THRESHOLD_BLOCKS",
        toml_path: "ingest.follow.lag_threshold_blocks",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Block lag at which the canonical follower reports `cause=syncing`. \
                      Defaults to 1.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__FOLLOW__MAX_REPLACEMENT_BYTES",
        toml_path: "ingest.follow.max_replacement_bytes",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum estimated bytes one canonical replacement may hold before its \
                      atomic commit: fetched source blocks not yet prepared plus prepared \
                      blocks. A deeper or heavier replacement fails with \
                      `ReplacementBytesExceeded` before any write; raise the limit to follow \
                      it. Defaults to 2147483648.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__TREE_STATE_DENSIFICATION__ENABLED",
        toml_path: "ingest.tree_state_densification.enabled",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Whether ingest stores dense historical tree-state checkpoints: during fresh \
                      construction when `selection = \"every-block\"`, and through the background \
                      pass for existing sparse stores or `commitment-budget` selections. The \
                      background pass runs only with `ingest_control.listen_addr` set; fresh \
                      construction applies `every-block` whenever enabled. Setting `false` keeps \
                      fresh construction on the 100-block cadence and disables the background \
                      pass. Defaults to `true`.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__TREE_STATE_DENSIFICATION__SELECTION",
        toml_path: "ingest.tree_state_densification.selection",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Which historical blocks receive a stored tree-state checkpoint: \
                      `every-block` stores one for every loaded block during fresh construction \
                      (so every constructed height through the build tip is a stored read at \
                      READY) and fills existing sparse stores in the background, while \
                      `commitment-budget` keeps fresh construction on the 100-block cadence and \
                      stores one in the background each time `commitment_budget` note commitments \
                      have accumulated. Defaults to `every-block`, or to `commitment-budget` when \
                      only `commitment_budget` is set. An operator who wants sparse construction \
                      or `commitment-budget` selects `enabled = false` or `commitment-budget` \
                      before the first sync or first upgraded start; switching later keeps every \
                      stored row, because a dense stride already satisfies the looser budget and \
                      the pass only inserts missing rows, and only changes the work still \
                      outstanding. Under `every-block`, writing checkpoints during construction \
                      moves frontier cross-checking before READY publication and avoids a second \
                      commitment-replay pass afterward; full Mainnet construction cost and disk \
                      growth (estimated at about 2.4 GB of rows before compression, or 4 to 5 \
                      CPU-hours to backfill an existing sparse store) await heavy-workload \
                      qualification.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__TREE_STATE_DENSIFICATION__COMMITMENT_BUDGET",
        toml_path: "ingest.tree_state_densification.commitment_budget",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Note commitments, over every pool, between two stored tree-state \
                      checkpoints under the `commitment-budget` selection, which setting it \
                      selects when `selection` is unset. Must be greater than zero and is \
                      refused with `selection = \"every-block\"`. Defaults to 1024.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__TREE_STATE_DENSIFICATION__WORKERS",
        toml_path: "ingest.tree_state_densification.workers",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Settled 100-block strides densified at once, each on one CPU. Must be \
                      greater than zero. Defaults to 2.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__TREE_STATE_DENSIFICATION__MAX_BLOCKS_PER_SECOND",
        toml_path: "ingest.tree_state_densification.max_blocks_per_second",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Average ceiling on stored blocks the tree-state densification pass \
                      replays per second, with a scan charged one block for every two reads it makes, \
                      counting each checkpoint row it visits and each compact block it reads as one read. \
                      Each block costs about two reads and adds at most one row, so this \
                      bounds the pass's reads and writes. Must be greater than zero. \
                      Defaults to 500.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__TREE_STATE_VERIFICATION__ENABLED",
        toml_path: "ingest.tree_state_verification.enabled",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Whether ingest verifies its stored tree-state checkpoints in the \
                      background, in repeated cycles over every settled row: each row must \
                      derive the roots stored in it, carry a frontier for exactly the \
                      pools active at its height, and carry the block time of the \
                      canonical block at that height. A row that fails parks ingest \
                      readiness with `canonical_store_damaged` and makes the store's \
                      wallet-serving readers refuse traffic, until the store is rebuilt. \
                      With the pass disabled, damage it recorded before stays reported. \
                      Defaults to `true`.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__TREE_STATE_VERIFICATION__MAX_ROWS_PER_SECOND",
        toml_path: "ingest.tree_state_verification.max_rows_per_second",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Average ceiling on stored tree-state checkpoint rows the verification \
                      pass checks per second, which sets both its cost and how long one \
                      cycle over settled history takes. A row with three pools costs about \
                      4.3 ms of CPU, so the default holds the pass near a tenth of one CPU \
                      and verifies about 2.2 million rows a day. Must be greater than zero. \
                      Defaults to 25.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__RUN_OVERRIDES__TARGET_HEIGHT",
        toml_path: "ingest.run_overrides.target_height",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "One-shot stop-at modifier; the loop exits 0 after committing this \
                      height.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__RUN_OVERRIDES__CHECKPOINT_HEIGHT",
        toml_path: "ingest.run_overrides.checkpoint_height",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Construct an empty store from an upstream-supplied checkpoint at this \
                      height.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__RUN_OVERRIDES__COVERAGE",
        toml_path: "ingest.run_overrides.coverage",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Ingest coverage mode: `\"explicit\"` or `\"wallet-serving\"`. Defaults to \
                      `\"explicit\"`.",
    },
    EnvVarDoc {
        name: "ZINDER_RETENTION__CHAIN_EVENT_RETENTION_HOURS",
        toml_path: "retention.chain_event_retention_hours",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Chain-event retention window in hours, enforced by `zinder-ingest`. \
                      Defaults to 168 (7 days). `0` disables eviction.",
    },
    EnvVarDoc {
        name: "ZINDER_RETENTION__CHAIN_EVENT_RETENTION_CHECK_INTERVAL_MS",
        toml_path: "retention.chain_event_retention_check_interval_ms",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Chain-event retention sweep cadence in milliseconds. Must be greater than \
                      zero. Defaults to 60000 (one minute).",
    },
    EnvVarDoc {
        name: "ZINDER_RETENTION__CURSOR_AT_RISK_WARNING_HOURS",
        toml_path: "retention.cursor_at_risk_warning_hours",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Cursor-at-risk warning lead time in hours. Must be \u{2264} \
                      `retention.chain_event_retention_hours`. Defaults to 24.",
    },
    EnvVarDoc {
        name: "ZINDER_RETENTION__MEMPOOL_MINED_RETENTION_MINUTES",
        toml_path: "retention.mempool_mined_retention_minutes",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Mined-mempool retention window in minutes, enforced by `zinder-ingest`. \
                      Defaults to 60. `0` disables retention.",
    },
    EnvVarDoc {
        name: "ZINDER_RETENTION__MEMPOOL_INVALIDATED_RETENTION_HOURS",
        toml_path: "retention.mempool_invalidated_retention_hours",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Invalidated-mempool retention window in hours, enforced by `zinder-ingest`. \
                      Defaults to 24. `0` disables retention.",
    },
    EnvVarDoc {
        name: "ZINDER_RETENTION__MEMPOOL_EVENT_RETENTION_CHECK_INTERVAL_MS",
        toml_path: "retention.mempool_event_retention_check_interval_ms",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Mempool-event retention sweep cadence in milliseconds. Must be greater \
                      than zero. Defaults to 30000.",
    },
    EnvVarDoc {
        name: "ZINDER_RETENTION__MEMPOOL_EVENT_RETENTION_MAX_EVENTS_PER_STEP",
        toml_path: "retention.mempool_event_retention_max_events_per_step",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum event rows examined by one bounded mempool-retention step. Must be \
                      greater than zero. Defaults to 1024.",
    },
    EnvVarDoc {
        name: "ZINDER_RETENTION__MEMPOOL_EVENT_RETENTION_MAX_ENCODED_BYTES_PER_STEP",
        toml_path: "retention.mempool_event_retention_max_encoded_bytes_per_step",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Target maximum encoded event bytes examined by one bounded \
                      mempool-retention step. The first row may exceed the target to guarantee \
                      progress. Must be greater than zero. Defaults to 16000000.",
    },
    EnvVarDoc {
        name: "ZINDER_EXPLORER__BEARER_TOKEN_PATH",
        toml_path: "explorer.bearer_token_path",
        used_by: &["zinder-explorer"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Path to the shared-secret bearer token the ExplorerQuery endpoint enforces \
                      on cross-service explorer-plane reads (ADR-0011).",
    },
    EnvVarDoc {
        name: "ZINDER_EXPLORER__LISTEN_ADDR",
        toml_path: "explorer.listen_addr",
        used_by: &["zinder-explorer"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Listen address for the ExplorerQuery gRPC endpoint. Defaults to 127.0.0.1:9068.",
    },
    EnvVarDoc {
        name: "ZINDER_EXPLORER__TRUST_PROXY_CLIENT_ADDRESS",
        toml_path: "explorer.trust_proxy_client_address",
        used_by: &["zinder-explorer"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Counts each ExplorerQuery request against the client connection that a \
                      reverse proxy names in the `x-zinder-client-address` header, instead of \
                      the proxy's own connection, when the receive limit picks which client to \
                      refuse. Set it only when clients reach the listener solely through a \
                      proxy that replaces the header. Defaults to false.",
    },
    EnvVarDoc {
        name: "ZINDER_EXPLORER__WALLET_QUERY_ENDPOINT",
        toml_path: "explorer.wallet_query_endpoint",
        used_by: &["zinder-explorer"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "WalletQuery gRPC endpoint backing the explorer's wallet-composed reads \
                      (transaction detail, block views, search, mempool activity). Empty/unset \
                      disables the explorer capabilities that compose canonical wallet reads.",
    },
    EnvVarDoc {
        name: "ZINDER_CIPHERSCAN__LISTEN_ADDR",
        toml_path: "cipherscan.listen_addr",
        used_by: &["zinder-compat-cipherscan"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Listen address for the Cipherscan-compatible REST endpoint. Defaults to \
                      127.0.0.1:9070. A public or unspecified address needs \
                      `ZINDER_SECURITY__ALLOW_PUBLIC_BIND=true`.",
    },
    EnvVarDoc {
        name: "ZINDER_CIPHERSCAN__EXPLORER_QUERY_ENDPOINT",
        toml_path: "cipherscan.explorer_query_endpoint",
        used_by: &["zinder-compat-cipherscan"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "ExplorerQuery gRPC endpoint the adapter reads explorer views from. \
                      Defaults to http://127.0.0.1:9068.",
    },
    EnvVarDoc {
        name: "ZINDER_CIPHERSCAN__WALLET_QUERY_ENDPOINT",
        toml_path: "cipherscan.wallet_query_endpoint",
        used_by: &["zinder-compat-cipherscan"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "WalletQuery gRPC endpoint the adapter reads chain and transaction data \
                      from. Defaults to http://127.0.0.1:9102.",
    },
    EnvVarDoc {
        name: "ZINDER_CIPHERSCAN__CURRENT_PRICE_ENDPOINT",
        toml_path: "cipherscan.current_price_endpoint",
        used_by: &["zinder-compat-cipherscan"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "HTTP or HTTPS URL of the external market-price provider queried for the \
                      current ZEC price. Defaults to the CoinGecko simple-price URL.",
    },
    EnvVarDoc {
        name: "ZINDER_CIPHERSCAN__HISTORICAL_PRICE_ENDPOINT_TEMPLATE",
        toml_path: "cipherscan.historical_price_endpoint_template",
        used_by: &["zinder-compat-cipherscan"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "URL template for historical market-price lookups; `{date}` is replaced \
                      with the requested date. Defaults to the CoinGecko history URL.",
    },
    EnvVarDoc {
        name: "ZINDER_CIPHERSCAN__BEARER_TOKEN_PATH",
        toml_path: "cipherscan.bearer_token_path",
        used_by: &["zinder-compat-cipherscan"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Path to the shared-secret bearer token the adapter presents to both the \
                      ExplorerQuery and WalletQuery endpoints (ADR-0011). Unset sends no token.",
    },
];

/// Renders [`ENVIRONMENT_VARIABLES`] as a GitHub-flavored Markdown table.
///
/// The rendered block is the canonical content of the
/// `env-var-table:public-interfaces` doc block; the doc-mirror integration
/// test fails when the doc and renderer diverge so adding a field to one
/// without the other is caught at build time.
#[must_use]
pub fn render_environment_variable_table() -> String {
    let mut output = String::new();
    output.push_str("| Variable | Used by | Requirement | TOML field | Description |\n");
    output.push_str("| -------- | ------- | ----------- | ---------- | ----------- |\n");
    for env_var in ENVIRONMENT_VARIABLES {
        let used_by = env_var.used_by.join(", ");
        let toml_field = format!("`{}`", env_var.toml_path);
        let sensitivity_marker = if env_var.sensitive {
            " (sensitive; redacted)"
        } else {
            ""
        };
        let _ = writeln!(
            output,
            "| `{}` | {} | {} | {} | {}{} |",
            env_var.name,
            used_by,
            env_var.requirement.label(),
            toml_field,
            env_var.description,
            sensitivity_marker,
        );
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendered_table_includes_required_network_row() {
        let rendered = render_environment_variable_table();
        assert!(rendered.contains("| `ZINDER_NETWORK__NAME` |"));
        assert!(rendered.contains("Required"));
        assert!(rendered.contains("zcash-mainnet"));
    }

    #[test]
    fn sensitive_variables_carry_redaction_marker_in_table() {
        let rendered = render_environment_variable_table();
        assert!(
            rendered
                .lines()
                .any(|line| line.starts_with("| `ZINDER_NODE__AUTH__PASSWORD`")
                    && line.contains("(sensitive; redacted)")),
            "sensitive rows must surface the redaction marker so operators do not \
             expect to see plaintext in --print-config:\n{rendered}",
        );
    }

    #[test]
    fn conditional_requirement_renders_when_clause() {
        let rendered = render_environment_variable_table();
        assert!(
            rendered
                .lines()
                .any(|line| line.starts_with("| `ZINDER_NODE__AUTH__USERNAME`")
                    && line.contains("When `ZINDER_NODE__AUTH__METHOD=basic`")),
            "conditional rows must render their precondition:\n{rendered}",
        );
    }

    #[test]
    fn background_job_variables_mark_secondary_inapplicability() {
        let background_job_variables = ENVIRONMENT_VARIABLES
            .iter()
            .filter(|env_var| env_var.name.ends_with("__ROCKSDB__MAX_BACKGROUND_JOBS"))
            .collect::<Vec<_>>();

        assert_eq!(background_job_variables.len(), 3);
        for env_var in background_job_variables {
            assert!(env_var.description.contains("primary-writer"));
            assert!(
                env_var
                    .description
                    .contains("not applied to secondary opens")
            );
        }
    }

    #[test]
    fn wallet_serving_environment_contract_keeps_both_protocol_runtimes() {
        let shared_names = [
            "ZINDER_NETWORK__NAME",
            "ZINDER_NODE__JSON_RPC_ADDR",
            "ZINDER_NODE__AUTH__METHOD",
            "ZINDER_NODE__AUTH__USERNAME",
            "ZINDER_NODE__AUTH__PASSWORD",
            "ZINDER_NODE__AUTH__PATH",
            "ZINDER_NODE__AUTH__COOKIE",
            "ZINDER_NODE__REQUEST_TIMEOUT_SECS",
            "ZINDER_NODE__MAX_RESPONSE_BYTES",
            "ZINDER_NODE__BROADCAST_TIMEOUT_SECS",
            "ZINDER_OPS__LISTEN_ADDR",
            "ZINDER_SECURITY__ALLOW_PUBLIC_BIND",
            "ZINDER_INGEST_CONTROL__ADDR",
            "ZINDER_INGEST_CONTROL__BEARER_TOKEN_PATH",
            "ZINDER_STORAGE__PATH",
            "ZINDER_STORAGE__SECONDARY_PATH",
            "ZINDER_STORAGE__INITIAL_CATCHUP_TIMEOUT_MS",
            "ZINDER_WALLET__PATH",
            "ZINDER_WALLET__SECONDARY_PATH",
            "ZINDER_WALLET__ROCKSDB__BLOCK_CACHE_BYTES",
            "ZINDER_WALLET__ROCKSDB__MAX_WAL_BYTES",
            "ZINDER_WALLET__ROCKSDB__MAX_OPEN_FILES",
            "ZINDER_WALLET__ROCKSDB__WRITE_BUFFER_BYTES",
            "ZINDER_WALLET__ROCKSDB__MAX_WRITE_BUFFER_COUNT",
            "ZINDER_WALLET__ROCKSDB__MAX_BACKGROUND_JOBS",
            "ZINDER_WALLET__ROCKSDB__MEMTABLE_BUDGET_BYTES",
            "ZINDER_WALLET__ROCKSDB__STATISTICS_LEVEL",
            "ZINDER_STORAGE__CANONICAL__ROCKSDB__BLOCK_CACHE_BYTES",
            "ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_WAL_BYTES",
            "ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES",
            "ZINDER_STORAGE__CANONICAL__ROCKSDB__WRITE_BUFFER_BYTES",
            "ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_WRITE_BUFFER_COUNT",
            "ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_BACKGROUND_JOBS",
            "ZINDER_STORAGE__CANONICAL__ROCKSDB__MEMTABLE_BUDGET_BYTES",
            "ZINDER_STORAGE__CANONICAL__ROCKSDB__STATISTICS_LEVEL",
        ];
        for name in shared_names {
            assert!(ENVIRONMENT_VARIABLES.iter().any(|env_var| {
                env_var.name == name
                    && env_var.used_by.contains(&"zinder-query")
                    && env_var.used_by.contains(&"zinder-compat-lightwalletd")
            }));
        }
    }

    #[test]
    fn wallet_serving_protocols_document_their_own_settings() {
        let query_specific = ENVIRONMENT_VARIABLES
            .iter()
            .filter(|env_var| env_var.name.starts_with("ZINDER_QUERY__"))
            .map(|env_var| env_var.name)
            .collect::<Vec<_>>();
        assert_eq!(
            query_specific,
            [
                "ZINDER_QUERY__LISTEN_ADDR",
                "ZINDER_QUERY__REORG_WINDOW_BLOCKS",
                "ZINDER_QUERY__PAIR_CONVERGENCE_ATTEMPTS",
                "ZINDER_QUERY__TREE_STATE_REPLAY_WAIT_MS",
                "ZINDER_QUERY__TREE_STATE_REPLAY_WAITERS_PER_SLOT",
                "ZINDER_QUERY__TRUST_PROXY_CLIENT_ADDRESS",
            ]
        );
        let compat_specific = ENVIRONMENT_VARIABLES
            .iter()
            .filter(|env_var| env_var.name.starts_with("ZINDER_COMPAT__"))
            .map(|env_var| env_var.name)
            .collect::<Vec<_>>();
        assert_eq!(
            compat_specific,
            [
                "ZINDER_COMPAT__LISTEN_ADDR",
                "ZINDER_COMPAT__BROWSER_ORIGINS",
                "ZINDER_COMPAT__SERVING",
                "ZINDER_COMPAT__REORG_WINDOW_BLOCKS",
                "ZINDER_COMPAT__PAIR_CONVERGENCE_ATTEMPTS",
                "ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS",
                "ZINDER_COMPAT__TREE_STATE_REPLAY_WAIT_MS",
                "ZINDER_COMPAT__TREE_STATE_REPLAY_WAITERS_PER_SLOT",
                "ZINDER_COMPAT__STALE_TIP_LIMIT_MS",
                "ZINDER_COMPAT__PING_VERY_INSECURE",
                "ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS",
            ]
        );
    }

    #[test]
    fn every_variable_starts_with_zinder_prefix() {
        for env_var in ENVIRONMENT_VARIABLES {
            assert!(
                env_var.name.starts_with("ZINDER_"),
                "documented variables must follow the ZINDER_* convention; got {}",
                env_var.name,
            );
        }
    }
}
