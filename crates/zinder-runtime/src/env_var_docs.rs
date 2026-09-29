//! Operator-facing environment-variable contract for Zinder service binaries.
//!
//! The `ZINDER_*` env-var schema is part of Zinder's public interface (see
//! [Public interfaces §Environment variable mapping]). This module is the
//! single source of truth for the operator-facing surface: the entries in
//! [`ENVIRONMENT_VARIABLES`] feed both the rendered Markdown table embedded
//! in `docs/architecture/public-interfaces.md` and the CI drift test that
//! catches doc/source skew.
//!
//! [`render_environment_variable_table`] returns the Markdown the doc block
//! must contain. Tests assert exact equality; the doc-mirror integration
//! test prints the expected rendering on failure so the operator can
//! copy-paste the canonical output back into the doc.
//!
//! Adding a new env-var to a service config is a two-line change here plus
//! the corresponding field on the relevant config struct. The mirror test
//! makes drift in either direction a build failure.
//!
//! [Public interfaces §Environment variable mapping]:
//!     ../../docs/architecture/public-interfaces.md#environment-variable-mapping

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
/// Mirrored into `docs/architecture/public-interfaces.md` between the
/// `<!-- env-var-table:public-interfaces:start -->` and corresponding `:end`
/// markers; the doc-mirror test in
/// `crates/zinder-runtime/tests/integration/env_var_docs.rs` fails when the
/// rendered table and the doc block diverge.
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
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Required,
        sensitive: false,
        description: "Upstream Zebra JSON-RPC URL the service connects to. Optional for \
                      `zinder-explorer`: without it the upstream-observation probe stays \
                      off and `ExplorerFreshness.chain_view.upstream_tip` is always unset.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__INDEXER_GRPC_ADDR",
        toml_path: "node.indexer_grpc_addr",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Optional Zebra indexer gRPC endpoint enabling the streaming mempool source \
                      and chain-tip wakeups. Falls back to JSON-RPC polling when unset or empty.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__AUTH__METHOD",
        toml_path: "node.auth.method",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
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
            "zinder-projector",
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
            "zinder-projector",
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
            "zinder-projector",
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
            "zinder-projector",
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
            "zinder-projector",
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
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum JSON-RPC response body size (bytes) accepted from the node.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__BROADCAST_TIMEOUT_SECS",
        toml_path: "node.broadcast_timeout_secs",
        used_by: &[
            "zinder-ingest",
            "zinder-projector",
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Per-call timeout (seconds) applied only to `sendrawtransaction`. When \
                      unset, the global `request_timeout_secs` applies instead. Recommended: 7.",
    },
    EnvVarDoc {
        name: "ZINDER_NODE__HEALTH__ADDR",
        toml_path: "node.health.addr",
        used_by: &[
            "zinder-ingest",
            "zinder-query",
            "zinder-compat-lightwalletd",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "URL of the upstream's HTTP `/ready` endpoint. When set, ingest and the \
                      wallet-serving readers poll it as the primary upstream-sync signal; when \
                      unset, they fall back \
                      to `getblockchaininfo.verificationprogress`/`estimatedheight`. See \
                      [ADR-0015](../adrs/0015-phase-driven-ingest.md).",
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
                      Must be greater than zero. Query and compatibility use it to refresh \
                      serving readiness; `zinder-explorer` reuses the same cadence for \
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
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Block gap between `estimatedheight` and the local tip above which the \
                      fallback path reports `upstream_not_ready`. Defaults to 10.",
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
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Listen address for the operational HTTP endpoint \
                      (`/healthz`, `/readyz`, `/metrics`). Defaults to a per-service \
                      loopback address (`127.0.0.1:9105` ingest, `9110` projector, `9106` query, \
                      `9107` compat, `9069` explorer). Set to an empty string to \
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
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Opts a binary in to binding its plaintext serving and operational \
                      surfaces to a public or unspecified (`0.0.0.0`, `::`) address. Defaults \
                      to `false`: a loopback or private-range bind is always allowed, but a \
                      public or unspecified bind is refused at startup unless this is `true`. \
                      Zinder ships no server TLS (ADR-0006); set this only when a reverse \
                      proxy terminates TLS and authorization in front of the listener.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST_CONTROL__LISTEN_ADDR",
        toml_path: "ingest_control.listen_addr",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Listen address of the private IngestControl gRPC endpoint. Localhost-only \
                      by default; cross-host deployments must add bearer-token auth per ADR-0006. \
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
                      on every request (ADR-0006). The writer reads it to verify; the readers \
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
        requirement: Requirement::Required,
        sensitive: false,
        description: "Canonical RocksDB store path. Writers open it as primary; readers open it \
                      as a secondary.",
    },
    EnvVarDoc {
        name: "ZINDER_STORAGE__SECONDARY_PATH",
        toml_path: "storage.secondary_path",
        used_by: &[
            "zinder-ingest (verify-canonical-replay only)",
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
        used_by: &[
            "zinder-query",
            "zinder-compat-lightwalletd",
            "zinder-explorer",
        ],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum startup RocksDB secondary catchup duration before a reader starts \
                      with the opened secondary and lets /readyz report replica lag. Defaults to \
                      30000.",
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
                      for the writer and 67108864 for the wallet-serving reader.",
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
                      for writers and 134217728 for readers.",
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
        description: "Canonical-store RocksDB open SST file cap. Defaults to 512 for writers and \
                      128 for readers.",
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
                      be greater than zero and match the canonical writer. Defaults to 100.",
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
        name: "ZINDER_COMPAT__LISTEN_ADDR",
        toml_path: "compat.listen_addr",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Listen address for the lightwalletd-compatible gRPC endpoint. Defaults to \
                      `127.0.0.1:9067`.",
    },
    EnvVarDoc {
        name: "ZINDER_COMPAT__SERVING",
        toml_path: "compat.serving",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Runtime serving selection: exactly `wallet` or `compact-blocks`; defaults \
                      to `wallet`. `compact-blocks` rejects wallet settings, and no canonical or \
                      legacy aliases are accepted.",
    },
    EnvVarDoc {
        name: "ZINDER_COMPAT__REORG_WINDOW_BLOCKS",
        toml_path: "compat.reorg_window_blocks",
        used_by: &["zinder-compat-lightwalletd"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Exact canonical replacement-depth identity expected by compatibility. Must \
                      be greater than zero and match the canonical writer. Defaults to 100.",
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
                      uses to talk to it. See [ADR-0016](../adrs/0016-source-segment-fetching.md).",
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
                      greater than zero. Defaults to 100.",
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
                      match the canonical writer. Defaults to 100.",
    },
    EnvVarDoc {
        name: "ZINDER_PROJECTOR__BUILD_OWNER_HEX",
        toml_path: "projector.build_owner_hex",
        used_by: &["zinder-projector"],
        requirement: Requirement::Required,
        sensitive: false,
        description: "Stable 16-byte wallet-build lease owner encoded as exactly 32 hexadecimal \
                      characters. Use a distinct value for each concurrently provisioned lane.",
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
        description: "Deadline for the projector's canonical secondary to authenticate the \
                      writer's exact fence. A secondary behind the writer is retried with \
                      capped backoff and reported as syncing once the lag lasts longer than one \
                      second; a same-sequence mismatch or a secondary ahead of the writer fails \
                      immediately. Must be greater than zero. Defaults to 30.",
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
                      68719476736.",
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
        description: "Maximum logical planner and write-batch bytes for one atomic wallet \
                      following transition. Defaults to 536870912.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__PHASE_CLASSIFICATION__CATCHUP_THRESHOLD_BLOCKS",
        toml_path: "ingest.phase_classification.catchup_threshold_blocks",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Gap (in blocks) at which the phase-driven ingest loop transitions between \
                      `BulkCatchup` and `TipFollow`. Defaults to `ingest.reorg_window_blocks`. \
                      See [ADR-0015](../adrs/0015-phase-driven-ingest.md).",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__CONSTRUCTION__CANONICAL_BATCH_MAX_BLOCKS",
        toml_path: "ingest.construction.canonical_batch_max_blocks",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Block count per bulk-catchup commit batch. Defaults to 1000.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__CONSTRUCTION__CANONICAL_BATCH_MAX_ARTIFACT_BYTES",
        toml_path: "ingest.construction.canonical_batch_max_artifact_bytes",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Canonical artifact bytes accumulated before closing a bulk-catchup batch. \
                      Defaults to 536870912.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__CONSTRUCTION__CANONICAL_BATCH_MAX_ESTIMATED_WRITE_BYTES",
        toml_path: "ingest.construction.canonical_batch_max_estimated_write_bytes",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Estimated canonical write bytes accumulated before closing a \
                      bulk-catchup batch. Defaults to 536870912.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__CONSTRUCTION__CANONICAL_BATCH_MIN_BLOCKS_BEFORE_ESTIMATED_WRITE_CLOSE",
        toml_path: "ingest.construction.canonical_batch_min_blocks_before_estimated_write_close",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Minimum blocks accumulated before estimated write bytes can close a \
                      bulk-catchup batch. Single oversized blocks can still close immediately. \
                      Defaults to 100.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__CONSTRUCTION__SOURCE_SEGMENT_MAX_BLOCKS",
        toml_path: "ingest.construction.source_segment_max_blocks",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Diagnostic override for the hard ceiling on connected blocks requested \
                      from the source in one segment. The resource-resolved default is 64.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__CONSTRUCTION__SOURCE_SEGMENT_TARGET_RESPONSE_BYTES",
        toml_path: "ingest.construction.source_segment_target_response_bytes",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Diagnostic override for adaptive response sizing. The default is \
                      `min(node.max_response_bytes, 33554432)`.",
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
                      402653184))`.",
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
        name: "ZINDER_INGEST__CONSTRUCTION__COMMIT_REASSEMBLY_MAX_QUEUED_ARTIFACT_BYTES",
        toml_path: "ingest.construction.commit_reassembly_max_queued_artifact_bytes",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Maximum settled-tip artifact bytes that can accumulate while the previous \
                      bulk-catchup batch is attaching metadata, committing, or flushing. \
                      Defaults to 536870912.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__CONSTRUCTION__FLUSH_INTERVAL_EPOCHS",
        toml_path: "ingest.construction.flush_interval_epochs",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Bulk-catchup RocksDB flush cadence in committed epochs. Must be greater \
                      than zero. Defaults to 5.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__FOLLOW__POLL_INTERVAL_MS",
        toml_path: "ingest.follow.poll_interval_ms",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Tip-follow poll cadence in milliseconds. Must be greater than zero. \
                      Defaults to 1000.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__FOLLOW__LAG_THRESHOLD_BLOCKS",
        toml_path: "ingest.follow.lag_threshold_blocks",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Block lag at which tip-follow reports `cause=syncing`. Defaults to 1.",
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
        description: "Pre-seed an empty store from an upstream-supplied checkpoint at this \
                      height.",
    },
    EnvVarDoc {
        name: "ZINDER_INGEST__RUN_OVERRIDES__ALLOW_REORG_WINDOW_SETTLEMENT",
        toml_path: "ingest.run_overrides.allow_reorg_window_settlement",
        used_by: &["zinder-ingest"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "Disposable-store override: lets bulk-catchup advance the settled tip inside \
                      the reorg window. Invalid combined with `coverage = \"wallet-serving\"`.",
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
                      on cross-service explorer-plane reads (ADR-0006).",
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
        name: "ZINDER_EXPLORER__WALLET_QUERY_ENDPOINT",
        toml_path: "explorer.wallet_query_endpoint",
        used_by: &["zinder-explorer"],
        requirement: Requirement::Optional,
        sensitive: false,
        description: "WalletQuery gRPC endpoint backing the explorer's wallet-composed reads \
                      (transaction detail, block views, search, mempool activity). Empty/unset \
                      disables the explorer capabilities that compose canonical wallet reads.",
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

        assert_eq!(background_job_variables.len(), 2);
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
                "ZINDER_COMPAT__SERVING",
                "ZINDER_COMPAT__REORG_WINDOW_BLOCKS",
                "ZINDER_COMPAT__PAIR_CONVERGENCE_ATTEMPTS",
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
