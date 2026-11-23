# Configuration

This page lists the settings each Zinder runtime reads, how they combine, and
the fixed limits that public listeners enforce. It is for operators who deploy
Zinder and for client developers who need to know why a request was refused.

## Settings for a first deployment

Most settings have working defaults. A wallet-serving deployment of
`zinder-ingest`, `zinder-projector`, `zinder-query`, and
`zinder-compat-lightwalletd` needs the settings below.

| Setting | Runtimes | Value |
| --- | --- | --- |
| `network.name` | all | `zcash-mainnet`, `zcash-testnet`, or `zcash-regtest` |
| `node.json_rpc_addr` | ingest; query and compat to broadcast | JSON-RPC URL of an archive Zebra or Zakura node |
| `node.auth.method` | wherever `[node]` is set | `cookie`, with a cookie file in `node.auth.path`; or `basic`, with `node.auth.username` and `node.auth.password` |
| `ingest.source` | ingest | `zebra-json-rpc`, the only supported value |
| `ingest.run_overrides.coverage` | ingest | `wallet-serving`, so ingest keeps the history and transaction bytes that wallets need |
| `storage.path` | ingest, query, compat, explorer | Canonical store directory |
| `storage.canonical_path` | projector | The same canonical store directory |
| `wallet.path` | projector, query, compat | Wallet store directory |
| `storage.secondary_path`, `wallet.secondary_path` | query, compat | A directory for each process, never shared |
| `projector.build_owner_hex` | projector | A stable value of 32 hexadecimal characters |
| `projector.lease_duration_seconds` | projector | At least `14400` |
| `ingest_control.bearer_token_path` | ingest, projector, query, compat | File holding a shared secret. Ingest requires it when `ingest_control.listen_addr` is not a loopback address. |
| `security.allow_public_bind` | any runtime bound to `0.0.0.0`, `::`, or a public address | `true`, and only behind a proxy that terminates TLS |

The `*.toml` files directly in `deploy/config/` are complete examples for the
Compose topology, and [Deploying Zinder](../runbooks/deploying.md) walks
through them. Run any binary with `--print-config` to see the merged result
before you start it.

Every listener binds to loopback by default. The
[security boundary](../architecture/service-operations.md#security-boundary)
explains which addresses `security.allow_public_bind` admits and why a public
listener needs your own proxy.

The runtime images change those defaults. Each image installs a file from
`deploy/config/images/` at `/etc/zinder/config.toml` and starts with
`--config /etc/zinder/config.toml`. The file sets
`security.allow_public_bind = true` and these listeners:

| Image | Listeners |
| --- | --- |
| `zinder-ingest` | `ops.listen_addr = "[::]:9105"` |
| `zinder-projector` | `ops.listen_addr = "[::]:9110"` |
| `zinder-query` | `query.listen_addr = "[::]:9102"`, `ops.listen_addr = "[::]:9106"` |
| `zinder-compat-lightwalletd` | `compat.listen_addr = "[::]:9067"`, `ops.listen_addr = "[::]:9107"` |
| `zinder-explorer` | `explorer.listen_addr = "[::]:9068"`, `ops.listen_addr = "[::]:9069"` |

Other listeners, such as `ingest_control.listen_addr`, keep their loopback
defaults. The image files leave network, node, and storage settings to each
binary. Supply required values and any overrides with `ZINDER_*` variables
or a mounted file. A file you mount at
`/etc/zinder/config.toml` replaces the image file whole. A listener it does
not set then binds loopback, and without `allow_public_bind = true` a
listener on `[::]` or a public address is refused. Arguments after the image
name replace `--config /etc/zinder/config.toml`, so repeat it when you pass
your own.

## How configuration is loaded

Every binary merges four layers. Each layer overrides the one before it:

1. Compiled defaults.
2. The TOML file passed with `--config <path>`.
3. `ZINDER_*` environment variables.
4. Command-line flags.

An environment variable name is `ZINDER_` followed by the TOML path in
uppercase, with `__` between levels. A single `_` stays inside a key. For
example, `ingest.follow.poll_interval_ms` becomes
`ZINDER_INGEST__FOLLOW__POLL_INTERVAL_MS`.

The loader enforces these rules:

- An unknown key fails startup, whether it comes from the file or the
  environment. For a rejected variable, the error names it and suggests a
  correction.
- Cross-field rules are checked before storage opens or a listener binds.
- `--print-config` prints the merged configuration as TOML without opening
  storage or binding. Passwords and inline cookies are redacted.
- `ZINDER_TEST_*` variables never reach the loader. They are test-only knobs.
- `RUST_LOG` sets the log filter and has no TOML field. Unset or blank means
  `info`. A value that does not parse fails startup.
- The single-volume image entrypoints read `ZINDER_WALLET_SERVING__*` and the
  `ZINDER_NODE__INDEXER_TLS__*_PEM` forms themselves. The binaries refuse the
  `*_PEM` variables. See
  [single-volume deployment](../runbooks/deploying-single-volume.md).

[ADR-0021](../adrs/0021-shared-configuration-sections.md) records why sections
such as `[node]`, `[ops]`, and `[security]` are shared by every runtime.

## Sections by runtime

| Runtime | Sections |
| --- | --- |
| `zinder-ingest` | `network`, `node`, `node_fallbacks.N`, `storage`, `ingest`, `retention`, `ingest_control`, `ops`, `security` |
| `zinder-projector` | `network`, `storage`, `wallet`, `projector`, `ingest_control`, `projector_control`, `ops`, `security` |
| `zinder-query` | `network`, `node`, `node_fallbacks.N`, `storage`, `wallet`, `query`, `ingest_control`, `ops`, `security` |
| `zinder-compat-lightwalletd` | `network`, `node`, `node_fallbacks.N`, `storage`, `wallet`, `compat`, `ingest_control`, `ops`, `security` |
| `zinder-explorer` | `network`, `node`, `node_fallbacks.N`, `storage`, `explorer`, `ops`, `security` |
| `zinder-compat-cipherscan` | `network`, `cipherscan`, `ops`, `security` |

`[ingest]` has the subsections `construction`, `follow`, `mempool`,
`phase_classification`, `run_overrides`, `tree_state_densification`, and
`tree_state_verification`. `[projector]` has `build` and `follow`.

### The node sections

Each runtime uses `[node]` differently:

- Ingest requires it.
- Query and wallet-mode compatibility use it only to broadcast transactions
  and, in compatibility, to read the recommended fee, for which a pruned node
  works. They start without contacting the node. Without `[node]` they serve
  stored data and do not advertise broadcast, and compatibility answers
  `GetStandardFee` with `UNIMPLEMENTED`. Any node setting requires
  `node.json_rpc_addr`.
- Explorer uses it only to report the node's tip in its freshness envelope.
- The projector, and compatibility with `compat.serving = "compact-blocks"`,
  reject it.

`[node_fallbacks.N]` adds fallback endpoints. `[node]` is position 0, and the
fallbacks count from 1 without a gap. Each fallback table takes the fields of
`[node]`, and its environment names start with `ZINDER_NODE_FALLBACKS__<N>__`.
A fallback does not inherit `broadcast_timeout_secs` from `[node]`, so set it
on each endpoint. Positions are table keys, not a TOML array, so a file and the
environment merge field by field. A TOML array of tables
(`[[node_fallbacks]]`) is refused:

```toml
[node]
json_rpc_addr = "http://zebra:8232"

[node_fallbacks.1]
json_rpc_addr = "http://zakura:8232"
indexer_grpc_addr = "http://zakura:8230"

[node_fallbacks.1.auth]
method = "cookie"
path = "/var/run/auth-fallback/.cookie"
```

[Node endpoint failover](../architecture/chain-ingestion.md#node-endpoint-failover)
describes how each runtime uses the list.

## Environment variables

Every `ZINDER_*` variable a binary accepts is listed below, with its TOML
field. The table is generated by
`zinder_runtime::render_environment_variable_table()`. A test checks that the
table matches, and each binary's `env_var_contract` test checks that it
accepts exactly the variables whose "Used by" column names it. The tests do
not check descriptions or defaults.

<!-- env-var-table:public-interfaces:start -->
| Variable | Used by | Requirement | TOML field | Description |
| -------- | ------- | ----------- | ---------- | ----------- |
| `ZINDER_NETWORK__NAME` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer, zinder-compat-cipherscan | Required | `network.name` | Network identifier: `zcash-mainnet`, `zcash-testnet`, or `zcash-regtest`. Note: live-test gating reads the bare `ZINDER_NETWORK` env var directly and never reaches the config loader, so test runbooks still quote that form. |
| `ZINDER_NODE__JSON_RPC_ADDR` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `running zinder-ingest, or broadcasting transactions or reading the recommended fee from a reader` | `node.json_rpc_addr` | Upstream Zebra JSON-RPC URL. `zinder-ingest` requires it. `zinder-query` and wallet-mode `zinder-compat-lightwalletd` broadcast transactions through it, compatibility also reads the recommended fee for `GetStandardFee` through it, and both start without contacting it. Without it they serve storage only and advertise no broadcast, and compatibility answers `GetStandardFee` with `UNIMPLEMENTED`. Compact-blocks compatibility rejects every `ZINDER_NODE__*` variable. Optional for `zinder-explorer`: without it the upstream-observation probe stays off and `ExplorerFreshness.chain_view.upstream_tip` is always unset. |
| `ZINDER_NODE__INDEXER_GRPC_ADDR` | zinder-ingest | Optional | `node.indexer_grpc_addr` | Optional Zebra indexer gRPC endpoint enabling the streaming mempool source. Falls back to JSON-RPC mempool polling when unset or empty. An `https://` URL negotiates TLS, configured by the `ZINDER_NODE__INDEXER_TLS__*` variables; without them the node is verified against the system roots. |
| `ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH` | zinder-ingest | Optional | `node.indexer_tls.ca_cert_path` | PEM CA bundle that verifies the indexer's server certificate instead of the system roots. Requires an `https://` `ZINDER_NODE__INDEXER_GRPC_ADDR`. Read at startup and whenever an indexer stream reconnects. |
| `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH` | zinder-ingest | When `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH is set` | `node.indexer_tls.client_cert_path` | PEM client certificate chain presented to an indexer that requires mutual TLS. Set together with `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH`. |
| `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH` | zinder-ingest | When `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH is set` | `node.indexer_tls.client_key_path` | Path to the PEM private key for the indexer client certificate. The path is shown in `--print-config`; the key is read at startup and whenever an indexer stream reconnects. The `*_PEM` variables that the single-volume image entrypoints convert into these files are refused by the binaries. |
| `ZINDER_NODE__INDEXER_TLS__SERVER_NAME` | zinder-ingest | Optional | `node.indexer_tls.server_name` | Name verified against the indexer's certificate when it differs from the host in `ZINDER_NODE__INDEXER_GRPC_ADDR`. |
| `ZINDER_NODE__AUTH__METHOD` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node.auth.method` | Upstream-node auth shape: `basic`, `cookie`, or unset for no auth. |
| `ZINDER_NODE__AUTH__USERNAME` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `ZINDER_NODE__AUTH__METHOD=basic` | `node.auth.username` | Basic-auth username. Paired with `ZINDER_NODE__AUTH__PASSWORD`. |
| `ZINDER_NODE__AUTH__PASSWORD` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `ZINDER_NODE__AUTH__METHOD=basic` | `node.auth.password` | Basic-auth password. Redacted in `--print-config` and structured logs. (sensitive; redacted) |
| `ZINDER_NODE__AUTH__PATH` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `ZINDER_NODE__AUTH__METHOD=cookie` | `node.auth.path` | Path to a cookie file. Mutually exclusive with `ZINDER_NODE__AUTH__COOKIE`. |
| `ZINDER_NODE__AUTH__COOKIE` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `ZINDER_NODE__AUTH__METHOD=cookie` | `node.auth.cookie` | Inline cookie credentials (`username:password`). Mutually exclusive with `ZINDER_NODE__AUTH__PATH`. Accepted for PaaS environments without persistent disks. (sensitive; redacted) |
| `ZINDER_NODE__REQUEST_TIMEOUT_SECS` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node.request_timeout_secs` | Upstream-node JSON-RPC request timeout in seconds. Defaults to 30. |
| `ZINDER_NODE__MAX_RESPONSE_BYTES` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node.max_response_bytes` | Maximum JSON-RPC response body size (bytes) accepted from the node. Defaults to 67108864 (64 MiB). This is a ceiling on what Zinder accepts, not a request size: Zebra 7.1.0 and Zakura 1.8.6 refuse responses above 50 MiB by default, and ingest splits a source segment the node refuses. |
| `ZINDER_NODE__BROADCAST_TIMEOUT_SECS` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node.broadcast_timeout_secs` | Per-call timeout (seconds) applied only to `sendrawtransaction`. When unset, the global `request_timeout_secs` applies instead. Recommended: 7. With fallback endpoints, a broadcast moves to the next endpoint only after the endpoint's own timeout, so keep it well below wallet request deadlines. Fallback endpoints do not inherit it; set `ZINDER_NODE_FALLBACKS__<N>__BROADCAST_TIMEOUT_SECS` for each. |
| `ZINDER_NODE__HEALTH__ADDR` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node.health.addr` | URL of the upstream's HTTP `/ready` endpoint. When set, ingest, the wallet-serving readers, and `zinder-explorer` poll it as the primary upstream-sync signal; when unset, they fall back to `getblockchaininfo.verificationprogress`/`estimatedheight`. Compatibility with `compat.serving = "compact-blocks"` rejects every node setting. See [ADR-0009](../adrs/0009-ingest-lifecycle-and-construction.md). |
| `ZINDER_NODE__HEALTH__POLL_INTERVAL_MS` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node.health.poll_interval_ms` | Cadence of the upstream-health probe in milliseconds. Defaults to 30000. Must be greater than zero. Query and wallet-serving compatibility use it to verify every broadcast endpoint, `[node]` and each fallback, on first contact and then refresh the node dependency their readiness reports; compact-blocks compatibility rejects it; `zinder-explorer` reuses the same cadence for its upstream-observation probe (the one that populates `ExplorerFreshness.chain_view.upstream_tip`). |
| `ZINDER_NODE__HEALTH__VERIFICATION_PROGRESS_FLOOR` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node.health.verification_progress_floor` | Lower bound on `getblockchaininfo.verificationprogress` below which the fallback path reports `upstream_not_ready`. Defaults to 0.999. Must be in `(0.0, 1.0)`. |
| `ZINDER_NODE__HEALTH__ESTIMATED_GAP_FLOOR_BLOCKS` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node.health.estimated_gap_floor_blocks` | Block gap between `estimatedheight` and the local tip above which the fallback path reports `upstream_not_ready`. Defaults to 10. |
| `ZINDER_NODE_FALLBACKS__<N>__JSON_RPC_ADDR` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `any ZINDER_NODE_FALLBACKS__<N>__* is set` | `node_fallbacks.<N>.json_rpc_addr` | JSON-RPC URL of fallback node endpoint `<N>`. Endpoints form one ordered list: `[node]` is position 0 and `[node_fallbacks.N]` is position `N`, starting at 1 with no gap. Every `ZINDER_NODE__*` field has a `ZINDER_NODE_FALLBACKS__<N>__*` counterpart. `zinder-ingest` switches to a fallback when its active endpoint fails. `zinder-query` and wallet-mode `zinder-compat-lightwalletd` broadcast, and compatibility reads the recommended fee, through the next verified endpoint when one is unreachable. `zinder-explorer` validates the list and reads only `[node]`. |
| `ZINDER_NODE_FALLBACKS__<N>__INDEXER_GRPC_ADDR` | zinder-ingest | Optional | `node_fallbacks.<N>.indexer_grpc_addr` | Same as `ZINDER_NODE__INDEXER_GRPC_ADDR`, for fallback endpoint `<N>`. After a switch, ingest follows the mempool of the new endpoint through it. |
| `ZINDER_NODE_FALLBACKS__<N>__INDEXER_TLS__CA_CERT_PATH` | zinder-ingest | Optional | `node_fallbacks.<N>.indexer_tls.ca_cert_path` | Same as `ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH`, for fallback endpoint `<N>`. The single-volume image entrypoints also accept the `_PEM` form of this file. |
| `ZINDER_NODE_FALLBACKS__<N>__INDEXER_TLS__CLIENT_CERT_PATH` | zinder-ingest | When `ZINDER_NODE_FALLBACKS__<N>__INDEXER_TLS__CLIENT_KEY_PATH is set` | `node_fallbacks.<N>.indexer_tls.client_cert_path` | Same as `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH`, for fallback endpoint `<N>`. The single-volume image entrypoints also accept the `_PEM` form of this file. |
| `ZINDER_NODE_FALLBACKS__<N>__INDEXER_TLS__CLIENT_KEY_PATH` | zinder-ingest | When `ZINDER_NODE_FALLBACKS__<N>__INDEXER_TLS__CLIENT_CERT_PATH is set` | `node_fallbacks.<N>.indexer_tls.client_key_path` | Same as `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH`, for fallback endpoint `<N>`. The single-volume image entrypoints also accept the `_PEM` form of this file. |
| `ZINDER_NODE_FALLBACKS__<N>__INDEXER_TLS__SERVER_NAME` | zinder-ingest | Optional | `node_fallbacks.<N>.indexer_tls.server_name` | Same as `ZINDER_NODE__INDEXER_TLS__SERVER_NAME`, for fallback endpoint `<N>`. |
| `ZINDER_NODE_FALLBACKS__<N>__AUTH__METHOD` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node_fallbacks.<N>.auth.method` | Same as `ZINDER_NODE__AUTH__METHOD`, for fallback endpoint `<N>`. |
| `ZINDER_NODE_FALLBACKS__<N>__AUTH__USERNAME` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `ZINDER_NODE_FALLBACKS__<N>__AUTH__METHOD=basic` | `node_fallbacks.<N>.auth.username` | Same as `ZINDER_NODE__AUTH__USERNAME`, for fallback endpoint `<N>`. |
| `ZINDER_NODE_FALLBACKS__<N>__AUTH__PASSWORD` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `ZINDER_NODE_FALLBACKS__<N>__AUTH__METHOD=basic` | `node_fallbacks.<N>.auth.password` | Same as `ZINDER_NODE__AUTH__PASSWORD`, for fallback endpoint `<N>`. Redacted in `--print-config` and structured logs. (sensitive; redacted) |
| `ZINDER_NODE_FALLBACKS__<N>__AUTH__PATH` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `ZINDER_NODE_FALLBACKS__<N>__AUTH__METHOD=cookie` | `node_fallbacks.<N>.auth.path` | Same as `ZINDER_NODE__AUTH__PATH`, for fallback endpoint `<N>`. |
| `ZINDER_NODE_FALLBACKS__<N>__AUTH__COOKIE` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `ZINDER_NODE_FALLBACKS__<N>__AUTH__METHOD=cookie` | `node_fallbacks.<N>.auth.cookie` | Same as `ZINDER_NODE__AUTH__COOKIE`, for fallback endpoint `<N>`. Redacted in `--print-config` and structured logs. (sensitive; redacted) |
| `ZINDER_NODE_FALLBACKS__<N>__REQUEST_TIMEOUT_SECS` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node_fallbacks.<N>.request_timeout_secs` | Same as `ZINDER_NODE__REQUEST_TIMEOUT_SECS`, for fallback endpoint `<N>`. |
| `ZINDER_NODE_FALLBACKS__<N>__MAX_RESPONSE_BYTES` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node_fallbacks.<N>.max_response_bytes` | Same as `ZINDER_NODE__MAX_RESPONSE_BYTES`, for fallback endpoint `<N>`. |
| `ZINDER_NODE_FALLBACKS__<N>__BROADCAST_TIMEOUT_SECS` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node_fallbacks.<N>.broadcast_timeout_secs` | Same as `ZINDER_NODE__BROADCAST_TIMEOUT_SECS`, for fallback endpoint `<N>`. It is not inherited from `[node]`: when unset, this endpoint's own `request_timeout_secs` applies. Recommended: 7. |
| `ZINDER_NODE_FALLBACKS__<N>__HEALTH__ADDR` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node_fallbacks.<N>.health.addr` | Same as `ZINDER_NODE__HEALTH__ADDR`, for fallback endpoint `<N>`. |
| `ZINDER_NODE_FALLBACKS__<N>__HEALTH__POLL_INTERVAL_MS` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node_fallbacks.<N>.health.poll_interval_ms` | Same as `ZINDER_NODE__HEALTH__POLL_INTERVAL_MS`, for fallback endpoint `<N>`, in ingest. Query and wallet-serving compatibility ignore it and verify and observe every endpoint on `ZINDER_NODE__HEALTH__POLL_INTERVAL_MS`. |
| `ZINDER_NODE_FALLBACKS__<N>__HEALTH__VERIFICATION_PROGRESS_FLOOR` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node_fallbacks.<N>.health.verification_progress_floor` | Same as `ZINDER_NODE__HEALTH__VERIFICATION_PROGRESS_FLOOR`, for fallback endpoint `<N>`. |
| `ZINDER_NODE_FALLBACKS__<N>__HEALTH__ESTIMATED_GAP_FLOOR_BLOCKS` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node_fallbacks.<N>.health.estimated_gap_floor_blocks` | Same as `ZINDER_NODE__HEALTH__ESTIMATED_GAP_FLOOR_BLOCKS`, for fallback endpoint `<N>`. |
| `ZINDER_OPS__LISTEN_ADDR` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer, zinder-compat-cipherscan | Optional | `ops.listen_addr` | Listen address for the operational HTTP endpoint (`/healthz`, `/readyz`, `/metrics`). Defaults to a per-service loopback address (`127.0.0.1:9105` ingest, `9110` projector, `9106` query, `9107` compat, `9069` explorer, `9108` cipherscan). Set to an empty string to disable the endpoint entirely. |
| `ZINDER_SECURITY__ALLOW_PUBLIC_BIND` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer, zinder-compat-cipherscan | Optional | `security.allow_public_bind` | Opts a binary in to binding its plaintext serving and operational surfaces to a public or unspecified (`0.0.0.0`, `::`) address. Defaults to `false`: a loopback or private-range bind is always allowed, but a public or unspecified bind is refused at startup unless this is `true`. Zinder ships no server TLS (ADR-0011); set this only when a reverse proxy terminates TLS and authorization in front of the listener. |
| `ZINDER_INGEST_CONTROL__LISTEN_ADDR` | zinder-ingest | Optional | `ingest_control.listen_addr` | Listen address of the private IngestControl gRPC endpoint. Localhost-only by default; cross-host deployments must add bearer-token auth per ADR-0011. Set to an empty string to disable the endpoint for diagnostic one-shot runs (such as `--target-height` pre-seed). |
| `ZINDER_INGEST_CONTROL__ADDR` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `ingest_control.addr` | URL of the colocated IngestControl writer (`http://host:port`). Readers use it for tip-change subscriptions, mempool reads, and writer-status lookups. Defaults to `http://127.0.0.1:9100`. |
| `ZINDER_INGEST_CONTROL__BEARER_TOKEN_PATH` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd | When `ingest enforces auth` | `ingest_control.bearer_token_path` | Path to the shared-secret bearer token the IngestControl endpoint enforces on every request (ADR-0011). The writer reads it to verify; the readers read the same file to present. File-only by policy; inline secrets are rejected at config load. |
| `ZINDER_INGEST_CONTROL__CHECKPOINT_STAGING_ROOT` | zinder-ingest | Optional | `ingest_control.checkpoint_staging_root` | Directory containing freshly prepared state-bundle candidate directories. CanonicalControl accepts only an opaque candidate id and creates its canonical checkpoint at `<root>/<candidate-id>/canonical.rocksdb`; production mounts this path from a dedicated staging volume into ingest and projector only, never query or compatibility. Defaults to `/var/lib/zinder/checkpoints`. |
| `ZINDER_INGEST_CONTROL__CHECKPOINT_BEARER_TOKEN_PATH` | zinder-ingest | When `canonical checkpoint capture is enabled` | `ingest_control.checkpoint_bearer_token_path` | Path to the separate method-level token required by CanonicalControl.CreateOwnerCheckpoint and ReadmitOwnerCheckpoint. Mount this file only into ingest and projector; query and compatibility must not receive it. |
| `ZINDER_PROJECTOR_CONTROL__LISTEN_ADDR` | zinder-projector | Optional | `projector_control.listen_addr` | Loopback-only private ProjectorControl gRPC endpoint for coherent capture. Empty or unset disables it; an enabled endpoint requires projector_control.bearer_token_path. |
| `ZINDER_PROJECTOR_CONTROL__BEARER_TOKEN_PATH` | zinder-projector | When `projector control is enabled` | `projector_control.bearer_token_path` | Path to the token required by ProjectorControl and presented as the canonical checkpoint capability. Mount it only into projector and ingest; query and compatibility never read it. |
| `ZINDER_PROJECTOR_CONTROL__CHECKPOINT_STAGING_ROOT` | zinder-projector | Optional | `projector_control.checkpoint_staging_root` | Shared candidate root whose realpath must match ingest_control.checkpoint_staging_root. The projector sends only a SHA-256 root binding to canonical control, never a path. |
| `ZINDER_STORAGE__PATH` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `running zinder-query or zinder-compat-lightwalletd` | `storage.path` | Canonical RocksDB store directory. Defaults to `/var/lib/zinder/canonical` for ingest and Explorer. Ingest writes the primary store here. Explorer locates its materialized-view store beneath this directory. Query and lightwalletd open the canonical store as a secondary. |
| `ZINDER_STORAGE__SECONDARY_PATH` | zinder-query, zinder-compat-lightwalletd, zinder-explorer | Required | `storage.secondary_path` | Process-unique RocksDB secondary metadata directory. Never share this path across reader processes. |
| `ZINDER_STORAGE__INITIAL_CATCHUP_TIMEOUT_MS` | zinder-query, zinder-compat-lightwalletd | Optional | `storage.initial_catchup_timeout_ms` | Maximum startup RocksDB secondary catchup duration before a reader starts with the opened secondary and lets /readyz report replica lag. Defaults to 30000. |
| `ZINDER_STORAGE__SECONDARY_CATCHUP_INTERVAL_MS` | zinder-query, zinder-compat-lightwalletd | Optional | `storage.secondary_catchup_interval_ms` | Cadence of RocksDB secondary catchup ticks in milliseconds. Defaults to 1000. Must be greater than zero. |
| `ZINDER_STORAGE__SECONDARY_REPLICA_LAG_THRESHOLD_CHAIN_EPOCHS` | zinder-query, zinder-compat-lightwalletd | Optional | `storage.secondary_replica_lag_threshold_chain_epochs` | Chain epochs the secondary may trail the primary before `/readyz` reports replica lag. Defaults to 4. |
| `ZINDER_STORAGE__SERVING_PAIR_STALENESS_CEILING_MS` | zinder-query, zinder-compat-lightwalletd | Optional | `storage.serving_pair_staleness_ceiling_ms` | Longest a wallet-serving pair keeps admitting traffic, in milliseconds, after the last writer attestation. The reader refreshes at least every half ceiling, even if `storage.secondary_catchup_interval_ms` is longer, so a writer that answers attests it again before the ceiling passes. Traffic stops when the ceiling passes, even while a refresh is still waiting for the writer. Defaults to 300000. Must be greater than zero. |
| `ZINDER_STORAGE__CANONICAL_PATH` | zinder-projector | Optional | `storage.canonical_path` | Canonical primary RocksDB path the projector opens as a read-only secondary. Defaults to `/var/lib/zinder/canonical`. |
| `ZINDER_STORAGE__CANONICAL_SECONDARY_PATH` | zinder-projector | Optional | `storage.canonical_secondary_path` | Projector-local RocksDB secondary metadata directory for canonical reads. Defaults to `/var/lib/zinder/projector/canonical-secondary`; never share it with another process. |
| `ZINDER_WALLET__PATH` | zinder-projector, zinder-query, zinder-compat-lightwalletd | When `running a wallet-serving reader` | `wallet.path` | Wallet-projection RocksDB primary path. The projector owns it as the primary writer and defaults to `/var/lib/zinder/wallet`; both serving runtimes open it as a read-only secondary and require an explicit path. |
| `ZINDER_WALLET__SECONDARY_PATH` | zinder-query, zinder-compat-lightwalletd | Required | `wallet.secondary_path` | Wallet-serving reader root for immutable wallet-secondary generations. Must be distinct from every primary and canonical-secondary path. |
| `ZINDER_WALLET__ROCKSDB__BLOCK_CACHE_BYTES` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.block_cache_bytes` | Wallet-projection RocksDB block cache budget in bytes. Defaults to 268435456 for the writer and 67108864 for the wallet-serving reader. In zinder-query and zinder-compat-lightwalletd, one cache of this size serves every wallet secondary generation of the process. |
| `ZINDER_WALLET__ROCKSDB__MAX_WAL_BYTES` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.max_wal_bytes` | Wallet-projection RocksDB live WAL ceiling in bytes. Defaults to 268435456 for the writer and 16777216 for the wallet-serving reader. |
| `ZINDER_WALLET__ROCKSDB__MAX_OPEN_FILES` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.max_open_files` | Wallet-projection RocksDB open SST file cap. Defaults to 512 for the writer and 64 for the wallet-serving reader. |
| `ZINDER_WALLET__ROCKSDB__WRITE_BUFFER_BYTES` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.write_buffer_bytes` | Wallet-projection RocksDB per-column-family write buffer size. Defaults to 16777216 for the writer and 4194304 for the wallet-serving reader. |
| `ZINDER_WALLET__ROCKSDB__MAX_WRITE_BUFFER_COUNT` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.max_write_buffer_count` | Wallet-projection RocksDB mutable plus immutable write buffer count. Defaults to 4 for the writer and 2 for the wallet-serving reader. |
| `ZINDER_WALLET__ROCKSDB__MAX_BACKGROUND_JOBS` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.max_background_jobs` | Wallet-projection primary-writer RocksDB background job cap shared by flush and compaction work. Defaults to 2 and is not applied to secondary opens, including wallet-serving readers. |
| `ZINDER_WALLET__ROCKSDB__MEMTABLE_BUDGET_BYTES` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.memtable_budget_bytes` | Wallet-projection RocksDB total memtable budget across column families. Defaults to 536870912 for the writer and 16777216 for a wallet-serving reader. |
| `ZINDER_WALLET__ROCKSDB__STATISTICS_LEVEL` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.statistics_level` | Wallet-projection RocksDB statistics collection gate: `off`, `tickers`, or `full`. Defaults to `tickers`. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__BLOCK_CACHE_BYTES` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.block_cache_bytes` | Canonical-store RocksDB block cache budget in bytes. Defaults to 536870912 for writers. Readers default to one eighth of the detected cgroup memory limit, clamped between 134217728 and 536870912, and to 134217728 without a limit. In zinder-query and zinder-compat-lightwalletd, one cache of this size serves every canonical secondary generation of the process. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_WAL_BYTES` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.max_wal_bytes` | Canonical-store RocksDB live WAL ceiling in bytes. Defaults to 268435456 for writers and 33554432 for readers. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.max_open_files` | Canonical-store RocksDB open SST file cap. Defaults to 512 for writers. Readers default to 128 to 256 based on the detected container memory limit, or 128 without a limit. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__WRITE_BUFFER_BYTES` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.write_buffer_bytes` | Canonical-store per-column-family RocksDB write buffer size. Defaults to 16777216 for writers and 8388608 for readers. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_WRITE_BUFFER_COUNT` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.max_write_buffer_count` | Canonical-store per-column-family mutable plus immutable RocksDB write buffer count. Defaults to 2. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_BACKGROUND_JOBS` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.max_background_jobs` | Canonical-store primary-writer RocksDB background job cap shared by flush and compaction work. Defaults to 2 and is not applied to secondary opens. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__MEMTABLE_BUDGET_BYTES` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.memtable_budget_bytes` | Canonical-store total RocksDB memtable budget across column families. Defaults to 268435456 for writers and 16777216 for readers. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__STATISTICS_LEVEL` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.statistics_level` | Canonical-store RocksDB statistics collection gate: `off`, `tickers`, or `full`. Defaults to `tickers`. |
| `ZINDER_QUERY__LISTEN_ADDR` | zinder-query | Optional | `query.listen_addr` | Listen address for the native WalletQuery gRPC endpoint. Defaults to `127.0.0.1:9102`. |
| `ZINDER_QUERY__REORG_WINDOW_BLOCKS` | zinder-query | Optional | `query.reorg_window_blocks` | Exact canonical replacement-depth identity expected by native query. Must be greater than zero and match the canonical writer. Defaults to 1000. |
| `ZINDER_QUERY__PAIR_CONVERGENCE_ATTEMPTS` | zinder-query | Optional | `query.pair_convergence_attempts` | Maximum bounded attempts to converge and admit native query's canonical and wallet secondary pair. Must be in 1..=64; defaults to 12. |
| `ZINDER_QUERY__TREE_STATE_REPLAY_WAIT_MS` | zinder-query | Optional | `query.tree_state_replay_wait_ms` | Milliseconds a tree-state rebuild waits for a rebuild slot before it is refused with `TREE_STATE_REPLAY_LIMIT_REACHED`. Must be in 0..=5000; 0 refuses at once; defaults to 2000. |
| `ZINDER_QUERY__TREE_STATE_REPLAY_WAITERS_PER_SLOT` | zinder-query | Optional | `query.tree_state_replay_waiters_per_slot` | Tree-state rebuilds that may wait for each rebuild slot; a rebuild that finds every waiting place taken is refused at once with `TREE_STATE_REPLAY_LIMIT_REACHED`. Must be in 1..=256; defaults to 64, enough for one slot to serve its waiters at a 30 ms rebuild within the default 2-second wait. |
| `ZINDER_QUERY__TRUST_PROXY_CLIENT_ADDRESS` | zinder-query | Optional | `query.trust_proxy_client_address` | Counts each WalletQuery request against the client connection that a reverse proxy names in the `x-zinder-client-address` header, instead of the proxy's own connection, when the receive limit picks which client to refuse. Set it only when clients reach the listener solely through a proxy that replaces the header. Defaults to false. |
| `ZINDER_COMPAT__LISTEN_ADDR` | zinder-compat-lightwalletd | Optional | `compat.listen_addr` | Listen address for the lightwalletd-compatible gRPC endpoint. Defaults to `127.0.0.1:9067`. |
| `ZINDER_COMPAT__BROWSER_ORIGINS` | zinder-compat-lightwalletd | Optional | `compat.browser_origins` | Enables gRPC-Web for the listed browser origins. Use comma-separated HTTP or HTTPS origins in the environment, or an array in TOML. An empty list disables browser access (the default). Use `*` alone to allow every origin. |
| `ZINDER_COMPAT__SERVING` | zinder-compat-lightwalletd | Optional | `compat.serving` | Runtime serving selection: exactly `wallet` or `compact-blocks`; defaults to `wallet`. `compact-blocks` rejects wallet and node settings, and no canonical or legacy aliases are accepted. |
| `ZINDER_COMPAT__REORG_WINDOW_BLOCKS` | zinder-compat-lightwalletd | Optional | `compat.reorg_window_blocks` | Exact canonical replacement-depth identity expected by compatibility. Must be greater than zero and match the canonical writer. Defaults to 1000. |
| `ZINDER_COMPAT__SHIELDED_HISTORY__DIRECTORY` | zinder-compat-lightwalletd | Optional | `compat.shielded_history.directory` | Follow the local publisher's checkpoint and signed catalog. Mutually exclusive with manifest. |
| `ZINDER_COMPAT__SHIELDED_HISTORY__MANIFEST` | zinder-compat-lightwalletd | Optional | `compat.shielded_history.manifest` | Admit an existing signed catalog before serving. Mutually exclusive with directory. |
| `ZINDER_COMPAT__SHIELDED_HISTORY__PUBLISHER_KEY` | zinder-compat-lightwalletd | When `compat.shielded_history is configured` | `compat.shielded_history.publisher_key` | File containing the trusted publisher's 32 raw Ed25519 public key bytes. |
| `ZINDER_COMPAT__SHIELDED_HISTORY__REQUIRE_FS_VERITY` | zinder-compat-lightwalletd | Optional | `compat.shielded_history.require_fs_verity` | Require Linux fs-verity on every segment. Defaults to false. See [signed settled history](lightwalletd-compatibility.md#signed-settled-history). |
| `ZINDER_COMPAT__PAIR_CONVERGENCE_ATTEMPTS` | zinder-compat-lightwalletd | Optional | `compat.pair_convergence_attempts` | Maximum bounded attempts to converge and admit compatibility's wallet-serving pair or compact-serving canonical secondary. Must be in 1..=64; defaults to 12. |
| `ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS` | zinder-compat-lightwalletd | Optional | `compat.max_concurrent_block_range_streams` | Maximum concurrent `GetBlockRange` and `GetBlockRangeNullifiers` streams the compatibility server serves. A valid request past the limit waits up to 10 seconds for a slot, then fails with `UNAVAILABLE`. Must be in 1..=65536; defaults to 100. |
| `ZINDER_COMPAT__TREE_STATE_REPLAY_WAIT_MS` | zinder-compat-lightwalletd | Optional | `compat.tree_state_replay_wait_ms` | Milliseconds a lightwalletd `GetTreeState` rebuild waits for a rebuild slot before it is refused with `UNAVAILABLE`. Wallet serving only; compact-blocks serving rejects it. Must be in 0..=5000; 0 refuses at once; defaults to 2000. |
| `ZINDER_COMPAT__TREE_STATE_REPLAY_WAITERS_PER_SLOT` | zinder-compat-lightwalletd | Optional | `compat.tree_state_replay_waiters_per_slot` | lightwalletd `GetTreeState` rebuilds that may wait for each rebuild slot; a rebuild that finds every waiting place taken fails at once with `UNAVAILABLE`. Wallet serving only; compact-blocks serving rejects it. Must be in 1..=256; defaults to 64. |
| `ZINDER_COMPAT__STALE_TIP_LIMIT_MS` | zinder-compat-lightwalletd | Optional | `compat.stale_tip_limit_ms` | Milliseconds the served tip may age, counted from its block time or from when the process first served it, whichever is earlier, while the writer cannot vouch for it because its node is unreachable, behind the network, or ahead of the writer, before lightwalletd `GetLatestBlock` and `GetLightdInfo` fail with `UNAVAILABLE`, so wallets stop building transactions on a stale tip. A tip the node still reports is never refused. 0 disables it; otherwise at least 300000. Defaults to 1500000 (25 minutes). |
| `ZINDER_COMPAT__PING_VERY_INSECURE` | zinder-compat-lightwalletd | Optional | `compat.ping_very_insecure` | Answers the lightwalletd `Ping` load-testing RPC, as `lightwalletd --ping-very-insecure` does. Each call holds a server task for as long as the client asks, so enable it only on a server you are load testing. Defaults to false, which refuses `Ping` with `FAILED_PRECONDITION`. |
| `ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS` | zinder-compat-lightwalletd | Optional | `compat.trust_proxy_client_address` | Counts each lightwalletd-compatible request against the client connection that a reverse proxy names in the `x-zinder-client-address` header, instead of the proxy's own connection, when the receive limit picks which client to refuse. Set it only when clients reach the listener solely through a proxy that replaces the header. Defaults to false. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__BLOCK_CACHE_BYTES` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.block_cache_bytes` | Materialized-view store RocksDB block cache budget in bytes. Defaults to 268435456 for writers and 67108864 for readers. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_WAL_BYTES` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.max_wal_bytes` | Materialized-view store RocksDB live WAL ceiling in bytes. Defaults to 268435456 for writers and 16777216 for readers. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_OPEN_FILES` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.max_open_files` | Materialized-view store RocksDB open SST file cap. Defaults to 512 for writers and 64 for readers. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__WRITE_BUFFER_BYTES` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.write_buffer_bytes` | Materialized-view store per-column-family RocksDB write buffer size. Defaults to 16777216 for writers and 4194304 for readers. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_WRITE_BUFFER_COUNT` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.max_write_buffer_count` | Materialized-view store per-column-family mutable plus immutable RocksDB write buffer count. Defaults to 4 for writers and 2 for readers. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_BACKGROUND_JOBS` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.max_background_jobs` | Materialized-view store primary-writer RocksDB background job cap shared by flush and compaction work. Defaults to 2 and is not applied to secondary opens, including `zinder-explorer`. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MEMTABLE_BUDGET_BYTES` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.memtable_budget_bytes` | Materialized-view store total RocksDB memtable budget across column families. Defaults to 536870912 for writers and 16777216 for readers. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__STATISTICS_LEVEL` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.statistics_level` | Materialized-view store RocksDB statistics collection gate: `off`, `tickers`, or `full`. Defaults to `tickers`. |
| `ZINDER_INGEST__SOURCE` | zinder-ingest | Required | `ingest.source` | Source-adapter selector. Lives on `[ingest]` (not `[node]`) because the choice is a writer-private implementation decision: `[node]` describes the upstream node itself, `[ingest].source` describes which adapter ingest uses to talk to it. See [ADR-0006](../adrs/0006-node-source-boundary.md). |
| `ZINDER_INGEST__EXPLORER_VIEWS` | zinder-ingest | Optional | `ingest.explorer_views` | Boolean request for the complete bundled Explorer materialized-view workload. Defaults to `false` for every coverage. `true` requires genesis-complete canonical history or the authenticated `checkpoint_height = 0`; checkpoint heights above zero are rejected before storage mutation. The request appears in `--print-config`; admission evidence appears only after the Explorer store opens successfully. |
| `ZINDER_INGEST__EXPLORER_VIEWS_MIN_FREE_BYTES` | zinder-ingest | Optional | `ingest.explorer_views_min_free_bytes` | Available bytes on the volume holding `storage.path` below which Explorer view replay, the transparent-address ranking build, and historical view backfills pause, so the views cannot fill the volume canonical ingest and wallet serving need. Replay resumes once at least a tenth more than this is available. Defaults to 10737418240 (10 GiB); `0` disables the guard. |
| `ZINDER_STORAGE__RAW_BLOB_POLICY` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `storage.raw_blob_policy` | Immutable raw-blob retention contract: `none`, `transactions`, or `all`. Defaults to `none` for explicit coverage so canonical indexing does not write raw block or transaction blobs unless a deployment explicitly needs raw export. Wallet-serving coverage defaults to `transactions` and rejects `none`, because native and lightwalletd-compatible transaction and transparent-history methods require retained bytes. Projector, query, and compatibility readers use the same value as an exact admission expectation; Zallet full-block serving requires `all`. The first canonical commit fixes historical coverage; changing a non-empty store requires a rebuild and blue-green cutover. |
| `ZINDER_INGEST__REORG_WINDOW_BLOCKS` | zinder-ingest | Optional | `ingest.reorg_window_blocks` | Chain-truth invariant: how deep the live reorg window extends. Bounds settlement, classifier default, and replacement traversal. Must be greater than zero. Defaults to 1000, Zebra's maximum finalization depth. A larger value raises an existing store's window in place; a smaller one is refused. |
| `ZINDER_INGEST__MEMPOOL__MAX_TRANSACTION_COUNT` | zinder-ingest | Optional | `ingest.mempool.max_transaction_count` | Maximum number of transactions admitted into one coherent live mempool. Exceeding the bound withdraws the serving generation and retries source hydration. Must be greater than zero. Defaults to 8000. |
| `ZINDER_INGEST__MEMPOOL__MAX_TOTAL_RAW_TRANSACTION_BYTES` | zinder-ingest | Optional | `ingest.mempool.max_total_raw_transaction_bytes` | Maximum cumulative raw transaction bytes admitted into one coherent live mempool. Exceeding the bound withdraws the serving generation and retries source hydration. Must be greater than zero. Defaults to 80000000. |
| `ZINDER_INGEST__MEMPOOL__RECONCILIATION_BATCH_TARGET_RAW_TRANSACTION_BYTES` | zinder-ingest | Optional | `ingest.mempool.reconciliation_batch_target_raw_transaction_bytes` | Target raw transaction bytes for one durable mempool reconciliation write. A single protocol-valid transaction above the target is written alone so reconciliation can make progress. Must be greater than zero. Defaults to 16000000. |
| `ZINDER_PROJECTOR__REORG_WINDOW_BLOCKS` | zinder-projector | Optional | `projector.reorg_window_blocks` | Wallet undo suffix depth and expected canonical replacement policy. Must match the canonical writer. Defaults to 1000. |
| `ZINDER_PROJECTOR__BUILD_OWNER_HEX` | zinder-projector | Required | `projector.build_owner_hex` | Stable 16-byte wallet-build lease owner encoded as exactly 32 hexadecimal characters. A restart with the same value reclaims its own construction leases immediately. Use a distinct value for each concurrently provisioned lane. |
| `ZINDER_PROJECTOR__LEASE_DURATION_SECONDS` | zinder-projector | Required | `projector.lease_duration_seconds` | Wallet-build and canonical-retention lease duration in seconds. Must be at least 14400 so a durable construction phase cannot outlive its lease. |
| `ZINDER_PROJECTOR__FENCE_CONVERGENCE_TIMEOUT_SECONDS` | zinder-projector | Optional | `projector.fence_convergence_timeout_seconds` | Deadline for each attempt of the projector's canonical secondary to reach the writer's fence. A projector with a wallet open, including one that resumes after a restart, stops waiting once its secondary holds a block the wallet has not applied, even while the writer is ahead; a fresh wallet build waits for the writer to attest the secondary's exact fence. An attempt that passes the deadline logs `canonical_secondary_behind_writer`, reports `replica_lagging`, or `writer_status_unavailable` when the writer did not answer, until the secondary gives the wallet a block to apply, and starts another attempt; the projector does not exit. A same-sequence mismatch or a secondary ahead of the writer fails immediately. Readiness while the wallet trails is described in [Readiness](readiness.md#projector). Must be greater than zero. Defaults to 30. |
| `ZINDER_PROJECTOR__BUILD__MAX_OUTPOINT_SORT_MEMORY_BYTES` | zinder-projector | Optional | `projector.build.max_outpoint_sort_memory_bytes` | Memory ceiling for the wallet builder's outpoint sorter. Defaults to 4294967296. |
| `ZINDER_PROJECTOR__BUILD__MAX_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER` | zinder-projector | Optional | `projector.build.max_secondary_sort_memory_bytes_per_sorter` | Memory ceiling for each wallet secondary-index sorter. Defaults to 1073741824. |
| `ZINDER_PROJECTOR__BUILD__MAX_TEMPORARY_FILE_BYTES_PER_SORTER` | zinder-projector | Optional | `projector.build.max_temporary_file_bytes_per_sorter` | Temporary spill-file ceiling for each wallet builder sorter. Defaults to 206158430208. |
| `ZINDER_PROJECTOR__BUILD__SST_TARGET_LOGICAL_BYTES` | zinder-projector | Optional | `projector.build.sst_target_logical_bytes` | Target logical payload per externally built wallet SST file. Defaults to 134217728. |
| `ZINDER_PROJECTOR__BUILD__MAX_ACCOUNTED_REORG_UNDO_BYTES` | zinder-projector | Optional | `projector.build.max_accounted_reorg_undo_bytes` | Maximum logical wallet undo bytes admitted during fixed-tip construction. Defaults to 536870912. |
| `ZINDER_PROJECTOR__FOLLOW__MAX_TRANSITION_LOGICAL_BYTES` | zinder-projector | Optional | `projector.follow.max_transition_logical_bytes` | Maximum logical bytes one atomic wallet following transition may plan. Each distinct wallet row the transition touches counts twice, as its key and final value in the plan and again in the write batch, and the control record counts once. Must be greater than zero and at most 1073741824. Defaults to 536870912. A transition that needs more leaves the wallet unchanged and parks the projector, not ready, with `wallet_transition_limit_exceeded` until it restarts with a larger value; see [Wallet transition exceeds its logical-byte limit](../runbooks/deploying-single-volume.md#wallet-transition-exceeds-its-logical-byte-limit). |
| `ZINDER_INGEST__PHASE_CLASSIFICATION__CATCHUP_THRESHOLD_BLOCKS` | zinder-ingest | Optional | `ingest.phase_classification.catchup_threshold_blocks` | Gap (in blocks) above which `zinder-ingest probe` reports `BulkCatchup` instead of `FollowingTip`. Defaults to `ingest.reorg_window_blocks`. See [ADR-0009](../adrs/0009-ingest-lifecycle-and-construction.md). |
| `ZINDER_INGEST__CONSTRUCTION__SOURCE_SEGMENT_MAX_BLOCKS` | zinder-ingest | Optional | `ingest.construction.source_segment_max_blocks` | Diagnostic override for the hard ceiling on connected blocks requested from the source in one segment. The resource-resolved default is 64. Values above 1000 are rejected at startup, because each block is one JSON-RPC batch entry and Zakura refuses batches above 1,000 entries. |
| `ZINDER_INGEST__CONSTRUCTION__SOURCE_SEGMENT_TARGET_RESPONSE_BYTES` | zinder-ingest | Optional | `ingest.construction.source_segment_target_response_bytes` | Diagnostic override for adaptive response sizing. The default is `min(node.max_response_bytes, 33554432)`; with `[node_fallbacks.N]` endpoints, the smallest `max_response_bytes` in the list replaces `node.max_response_bytes`. |
| `ZINDER_INGEST__CONSTRUCTION__SOURCE_FETCH_MAX_IN_FLIGHT_REQUESTS` | zinder-ingest | Optional | `ingest.construction.source_fetch_max_in_flight_requests` | Maximum concurrent source segment requests. Defaults to 12. |
| `ZINDER_INGEST__CONSTRUCTION__SOURCE_FETCH_MAX_IN_FLIGHT_BYTES` | zinder-ingest | Optional | `ingest.construction.source_fetch_max_in_flight_bytes` | Diagnostic override for predicted active source responses plus measured completed reassembly. The default is `max(node.max_response_bytes, clamp(container_memory / 64, 134217728, 402653184))`; with `[node_fallbacks.N]` endpoints, the largest `max_response_bytes` in the list replaces `node.max_response_bytes`, and an override must be at least that large. |
| `ZINDER_INGEST__CONSTRUCTION__BLOCK_PREPARE_CONCURRENCY` | zinder-ingest | Optional | `ingest.construction.block_prepare_concurrency` | Diagnostic override for parallel canonical block-prepare slots. The default is `min(available_parallelism(), 16)`. |
| `ZINDER_INGEST__CONSTRUCTION__BLOCK_PREPARE_MEMORY_WATERMARK_BYTES` | zinder-ingest | Optional | `ingest.construction.block_prepare_memory_watermark_bytes` | Diagnostic override for the prepare and resident-handoff admission watermark. The default is `clamp(container_memory / 64, 134217728, 536870912)`. |
| `ZINDER_INGEST__CONSTRUCTION__TREE_HASHING_WORKERS` | zinder-ingest | Optional | `ingest.construction.tree_hashing_workers` | Threads that hash note-commitment trees for the ordered construction loader. Defaults to the detected logical CPU count, at most 8, when fresh construction stores checkpoints beyond the 100-block cadence and build tip, and to 1 otherwise or when the CPU count cannot be detected. With 1, hashing runs on the loader thread. The automatic count reserves no CPU for a node or projector on the same host; in one measured Mainnet build, 8 workers sharing 8 cores with a node pinned to 2 of them reached READY in 1 h 40 min 30 s. An explicit value may exceed 8. A higher value hashes each block's pools and large commitment batches in parallel, and derives the checkpoints of up to twice that many blocks, at most 64, while the loader writes earlier blocks. The stored trees, checkpoints, and subtree roots do not change. Must be greater than zero. |
| `ZINDER_INGEST__FOLLOW__POLL_INTERVAL_MS` | zinder-ingest | Optional | `ingest.follow.poll_interval_ms` | Canonical follower poll cadence in milliseconds. Must be greater than zero. Defaults to 1000. |
| `ZINDER_INGEST__FOLLOW__LAG_THRESHOLD_BLOCKS` | zinder-ingest | Optional | `ingest.follow.lag_threshold_blocks` | Block lag at which the canonical follower reports `cause=syncing`. Defaults to 1. |
| `ZINDER_INGEST__FOLLOW__MAX_REPLACEMENT_BYTES` | zinder-ingest | Optional | `ingest.follow.max_replacement_bytes` | Maximum estimated bytes one canonical replacement may hold before its atomic commit: fetched source blocks not yet prepared plus prepared blocks. A deeper or heavier replacement fails with `ReplacementBytesExceeded` before any write; raise the limit to follow it. Defaults to 2147483648. |
| `ZINDER_INGEST__TREE_STATE_DENSIFICATION__ENABLED` | zinder-ingest | Optional | `ingest.tree_state_densification.enabled` | Enables tree-state densification. The background pass requires `ingest_control.listen_addr`. Defaults to `true`. |
| `ZINDER_INGEST__TREE_STATE_DENSIFICATION__DURING_CONSTRUCTION` | zinder-ingest | Optional | `ingest.tree_state_densification.during_construction` | Defaults to `true` for `wallet-serving` coverage with Explorer views off, and to `false` otherwise. When `true` with densification enabled, fresh construction applies the densification selection before READY, which with `every-block` stores a checkpoint at every height. When `false`, it stores checkpoints every 100 blocks and at the build tip. In the measured Mainnet runs, with 6 hashing workers on 8 physical cores and one thread per core, `every-block` and 256-commitment-budget construction reached READY within three hours; hosts with fewer logical CPUs take longer, as the bulk-catchup tuning runbook describes. Changing this setting leaves stored checkpoints in place. |
| `ZINDER_INGEST__TREE_STATE_DENSIFICATION__SELECTION` | zinder-ingest | Optional | `ingest.tree_state_densification.selection` | Which checkpoints densification adds. The default `every-block` fills every height. The `commitment-budget` selection stores a checkpoint when at least `commitment_budget` note commitments have accumulated since the previous checkpoint. Setting only the budget selects this policy. Changing selection retains existing rows. |
| `ZINDER_INGEST__TREE_STATE_DENSIFICATION__COMMITMENT_BUDGET` | zinder-ingest | Optional | `ingest.tree_state_densification.commitment_budget` | Note commitments, over every pool, between two stored tree-state checkpoints under the `commitment-budget` selection, which setting it selects when `selection` is unset. Must be greater than zero and is refused with `selection = "every-block"`. Defaults to 1024. |
| `ZINDER_INGEST__TREE_STATE_DENSIFICATION__WORKERS` | zinder-ingest | Optional | `ingest.tree_state_densification.workers` | Settled 100-block strides densified at once, each on one CPU. Must be greater than zero. Defaults to 2. |
| `ZINDER_INGEST__TREE_STATE_DENSIFICATION__MAX_BLOCKS_PER_SECOND` | zinder-ingest | Optional | `ingest.tree_state_densification.max_blocks_per_second` | Average ceiling on stored blocks the tree-state densification pass replays per second, with a scan charged one block for every two reads it makes, counting each checkpoint row it visits and each compact block it reads as one read. Each block costs about two reads and adds at most one row, so this bounds the pass's reads and writes. Must be greater than zero. Defaults to 500. |
| `ZINDER_INGEST__TREE_STATE_VERIFICATION__ENABLED` | zinder-ingest | Optional | `ingest.tree_state_verification.enabled` | Whether ingest verifies its stored tree-state checkpoints in the background, in repeated cycles over every settled row: each row must derive the roots stored in it, carry a frontier for exactly the pools active at its height, and carry the block time of the canonical block at that height. A row that fails parks ingest readiness with `canonical_store_damaged` and makes the store's wallet-serving readers refuse traffic, until the store is rebuilt. With the pass disabled, damage it recorded before stays reported. Defaults to `true`. |
| `ZINDER_INGEST__TREE_STATE_VERIFICATION__MAX_ROWS_PER_SECOND` | zinder-ingest | Optional | `ingest.tree_state_verification.max_rows_per_second` | Average ceiling on stored tree-state checkpoint rows the verification pass checks per second, which sets both its cost and how long one cycle over settled history takes. A row with three pools costs about 4.3 ms of CPU, so the default holds the pass near a tenth of one CPU and verifies about 2.2 million rows a day. Must be greater than zero. Defaults to 25. |
| `ZINDER_INGEST__RUN_OVERRIDES__TARGET_HEIGHT` | zinder-ingest | Optional | `ingest.run_overrides.target_height` | One-shot stop-at modifier; the loop exits 0 after committing this height. |
| `ZINDER_INGEST__RUN_OVERRIDES__CHECKPOINT_HEIGHT` | zinder-ingest | Optional | `ingest.run_overrides.checkpoint_height` | Construct an empty store from an upstream-supplied checkpoint at this height. |
| `ZINDER_INGEST__RUN_OVERRIDES__COVERAGE` | zinder-ingest | Optional | `ingest.run_overrides.coverage` | Ingest coverage mode: `"explicit"` or `"wallet-serving"`. Defaults to `"explicit"`. |
| `ZINDER_RETENTION__CHAIN_EVENT_RETENTION_HOURS` | zinder-ingest | Optional | `retention.chain_event_retention_hours` | Chain-event retention window in hours, enforced by `zinder-ingest`. Defaults to 168 (7 days). `0` disables eviction. |
| `ZINDER_RETENTION__CHAIN_EVENT_RETENTION_CHECK_INTERVAL_MS` | zinder-ingest | Optional | `retention.chain_event_retention_check_interval_ms` | Chain-event retention sweep cadence in milliseconds. Must be greater than zero. Defaults to 60000 (one minute). |
| `ZINDER_RETENTION__CURSOR_AT_RISK_WARNING_HOURS` | zinder-ingest | Optional | `retention.cursor_at_risk_warning_hours` | Cursor-at-risk warning lead time in hours. Must be ≤ `retention.chain_event_retention_hours`. Defaults to 24. |
| `ZINDER_RETENTION__MEMPOOL_MINED_RETENTION_MINUTES` | zinder-ingest | Optional | `retention.mempool_mined_retention_minutes` | Mined-mempool retention window in minutes, enforced by `zinder-ingest`. Defaults to 60. `0` disables retention. |
| `ZINDER_RETENTION__MEMPOOL_INVALIDATED_RETENTION_HOURS` | zinder-ingest | Optional | `retention.mempool_invalidated_retention_hours` | Invalidated-mempool retention window in hours, enforced by `zinder-ingest`. Defaults to 24. `0` disables retention. |
| `ZINDER_RETENTION__MEMPOOL_EVENT_RETENTION_CHECK_INTERVAL_MS` | zinder-ingest | Optional | `retention.mempool_event_retention_check_interval_ms` | Mempool-event retention sweep cadence in milliseconds. Must be greater than zero. Defaults to 30000. |
| `ZINDER_RETENTION__MEMPOOL_EVENT_RETENTION_MAX_EVENTS_PER_STEP` | zinder-ingest | Optional | `retention.mempool_event_retention_max_events_per_step` | Maximum event rows examined by one bounded mempool-retention step. Must be greater than zero. Defaults to 1024. |
| `ZINDER_RETENTION__MEMPOOL_EVENT_RETENTION_MAX_ENCODED_BYTES_PER_STEP` | zinder-ingest | Optional | `retention.mempool_event_retention_max_encoded_bytes_per_step` | Target maximum encoded event bytes examined by one bounded mempool-retention step. The first row may exceed the target to guarantee progress. Must be greater than zero. Defaults to 16000000. |
| `ZINDER_EXPLORER__BEARER_TOKEN_PATH` | zinder-explorer | Optional | `explorer.bearer_token_path` | Path to the shared-secret bearer token the ExplorerQuery endpoint enforces on cross-service explorer-plane reads (ADR-0011). |
| `ZINDER_EXPLORER__LISTEN_ADDR` | zinder-explorer | Optional | `explorer.listen_addr` | Listen address for the ExplorerQuery gRPC endpoint. Defaults to 127.0.0.1:9068. |
| `ZINDER_EXPLORER__TRUST_PROXY_CLIENT_ADDRESS` | zinder-explorer | Optional | `explorer.trust_proxy_client_address` | Counts each ExplorerQuery request against the client connection that a reverse proxy names in the `x-zinder-client-address` header, instead of the proxy's own connection, when the receive limit picks which client to refuse. Set it only when clients reach the listener solely through a proxy that replaces the header. Defaults to false. |
| `ZINDER_EXPLORER__WALLET_QUERY_ENDPOINT` | zinder-explorer | Optional | `explorer.wallet_query_endpoint` | WalletQuery gRPC endpoint backing the explorer's wallet-composed reads (transaction detail, block views, search, mempool activity). Empty/unset disables the explorer capabilities that compose canonical wallet reads. |
| `ZINDER_CIPHERSCAN__LISTEN_ADDR` | zinder-compat-cipherscan | Optional | `cipherscan.listen_addr` | Listen address for the Cipherscan-compatible REST endpoint. Defaults to 127.0.0.1:9070. A public or unspecified address needs `ZINDER_SECURITY__ALLOW_PUBLIC_BIND=true`. |
| `ZINDER_CIPHERSCAN__EXPLORER_QUERY_ENDPOINT` | zinder-compat-cipherscan | Optional | `cipherscan.explorer_query_endpoint` | ExplorerQuery gRPC endpoint the adapter reads explorer views from. Defaults to http://127.0.0.1:9068. |
| `ZINDER_CIPHERSCAN__WALLET_QUERY_ENDPOINT` | zinder-compat-cipherscan | Optional | `cipherscan.wallet_query_endpoint` | WalletQuery gRPC endpoint the adapter reads chain and transaction data from. Defaults to http://127.0.0.1:9102. |
| `ZINDER_CIPHERSCAN__CURRENT_PRICE_ENDPOINT` | zinder-compat-cipherscan | Optional | `cipherscan.current_price_endpoint` | HTTP or HTTPS URL of the external market-price provider queried for the current ZEC price. Defaults to the CoinGecko simple-price URL. |
| `ZINDER_CIPHERSCAN__HISTORICAL_PRICE_ENDPOINT_TEMPLATE` | zinder-compat-cipherscan | Optional | `cipherscan.historical_price_endpoint_template` | URL template for historical market-price lookups; `{date}` is replaced with the requested date. Defaults to the CoinGecko history URL. |
| `ZINDER_CIPHERSCAN__BEARER_TOKEN_PATH` | zinder-compat-cipherscan | Optional | `cipherscan.bearer_token_path` | Path to the shared-secret bearer token the adapter presents to both the ExplorerQuery and WalletQuery endpoints (ADR-0011). Unset sends no token. |
<!-- env-var-table:public-interfaces:end -->

## Listener and request limits

These limits are compiled in. No setting changes them unless the entry names
one. A refusal from the transport carries no `ErrorInfo` reason; a refusal
from a handler carries one of the reasons in
[Error vocabulary](error-vocabulary.md).

### Public gRPC listeners

`zinder-query`, `zinder-compat-lightwalletd`, and `zinder-explorer` share one
transport policy.

| Limit | Value | Client sees |
| --- | --- | --- |
| Request message size, including gRPC reflection | 2 MiB | `OUT_OF_RANGE` |
| Concurrent streams on one HTTP/2 connection | 200 | Queued stream or a new connection, depending on the client |
| Unary and server-streaming requests still arriving, per process | 32 | `UNAVAILABLE` |
| Client-streaming and bidirectional calls open, per process | 32 | `UNAVAILABLE` |
| Time for a request to arrive after its stream opens | 20 s | `DEADLINE_EXCEEDED` |
| Time a client stream may go without sending a whole message | 20 s | `DEADLINE_EXCEEDED` |
| Time a response may wait for its client to read it | 60 s | `DEADLINE_EXCEEDED` after the data already sent, if it reads again |
| HTTP/2 keepalive | Ping after 60 s without a frame; close if unanswered for 20 s | Connection closed |
| Open `grpc.health.v1.Health/Watch` streams, per process (`zinder-query`, `zinder-compat-lightwalletd`, and `zinder-explorer`) | 1,024 | `RESOURCE_EXHAUSTED` |

A unary or server-streaming call holds its receive slot only until its request
has arrived. Long response streams such as `ChainEvents`,
`CompactBlocksInRange`, and `GetBlockRange` therefore hold no slot. When a
pool is full, a request from a client that holds fewer slots takes the
oldest slot of the client that holds the most. The request that loses its
slot fails with `UNAVAILABLE`. A request from a client that holds no slot is
therefore always admitted, and a client is refused only while it holds at
least as many slots as every other client. Retry these refusals with backoff.

A client is the connection a request arrives on. A proxy reaches Zinder over
a few pooled connections, so behind one every client looks like the same few.
Set `query.trust_proxy_client_address`, `compat.trust_proxy_client_address`,
or `explorer.trust_proxy_client_address` to `true` when the listener is
reachable only through a proxy that sets the `x-zinder-client-address`
request header to the client connection's IP address and port. Zinder then
counts each request against that connection. A request that carries the
header more than once, as a comma-separated list, or as anything but one IP
address and port, or that carries none, counts against the proxy's own
connection instead, and `zinder_grpc_proxy_client_address_fallbacks_total`
counts it. A loopback `grpc.health.v1.Health/Check` with no header uses its
own connection for the receive limit and leaves the fallback counter unchanged.
With the setting off, the default, Zinder ignores the header.
[Exposing over TLS](../runbooks/exposing-over-tls.md#keep-wallets-apart-in-the-receive-limit)
lists the proxy settings this depends on.

These limits count all clients together, and a client that opens many
connections counts as many clients. Limit connections per client in the
proxy.

A response that the client stops reading, by withholding HTTP/2 flow-control
window, is abandoned after 60 seconds. The listener drops the call's handler
and the response data it has not yet handed to HTTP/2, so the call starts no
further work. A read already in flight finishes first, and a `GetBlockRange`
stream returns its range-stream slot once that read is done. A client that reads again
receives the data HTTP/2 already holds, then `DEADLINE_EXCEEDED`. A response
that is waiting for the server to produce its next message is not affected.

Abandoning a response does not release what HTTP/2 holds for it: up to about
528 KiB of response data (the 400 KiB send buffer, one piece queued past it,
and one waiting for flow-control window) and the stream's place among its
connection's 200 streams. These stay until the client reads them, resets the
stream, or closes the connection. A client that keeps answering keepalive
pings keeps its connection open, so one connection can keep about 103 MiB
this way with no time limit, and the proxy's per-client connection limit is
the only bound across the listener. A `GetBlockRange` response on the
lightwalletd listener hands HTTP/2 slices of its stored blocks instead of
copies, so a stalled range stream can keep up to about 2 MiB alive and one
connection about 406 MiB
([lightwalletd compatibility](lightwalletd-compatibility.md)).
[ADR-0007](../adrs/0007-node-failure-recovery-and-transport-policy.md)
records this policy.

### Request list bounds

The server counts the elements of every repeated request field before it
decodes the request. Without that bound, one 2 MiB request could decode into
tens of MiB of empty elements. The count covers every frame of the request and
the lists inside nested messages, and allocates nothing.

A request over a bound fails with `INVALID_ARGUMENT` and the message
`<Method> request field <field> carries <count> elements; at most <bound> are
accepted`. `INVALID_ARGUMENT` fits because the same request can never
succeed.

A field that its handler also caps gets a bound of twice the cap. A request
just over the cap therefore still reaches the handler and gets its typed
reason. An enum list that no handler caps gets 1,000
(`zinder_runtime::MAX_UNCAPPED_ENUM_LIST_ELEMENTS`).

| Service | Method | Field | Bound | Handler cap |
| --- | --- | --- | --- | --- |
| `WalletQuery` | `ChainEvents` | `address_filter` | 512 | 256 |
| `WalletQuery` | `TransparentOutpointSpendStatus` | `outpoints` | 2,048 | 1,024 |
| `ExplorerQuery` | `ValuePoolFlowHistory` | `filter.directions`, `filter.pools` | 1,000 each | none |
| `ExplorerQuery` | `ValuePoolFlowEventsInRange` | `directions`, `pools` | 1,000 each | none |
| `ExplorerQuery` | `ValuePoolFlowSummary`, `ValuePoolFlowRoundedAmountSummary` | `pools` | 1,000 | none |
| `ExplorerQuery` | `ValuePoolFlowAmountThresholdSummary` | `pools` | 1,000 | none |
| `ExplorerQuery` | `ValuePoolFlowAmountThresholdSummary` | `minimum_amounts_zat` | 64 | 32 |
| `ExplorerQuery` | `TransactionHistory` | `filter.privacy_shapes`, `filter.contains_any_protocol` | 1,000 each | none |

[Lightwalletd compatibility](lightwalletd-compatibility.md) lists the
`CompactTxStreamer` bounds. Each adapter keeps its table in
`WALLET_QUERY_FIELD_LIMITS`, `EXPLORER_QUERY_FIELD_LIMITS`, or
`COMPACT_TX_STREAMER_FIELD_LIMITS`. A test compares each table with the
service descriptor, so a new repeated request field fails the tests until it
has a bound.

### Per-request caps

| Request | Cap | Refusal |
| --- | --- | --- |
| `WalletQuery` compact-block or full-block range | 1,000 blocks | `BLOCK_RANGE_TOO_LARGE` |
| `WalletQuery.SubtreeRoots` `max_entries` | 1,024 | `SUBTREE_ROOT_RANGE_TOO_LARGE` |
| `WalletQuery.TransparentOutpointSpendStatus` | 1 to 1,024 outpoints | `TRANSPARENT_OUTPOINT_COUNT_OUT_OF_RANGE` |
| `WalletQuery.ChainEvents` `address_filter` | 256 addresses | `INVALID_ADDRESS` |
| Transparent balance, lightwalletd only | 256 distinct addresses | `TRANSPARENT_BALANCE_ADDRESS_COUNT_EXCEEDED`; an empty list is a plain `INVALID_ARGUMENT` |
| Raw transaction for broadcast | 2,000,000 bytes | `BROADCAST_TRANSACTION_TOO_LARGE` |
| Transaction bytes being broadcast at once, per process | 16 MiB | `BROADCAST_BUDGET_EXHAUSTED` |
| Open `WalletQuery.CompactBlocksInRange` streams, per process | 100 | `COMPACT_BLOCK_STREAM_LIMIT_REACHED` |
| Tree states rebuilt at once for heights without a stored checkpoint | One per CPU; a rebuild waits for a slot up to 2 s by default (`query.tree_state_replay_wait_ms` in `zinder-query`, `compat.tree_state_replay_wait_ms` in wallet-serving `zinder-compat-lightwalletd`), with at most 64 waiting per slot by default (`query.tree_state_replay_waiters_per_slot` and `compat.tree_state_replay_waiters_per_slot`, 1 to 256) and any further rebuild refused at once | `TREE_STATE_REPLAY_LIMIT_REACHED` natively; `UNAVAILABLE` with no reason in lightwalletd `GetTreeState` |

The broadcast budget covers native `BroadcastTransaction` and lightwalletd
`SendTransaction`. A broadcast holds its transaction's bytes until the node
answers or the broadcast times out. One that does not fit is refused at once,
without reaching any node. The budget always fits one maximum-size
transaction.

`zinder-compat-lightwalletd` adds `compat.max_concurrent_block_range_streams`,
`compat.tree_state_replay_wait_ms`, `compat.tree_state_replay_waiters_per_slot`,
`compat.stale_tip_limit_ms`, and `compat.ping_very_insecure`, which
[Lightwalletd compatibility](lightwalletd-compatibility.md) describes with the
methods they govern.

### HTTP listeners

The operational endpoint of every runtime (`/healthz`, `/readyz`, `/metrics`)
and the Cipherscan adapter use one hardened HTTP/1 listener.

| Limit | Cipherscan adapter | Operational endpoint |
| --- | --- | --- |
| Time to receive a complete request head, and idle keep-alive | 10 s | 10 s |
| Time to receive a request body | 30 s | 30 s |
| Time a write may stall behind a client that is not reading | 30 s | 30 s |
| Open connections | 2,048 | 64 |
| Request body size | 2 MiB | 4 KiB |

The operational endpoint must stay private. `ops.listen_addr` sets its
address, and an empty value disables it.

### Dials between Zinder runtimes

A runtime that calls another Zinder runtime, such as a reader dialing
`ingest_control.addr` or the Cipherscan adapter dialing
`cipherscan.explorer_query_endpoint`, gives each dial 5 seconds. The dial must
finish the name lookup, the TCP connect, and the TLS handshake and receive the
peer's whole initial HTTP/2 `SETTINGS` frame within that time. A peer that
opens with any other frame fails the dial at once. The dial sends no request,
so a TLS-terminating proxy in front of the peer needs no route or credential
for it. A reconnect that the open channel makes on its own gets the same 5
seconds, from the name lookup until the peer's `SETTINGS` frame is complete, so
a call that waits on it fails within that time when the peer stalls.

| Caller | When a dial runs out of time |
| --- | --- |
| `zinder-projector` and `zinder-compat-cipherscan` at startup | The runtime logs `zinder_grpc_startup_dial_timed_out`, waits 1 second, and dials again until the peer answers or the runtime is stopped. A refused connection, a failed TLS handshake, or a peer that does not open with an HTTP/2 `SETTINGS` frame still ends startup. |
| `zinder-query` and `zinder-compat-lightwalletd` dialing `ingest_control.addr` at startup | Startup fails with the connect timeout. |
| Mempool reads in `zinder-compat-lightwalletd` and wallet reads in `zinder-explorer` | The request fails as unavailable, and the next request dials again. Requests that arrive during a dial wait for that dial. |

The bound is not configurable.
[ADR-0007](../adrs/0007-node-failure-recovery-and-transport-policy.md)
records this policy.
