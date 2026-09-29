# Public interfaces

This document is Zinder's vocabulary spine. Public Rust types, protocol fields,
configuration, errors, capability strings, metrics, and documentation use the
same domain names.

Names optimize for honesty first, then domain specificity, repository
vocabulary, and brevity. Do not add temporal labels such as `new`, `legacy`,
`next`, or `v2` to source identifiers. Version numbers belong only to persisted
and wire contracts that actually carry versions.

## Core vocabulary

| Name | Meaning |
| --- | --- |
| `NodeSource` | Typed upstream observations and block acquisition |
| `ChainEpoch` | One immutable visible best-chain state |
| `CanonicalEventFence` | Authenticated chain epoch, event sequence, visible tip, and sequence digest |
| `CanonicalBlockFacts` | Complete block-local semantic input for replay and materialized-view construction |
| `CanonicalBlockReplay` | Reversible storage envelope for canonical block facts |
| `CanonicalReader` | Immutable canonical side of an admitted serving pair |
| `WalletProjectionReader` | Immutable wallet side of an admitted serving pair |
| `WalletServingReadPair` | Canonical and wallet readers proven to describe one exact fence |
| `WalletServingQuery` | `WalletQueryApi` implementation over an atomically replaceable serving pair, shared by native and compatibility runtimes |
| `WalletServingPairPublisher` | Serving-runtime owner that catches up, admits, and publishes process-local serving pairs |
| `MaterializedViewConsumer` | Explorer materialized-view consumer that applies retained canonical events |
| `MaterializedViewStore` | Independent store for materialized-view rows, cursors, coverage, and schemas |
| `ChainEvent` | Durable canonical append or replacement transition |
| `MempoolEvent` | Typed live-pool transition: added, invalidated, or mined |
| `ChainSnapshot` | Borrowed `ChainIndex` view whose pinnable canonical reads all use one captured `ChainEpoch`; it contains no live mempool surface |
| `OwnedChainSnapshot` | Cloneable, `Arc`-backed form of `ChainSnapshot` for retained consumer chain views, with the same canonical-only epoch pin |
| `MempoolSnapshotView` | Bounded live-pool page with a mempool-resume cursor, canonical `ChainEpoch` fence, and matching certified source tip |

Use `canonical` for chain truth, `wallet projection` for wallet query state,
and `materialized view` for optional explorer aggregates. `fact` is appropriate
only for semantic source records such as `CanonicalBlockFacts`,
`TransactionPublicFacts`, or `TransparentSpendFact`. It is not a runtime,
topology, migration, or backend label.

## Runtime and module names

| Name | Role |
| --- | --- |
| `zinder-ingest` | Canonical writer and live mempool owner |
| `zinder-projector` | Wallet projection writer |
| `zinder-query` | Native `WalletQuery` gRPC server and wallet-serving reader |
| `zinder-compat-lightwalletd` | Lightwalletd protocol adapter and wallet-serving reader |
| `zinder-explorer` | Optional ExplorerQuery runtime |
| `zinder-compat-cipherscan` | Optional Cipherscan REST and WebSocket adapter |
| `zinder-materialized-views` | Explorer materialized-view SDK and store |
| `zinder-rocksdb-bulk-load` | RocksDB sorted-file bulk-load support |

Modules name the domain behavior they contain. Current examples are
`canonical_replay_storage`, `wallet_serving_pair`, `wallet_serving_query`,
`wallet_serving_pair_publisher`, `canonical_writer_control`,
`materialized_view_replay`, `canonical_block_context`, and `runtime_config`.

Avoid `utils`, `helpers`, `common`, `manager`, `handler`, `processor`, and
`service` unless the term is part of a protocol contract or the file genuinely
owns that framework boundary. A module named for an implementation technology
must contain a technology-specific boundary, as
`zinder-rocksdb-bulk-load` does.

## Chain-view vocabulary

`ChainView` is the protocol envelope for freshness and source identity.
`ChainEpoch` carries the visible and settled chain tips. Epoch identifiers begin
at 1 and increase by exactly one for each durable canonical commit; this keeps
the epoch id and canonical chain-event sequence in the same identity space.
Optional materialized-view
fields report independently queryable state.

| Field | Meaning |
| --- | --- |
| `visible_tip` | Best block visible in the admitted chain epoch |
| `settled_tip` | Reorg-window finality watermark for settlement-sensitive policy |
| `indexed_tip` | Highest block covered by the required materialized view |
| `upstream_tip` | Latest source-node observation |
| `materialized_views` | Materialized-view status and lag |

Do not substitute generic safety or finality labels for this field. `safe` is
ambiguous, and `finalized` collides with Zcash consensus finality. Use
`settled_tip` for the reorg-window boundary and a named consensus term when
describing an actual consensus rule.

`MempoolSnapshotView` keeps two independent monotonic positions. Its
`events_resume_cursor` resumes `MempoolEvents` without a delivery gap. Its
`chain_epoch.id` proves which canonical chain fence was captured before the
page read. Its `source_tip` must exactly equal that epoch's `visible_tip` by
height and hash; the server returns `UNAVAILABLE` instead of exposing an empty
or stale answer when the certified mempool generation differs. Epoch ids and
chain-event sequences share one identity space, so a tip-coherent consumer
restarts when it observes a larger chain-event sequence. The mempool cursor and
chain epoch do not substitute for each other; live mempool-event consumers
resume from the opaque cursor.

## Canonical writer vocabulary

The release writer uses these source types:

- `CanonicalWriterConfig` for the complete writer configuration;
- `CanonicalConstructionSettings` for bounded fresh construction;
- `CanonicalFollowSettings` and `CanonicalFollowConfig` for continuous
  following;
- `CanonicalRunOverrides` for one-run target and checkpoint inputs;
- `CanonicalWriterControlClient` for authenticated projector coordination; and
- `CanonicalRetentionLease` for the writer-owned retained-event authority.

`construction` and `follow` describe durable lifecycle modes. `bulk catchup`
remains valid for the artifact-oriented ingestion library and test harness, but
it is not the name of the release store or a backend.

## Wallet and materialized-view vocabulary

Wallet projection names begin with `Wallet` when the type belongs to wallet
state, source identity, rows, build leases, or serving. Explorer materialized-view
names begin with `MaterializedView` when they belong to the reusable
consumer and store framework.

`MaterializedViewPreset` selects a closed set of bundled materialized-view consumers.
The stable values are `wallet` and `explorer`. A preset does not select a
database engine, raw-byte retention policy, or release topology.

## Rust API shape

- Public operations accept and return domain types, not RocksDB handles, column
  families, SQL rows, or untyped byte maps.
- Writer and reader roles use different types. A read-only process cannot obtain
  a primary handle through a flag on a generic store.
- Fallible constructors validate immutable identity before returning a handle.
- Range requests use typed bounds and enforce explicit maximum sizes.
- Pagination cursors bind to the filters, order, network, and source fence that
  produced them.
- A request that combines canonical and projected data captures one
  `WalletServingReadPair` or equivalent snapshot at entry.
- `zinder-client::ChainSnapshot` is the borrowed consumer view over one
  captured `ChainEpoch`; `OwnedChainSnapshot` is the `Arc`-backed,
  cloneable form for adapters that retain a `'static` view. Both remove the
  epoch argument from pinnable canonical reads and always forward the captured
  epoch id.
- Snapshot views are canonical-only. Live mempool state, event subscriptions,
  broadcast, current address history, and current balance remain on their
  existing traits and are never presented as epoch-pinned state.
- Immutable network-upgrade activations live on `ChainIndex` itself, not on a
  snapshot. Remote clients preflight server identity, network, contract
  revision, and `wallet.read.network_upgrade_activations_v1` before discovery.
- The public, remote-first `zinder-client` surface enables the remote adapter
  by default and has no normal dependency on storage or RocksDB crates.
- Zinder serving runtimes compose service-internal reads through
  `WalletServingQuery` and an admitted `WalletServingReadPair`; they do not
  expose a storage-backed public client adapter.
- Transport metadata and errors cross the SDK boundary as client-owned
  `ServerInfo`, `Capability`, `CapabilityDescriptor`, and `ErrorReason` types.
  Unknown wire capability and reason strings remain available through typed
  `Unknown(String)` variants. Every wallet operation that an external client
  preflights has a client-owned `Capability` variant, so consumers do not
  import protocol constants or copy wire strings. Generated protobuf types are
  not public SDK vocabulary.
- Public traits describe consumer capabilities. Concrete RocksDB types remain
  at composition roots and storage adapters.

## Error vocabulary

Every gRPC error carries a canonical gRPC status plus
`google.rpc.ErrorInfo` with `domain = "zinder.dev"` and one stable
`ErrorReason`. The outer status controls general retry behavior; the reason
controls typed handling.

Important current reasons include:

| Reason | Meaning |
| --- | --- |
| `CHAIN_EPOCH_PIN_UNAVAILABLE` | Requested epoch is no longer retained |
| `SCHEMA_MISMATCH` | Persisted store contract differs from the running binary |
| `REORG_WINDOW_EXCEEDED` | Replacement crosses the configured reorg policy |
| `BLOCK_NOT_IN_BEST_CHAIN` | Requested block is not visible in the admitted epoch |
| `MATERIALIZED_VIEW_UNAVAILABLE` | Required materialized view is not configured or admitted |
| `DEPENDENCY_NOT_CONFIGURED` | A required federated dependency is absent |
| `UPSTREAM_UNREACHABLE` | A configured dependency is temporarily unreachable |
| `NODE_CAPABILITY_MISSING` | The source node cannot provide a required capability |

Do not encode retry policy in error text. Do not reuse a reason for a different
failure mode. Additive proto reasons receive new scalar values, mappings,
documentation, and client round-trip tests. The complete table is in
[Error vocabulary](../reference/error-vocabulary.md).

## Configuration

Configuration precedence is compiled defaults, TOML, `ZINDER_*` environment
variables, then CLI overrides. Unknown fields fail. Cross-field invariants are
validated before storage opens or listeners bind.

Release configuration is grouped by owner:

- `[network]`, `[node]`, `[node.auth]`, `[ops]`, and `[security]` are shared
  sections;
- `[storage]`, `[ingest]`, `[ingest.construction]`, `[ingest.mempool]`,
  `[ingest.follow]`, `[ingest.run_overrides]`, `[retention]`, and
  `[ingest_control]` configure the canonical writer;
- `[storage]`, `[projector]`, `[ingest_control]`, and `[projector_control]`
  configure the wallet projector; and
- `[storage]`, `[wallet]`, `[compat]`, and `[ingest_control]` configure the
  lightwalletd adapter.

Nested environment names use double underscores, for example:

```text
ZINDER_NETWORK__NAME
ZINDER_NODE__JSON_RPC_ADDR
ZINDER_NODE__AUTH__METHOD
ZINDER_INGEST__CONSTRUCTION__SOURCE_FETCH_MAX_IN_FLIGHT_REQUESTS
ZINDER_INGEST__MEMPOOL__MAX_TOTAL_RAW_TRANSACTION_BYTES
ZINDER_INGEST__FOLLOW__POLL_INTERVAL_MS
ZINDER_PROJECTOR__BUILD_OWNER_HEX
ZINDER_COMPAT__PAIR_CONVERGENCE_ATTEMPTS
```

The service's `--print-config` output is the authoritative field and default
inventory for its current binary. It must show explicit redaction markers for
secrets and raw authorization material. Examples under `deploy/config/` must
stay synchronized with it.

### Environment-variable reference

<!-- env-var-table:public-interfaces:start -->
| Variable | Used by | Requirement | TOML field | Description |
| -------- | ------- | ----------- | ---------- | ----------- |
| `ZINDER_NETWORK__NAME` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Required | `network.name` | Network identifier: `zcash-mainnet`, `zcash-testnet`, or `zcash-regtest`. Note: live-test gating reads the bare `ZINDER_NETWORK` env var directly and never reaches the config loader, so test runbooks still quote that form. |
| `ZINDER_NODE__JSON_RPC_ADDR` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Required | `node.json_rpc_addr` | Upstream Zebra JSON-RPC URL the service connects to. Optional for `zinder-explorer`: without it the upstream-observation probe stays off and `ExplorerFreshness.chain_view.upstream_tip` is always unset. |
| `ZINDER_NODE__INDEXER_GRPC_ADDR` | zinder-ingest | Optional | `node.indexer_grpc_addr` | Optional Zebra indexer gRPC endpoint enabling the streaming mempool source and chain-tip wakeups. Falls back to JSON-RPC polling when unset or empty. |
| `ZINDER_NODE__AUTH__METHOD` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node.auth.method` | Upstream-node auth shape: `basic`, `cookie`, or unset for no auth. |
| `ZINDER_NODE__AUTH__USERNAME` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `ZINDER_NODE__AUTH__METHOD=basic` | `node.auth.username` | Basic-auth username. Paired with `ZINDER_NODE__AUTH__PASSWORD`. |
| `ZINDER_NODE__AUTH__PASSWORD` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `ZINDER_NODE__AUTH__METHOD=basic` | `node.auth.password` | Basic-auth password. Redacted in `--print-config` and structured logs. (sensitive; redacted) |
| `ZINDER_NODE__AUTH__PATH` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `ZINDER_NODE__AUTH__METHOD=cookie` | `node.auth.path` | Path to a cookie file. Mutually exclusive with `ZINDER_NODE__AUTH__COOKIE`. |
| `ZINDER_NODE__AUTH__COOKIE` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | When `ZINDER_NODE__AUTH__METHOD=cookie` | `node.auth.cookie` | Inline cookie credentials (`username:password`). Mutually exclusive with `ZINDER_NODE__AUTH__PATH`. Accepted for PaaS environments without persistent disks. (sensitive; redacted) |
| `ZINDER_NODE__REQUEST_TIMEOUT_SECS` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node.request_timeout_secs` | Upstream-node JSON-RPC request timeout in seconds. Defaults to 30. |
| `ZINDER_NODE__MAX_RESPONSE_BYTES` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node.max_response_bytes` | Maximum JSON-RPC response body size (bytes) accepted from the node. |
| `ZINDER_NODE__BROADCAST_TIMEOUT_SECS` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `node.broadcast_timeout_secs` | Per-call timeout (seconds) applied only to `sendrawtransaction`. When unset, the global `request_timeout_secs` applies instead. Recommended: 7. |
| `ZINDER_NODE__HEALTH__ADDR` | zinder-ingest | Optional | `node.health.addr` | URL of the upstream's HTTP `/ready` endpoint. When set, the writer polls it as the primary upstream-sync signal; when unset, the writer falls back to `getblockchaininfo.verificationprogress`/`estimatedheight`. See [ADR-0015](../adrs/0015-phase-driven-ingest.md). |
| `ZINDER_NODE__HEALTH__POLL_INTERVAL_MS` | zinder-ingest, zinder-explorer | Optional | `node.health.poll_interval_ms` | Cadence of the upstream-health probe in milliseconds. Defaults to 30000. Must be greater than zero. `zinder-explorer` reuses the same cadence for its upstream-observation probe (the one that populates `ExplorerFreshness.chain_view.upstream_tip`). |
| `ZINDER_NODE__HEALTH__VERIFICATION_PROGRESS_FLOOR` | zinder-ingest | Optional | `node.health.verification_progress_floor` | Lower bound on `getblockchaininfo.verificationprogress` below which the fallback path reports `upstream_not_ready`. Defaults to 0.999. Must be in `(0.0, 1.0)`. |
| `ZINDER_NODE__HEALTH__ESTIMATED_GAP_FLOOR_BLOCKS` | zinder-ingest | Optional | `node.health.estimated_gap_floor_blocks` | Block gap between `estimatedheight` and the local tip above which the fallback path reports `upstream_not_ready`. Defaults to 10. |
| `ZINDER_OPS__LISTEN_ADDR` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `ops.listen_addr` | Listen address for the operational HTTP endpoint (`/healthz`, `/readyz`, `/metrics`). Defaults to a per-service loopback address (`127.0.0.1:9105` ingest, `9110` projector, `9106` query, `9107` compat, `9069` explorer). Set to an empty string to disable the endpoint entirely. |
| `ZINDER_SECURITY__ALLOW_PUBLIC_BIND` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `security.allow_public_bind` | Opts a binary in to binding its plaintext serving and operational surfaces to a public or unspecified (`0.0.0.0`, `::`) address. Defaults to `false`: a loopback or private-range bind is always allowed, but a public or unspecified bind is refused at startup unless this is `true`. Zinder ships no server TLS (ADR-0006); set this only when a reverse proxy terminates TLS and authorization in front of the listener. |
| `ZINDER_INGEST_CONTROL__LISTEN_ADDR` | zinder-ingest | Optional | `ingest_control.listen_addr` | Listen address of the private IngestControl gRPC endpoint. Localhost-only by default; cross-host deployments must add bearer-token auth per ADR-0006. Set to an empty string to disable the endpoint for diagnostic one-shot runs (such as `--target-height` pre-seed). |
| `ZINDER_INGEST_CONTROL__ADDR` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `ingest_control.addr` | URL of the colocated IngestControl writer (`http://host:port`). Readers use it for tip-change subscriptions, mempool reads, and writer-status lookups. Defaults to `http://127.0.0.1:9100`. |
| `ZINDER_INGEST_CONTROL__BEARER_TOKEN_PATH` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd | When `ingest enforces auth` | `ingest_control.bearer_token_path` | Path to the shared-secret bearer token the IngestControl endpoint enforces on every request (ADR-0006). The writer reads it to verify; the readers read the same file to present. File-only by policy; inline secrets are rejected at config load. |
| `ZINDER_INGEST_CONTROL__CHECKPOINT_STAGING_ROOT` | zinder-ingest | Optional | `ingest_control.checkpoint_staging_root` | Directory containing freshly prepared state-bundle candidate directories. CanonicalControl accepts only an opaque candidate id and creates its canonical checkpoint at `<root>/<candidate-id>/canonical.rocksdb`; production mounts this path from a dedicated staging volume into ingest and projector only, never query or compatibility. Defaults to `/var/lib/zinder/checkpoints`. |
| `ZINDER_INGEST_CONTROL__CHECKPOINT_BEARER_TOKEN_PATH` | zinder-ingest | When `canonical checkpoint capture is enabled` | `ingest_control.checkpoint_bearer_token_path` | Path to the separate method-level token required by CanonicalControl.CreateOwnerCheckpoint and ReadmitOwnerCheckpoint. Mount this file only into ingest and projector; query and compatibility must not receive it. |
| `ZINDER_PROJECTOR_CONTROL__LISTEN_ADDR` | zinder-projector | Optional | `projector_control.listen_addr` | Loopback-only private ProjectorControl gRPC endpoint for coherent capture. Empty or unset disables it; an enabled endpoint requires projector_control.bearer_token_path. |
| `ZINDER_PROJECTOR_CONTROL__BEARER_TOKEN_PATH` | zinder-projector | When `projector control is enabled` | `projector_control.bearer_token_path` | Path to the token required by ProjectorControl and presented as the canonical checkpoint capability. Mount it only into projector and ingest; query and compatibility never read it. |
| `ZINDER_PROJECTOR_CONTROL__CHECKPOINT_STAGING_ROOT` | zinder-projector | Optional | `projector_control.checkpoint_staging_root` | Shared candidate root whose realpath must match ingest_control.checkpoint_staging_root. The projector sends only a SHA-256 root binding to canonical control, never a path. |
| `ZINDER_STORAGE__PATH` | zinder-ingest, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Required | `storage.path` | Canonical RocksDB store path. Writers open it as primary; readers open it as a secondary. |
| `ZINDER_STORAGE__SECONDARY_PATH` | zinder-ingest (verify-canonical-replay only), zinder-query, zinder-compat-lightwalletd, zinder-explorer | Required | `storage.secondary_path` | Process-unique RocksDB secondary metadata directory. Never share this path across reader processes. |
| `ZINDER_STORAGE__INITIAL_CATCHUP_TIMEOUT_MS` | zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.initial_catchup_timeout_ms` | Maximum startup RocksDB secondary catchup duration before a reader starts with the opened secondary and lets /readyz report replica lag. Defaults to 30000. |
| `ZINDER_STORAGE__CANONICAL_PATH` | zinder-projector | Optional | `storage.canonical_path` | Canonical primary RocksDB path the projector opens as a read-only secondary. Defaults to `/var/lib/zinder/canonical`. |
| `ZINDER_STORAGE__CANONICAL_SECONDARY_PATH` | zinder-projector | Optional | `storage.canonical_secondary_path` | Projector-local RocksDB secondary metadata directory for canonical reads. Defaults to `/var/lib/zinder/projector/canonical-secondary`; never share it with another process. |
| `ZINDER_WALLET__PATH` | zinder-projector, zinder-query, zinder-compat-lightwalletd | When `running a wallet-serving reader` | `wallet.path` | Wallet-projection RocksDB primary path. The projector owns it as the primary writer and defaults to `/var/lib/zinder/wallet`; both serving runtimes open it as a read-only secondary and require an explicit path. |
| `ZINDER_WALLET__SECONDARY_PATH` | zinder-query, zinder-compat-lightwalletd | Required | `wallet.secondary_path` | Wallet-serving reader root for immutable wallet-secondary generations. Must be distinct from every primary and canonical-secondary path. |
| `ZINDER_WALLET__ROCKSDB__BLOCK_CACHE_BYTES` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.block_cache_bytes` | Wallet-projection RocksDB block cache budget in bytes. Defaults to 268435456 for the writer and 67108864 for the wallet-serving reader. |
| `ZINDER_WALLET__ROCKSDB__MAX_WAL_BYTES` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.max_wal_bytes` | Wallet-projection RocksDB live WAL ceiling in bytes. Defaults to 268435456 for the writer and 16777216 for the wallet-serving reader. |
| `ZINDER_WALLET__ROCKSDB__MAX_OPEN_FILES` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.max_open_files` | Wallet-projection RocksDB open SST file cap. Defaults to 512 for the writer and 64 for the wallet-serving reader. |
| `ZINDER_WALLET__ROCKSDB__WRITE_BUFFER_BYTES` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.write_buffer_bytes` | Wallet-projection RocksDB per-column-family write buffer size. Defaults to 16777216 for the writer and 4194304 for the wallet-serving reader. |
| `ZINDER_WALLET__ROCKSDB__MAX_WRITE_BUFFER_COUNT` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.max_write_buffer_count` | Wallet-projection RocksDB mutable plus immutable write buffer count. Defaults to 4 for the writer and 2 for the wallet-serving reader. |
| `ZINDER_WALLET__ROCKSDB__MAX_BACKGROUND_JOBS` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.max_background_jobs` | Wallet-projection primary-writer RocksDB background job cap shared by flush and compaction work. Defaults to 2 and is not applied to secondary opens, including wallet-serving readers. |
| `ZINDER_WALLET__ROCKSDB__MEMTABLE_BUDGET_BYTES` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.memtable_budget_bytes` | Wallet-projection RocksDB total memtable budget across column families. Defaults to 536870912 for the writer and 16777216 for a wallet-serving reader. |
| `ZINDER_WALLET__ROCKSDB__STATISTICS_LEVEL` | zinder-projector, zinder-query, zinder-compat-lightwalletd | Optional | `wallet.rocksdb.statistics_level` | Wallet-projection RocksDB statistics collection gate: `off`, `tickers`, or `full`. Defaults to `tickers`. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__BLOCK_CACHE_BYTES` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.block_cache_bytes` | Canonical-store RocksDB block cache budget in bytes. Defaults to 536870912 for writers and 134217728 for readers. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_WAL_BYTES` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.max_wal_bytes` | Canonical-store RocksDB live WAL ceiling in bytes. Defaults to 268435456 for writers and 33554432 for readers. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.max_open_files` | Canonical-store RocksDB open SST file cap. Defaults to 512 for writers and 128 for readers. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__WRITE_BUFFER_BYTES` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.write_buffer_bytes` | Canonical-store per-column-family RocksDB write buffer size. Defaults to 16777216 for writers and 8388608 for readers. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_WRITE_BUFFER_COUNT` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.max_write_buffer_count` | Canonical-store per-column-family mutable plus immutable RocksDB write buffer count. Defaults to 2. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_BACKGROUND_JOBS` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.max_background_jobs` | Canonical-store primary-writer RocksDB background job cap shared by flush and compaction work. Defaults to 2 and is not applied to secondary opens. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__MEMTABLE_BUDGET_BYTES` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.memtable_budget_bytes` | Canonical-store total RocksDB memtable budget across column families. Defaults to 268435456 for writers and 16777216 for readers. |
| `ZINDER_STORAGE__CANONICAL__ROCKSDB__STATISTICS_LEVEL` | zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, zinder-explorer | Optional | `storage.canonical.rocksdb.statistics_level` | Canonical-store RocksDB statistics collection gate: `off`, `tickers`, or `full`. Defaults to `tickers`. |
| `ZINDER_QUERY__LISTEN_ADDR` | zinder-query | Optional | `query.listen_addr` | Listen address for the native WalletQuery gRPC endpoint. Defaults to `127.0.0.1:9102`. |
| `ZINDER_QUERY__REORG_WINDOW_BLOCKS` | zinder-query | Optional | `query.reorg_window_blocks` | Exact canonical replacement-depth identity expected by native query. Must be greater than zero and match the canonical writer. Defaults to 100. |
| `ZINDER_QUERY__PAIR_CONVERGENCE_ATTEMPTS` | zinder-query | Optional | `query.pair_convergence_attempts` | Maximum bounded attempts to converge and admit native query's canonical and wallet secondary pair. Must be in 1..=64; defaults to 12. |
| `ZINDER_COMPAT__LISTEN_ADDR` | zinder-compat-lightwalletd | Optional | `compat.listen_addr` | Listen address for the lightwalletd-compatible gRPC endpoint. Defaults to `127.0.0.1:9067`. |
| `ZINDER_COMPAT__REORG_WINDOW_BLOCKS` | zinder-compat-lightwalletd | Optional | `compat.reorg_window_blocks` | Exact canonical replacement-depth identity expected by compatibility. Must be greater than zero and match the canonical writer. Defaults to 100. |
| `ZINDER_COMPAT__PAIR_CONVERGENCE_ATTEMPTS` | zinder-compat-lightwalletd | Optional | `compat.pair_convergence_attempts` | Maximum bounded attempts to converge and admit compatibility's canonical and wallet secondary pair. Must be in 1..=64; defaults to 12. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__BLOCK_CACHE_BYTES` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.block_cache_bytes` | Materialized-view store RocksDB block cache budget in bytes. Defaults to 268435456 for writers and 67108864 for readers. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_WAL_BYTES` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.max_wal_bytes` | Materialized-view store RocksDB live WAL ceiling in bytes. Defaults to 268435456 for writers and 16777216 for readers. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_OPEN_FILES` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.max_open_files` | Materialized-view store RocksDB open SST file cap. Defaults to 512 for writers and 64 for readers. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__WRITE_BUFFER_BYTES` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.write_buffer_bytes` | Materialized-view store per-column-family RocksDB write buffer size. Defaults to 16777216 for writers and 4194304 for readers. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MAX_WRITE_BUFFER_COUNT` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.max_write_buffer_count` | Materialized-view store per-column-family mutable plus immutable RocksDB write buffer count. Defaults to 4 for writers and 2 for readers. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__MEMTABLE_BUDGET_BYTES` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.memtable_budget_bytes` | Materialized-view store total RocksDB memtable budget across column families. Defaults to 536870912 for writers and 16777216 for readers. |
| `ZINDER_STORAGE__MATERIALIZED_VIEWS__ROCKSDB__STATISTICS_LEVEL` | zinder-ingest, zinder-explorer | Optional | `storage.materialized_views.rocksdb.statistics_level` | Materialized-view store RocksDB statistics collection gate: `off`, `tickers`, or `full`. Defaults to `tickers`. |
| `ZINDER_INGEST__SOURCE` | zinder-ingest | Required | `ingest.source` | Source-adapter selector. Lives on `[ingest]` (not `[node]`) because the choice is a writer-private implementation decision: `[node]` describes the upstream node itself, `[ingest].source` describes which adapter ingest uses to talk to it. See [ADR-0016](../adrs/0016-source-segment-fetching.md). |
| `ZINDER_STORAGE__RAW_BLOB_POLICY` | zinder-ingest | Optional | `storage.raw_blob_policy` | Immutable raw-blob retention contract: `none`, `transactions`, or `all`. Defaults to `none` for explicit coverage so canonical indexing does not write raw block or transaction blobs unless a deployment explicitly needs raw export. Wallet-serving coverage defaults to `transactions` and rejects `none`, because native and lightwalletd-compatible transaction and transparent-history methods require retained bytes. The first canonical commit fixes historical coverage; changing a non-empty store requires a rebuild. |
| `ZINDER_INGEST__REORG_WINDOW_BLOCKS` | zinder-ingest | Optional | `ingest.reorg_window_blocks` | Chain-truth invariant: how deep the live reorg window extends. Bounds settlement, classifier default, and replacement traversal. Must be greater than zero. Defaults to 100. |
| `ZINDER_INGEST__MEMPOOL__MAX_TRANSACTION_COUNT` | zinder-ingest | Optional | `ingest.mempool.max_transaction_count` | Maximum number of transactions admitted into one coherent live mempool. Exceeding the bound withdraws the serving generation and retries source hydration. Must be greater than zero. Defaults to 8000. |
| `ZINDER_INGEST__MEMPOOL__MAX_TOTAL_RAW_TRANSACTION_BYTES` | zinder-ingest | Optional | `ingest.mempool.max_total_raw_transaction_bytes` | Maximum cumulative raw transaction bytes admitted into one coherent live mempool. Exceeding the bound withdraws the serving generation and retries source hydration. Must be greater than zero. Defaults to 80000000. |
| `ZINDER_INGEST__MEMPOOL__RECONCILIATION_BATCH_TARGET_RAW_TRANSACTION_BYTES` | zinder-ingest | Optional | `ingest.mempool.reconciliation_batch_target_raw_transaction_bytes` | Target raw transaction bytes for one durable mempool reconciliation write. A single protocol-valid transaction above the target is written alone so reconciliation can make progress. Must be greater than zero. Defaults to 16000000. |
| `ZINDER_PROJECTOR__REORG_WINDOW_BLOCKS` | zinder-projector | Optional | `projector.reorg_window_blocks` | Wallet undo suffix depth and expected canonical replacement policy. Must match the canonical writer. Defaults to 100. |
| `ZINDER_PROJECTOR__BUILD_OWNER_HEX` | zinder-projector | Required | `projector.build_owner_hex` | Stable 16-byte wallet-build lease owner encoded as exactly 32 hexadecimal characters. Use a distinct value for each concurrently provisioned lane. |
| `ZINDER_PROJECTOR__LEASE_DURATION_SECONDS` | zinder-projector | Required | `projector.lease_duration_seconds` | Wallet-build and canonical-retention lease duration in seconds. Must be at least 14400 so a durable construction phase cannot outlive its lease. |
| `ZINDER_PROJECTOR__FENCE_CONVERGENCE_TIMEOUT_SECONDS` | zinder-projector | Optional | `projector.fence_convergence_timeout_seconds` | Deadline for the projector's canonical secondary to authenticate the writer's exact fence. A secondary behind the writer is retried with capped backoff and reported as syncing once the lag lasts longer than one second; a same-sequence mismatch or a secondary ahead of the writer fails immediately. Must be greater than zero. Defaults to 30. |
| `ZINDER_PROJECTOR__BUILD__MAX_OUTPOINT_SORT_MEMORY_BYTES` | zinder-projector | Optional | `projector.build.max_outpoint_sort_memory_bytes` | Memory ceiling for the wallet builder's outpoint sorter. Defaults to 4294967296. |
| `ZINDER_PROJECTOR__BUILD__MAX_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER` | zinder-projector | Optional | `projector.build.max_secondary_sort_memory_bytes_per_sorter` | Memory ceiling for each wallet secondary-index sorter. Defaults to 1073741824. |
| `ZINDER_PROJECTOR__BUILD__MAX_TEMPORARY_FILE_BYTES_PER_SORTER` | zinder-projector | Optional | `projector.build.max_temporary_file_bytes_per_sorter` | Temporary spill-file ceiling for each wallet builder sorter. Defaults to 68719476736. |
| `ZINDER_PROJECTOR__BUILD__SST_TARGET_LOGICAL_BYTES` | zinder-projector | Optional | `projector.build.sst_target_logical_bytes` | Target logical payload per externally built wallet SST file. Defaults to 134217728. |
| `ZINDER_PROJECTOR__BUILD__MAX_ACCOUNTED_REORG_UNDO_BYTES` | zinder-projector | Optional | `projector.build.max_accounted_reorg_undo_bytes` | Maximum logical wallet undo bytes admitted during fixed-tip construction. Defaults to 536870912. |
| `ZINDER_PROJECTOR__FOLLOW__MAX_TRANSITION_LOGICAL_BYTES` | zinder-projector | Optional | `projector.follow.max_transition_logical_bytes` | Maximum logical planner and write-batch bytes for one atomic wallet following transition. Defaults to 536870912. |
| `ZINDER_INGEST__PHASE_CLASSIFICATION__CATCHUP_THRESHOLD_BLOCKS` | zinder-ingest | Optional | `ingest.phase_classification.catchup_threshold_blocks` | Gap (in blocks) at which the phase-driven ingest loop transitions between `BulkCatchup` and `TipFollow`. Defaults to `ingest.reorg_window_blocks`. See [ADR-0015](../adrs/0015-phase-driven-ingest.md). |
| `ZINDER_INGEST__CONSTRUCTION__CANONICAL_BATCH_MAX_BLOCKS` | zinder-ingest | Optional | `ingest.construction.canonical_batch_max_blocks` | Block count per bulk-catchup commit batch. Defaults to 1000. |
| `ZINDER_INGEST__CONSTRUCTION__CANONICAL_BATCH_MAX_ARTIFACT_BYTES` | zinder-ingest | Optional | `ingest.construction.canonical_batch_max_artifact_bytes` | Canonical artifact bytes accumulated before closing a bulk-catchup batch. Defaults to 536870912. |
| `ZINDER_INGEST__CONSTRUCTION__CANONICAL_BATCH_MAX_ESTIMATED_WRITE_BYTES` | zinder-ingest | Optional | `ingest.construction.canonical_batch_max_estimated_write_bytes` | Estimated canonical write bytes accumulated before closing a bulk-catchup batch. Defaults to 536870912. |
| `ZINDER_INGEST__CONSTRUCTION__CANONICAL_BATCH_MIN_BLOCKS_BEFORE_ESTIMATED_WRITE_CLOSE` | zinder-ingest | Optional | `ingest.construction.canonical_batch_min_blocks_before_estimated_write_close` | Minimum blocks accumulated before estimated write bytes can close a bulk-catchup batch. Single oversized blocks can still close immediately. Defaults to 100. |
| `ZINDER_INGEST__CONSTRUCTION__SOURCE_SEGMENT_MAX_BLOCKS` | zinder-ingest | Optional | `ingest.construction.source_segment_max_blocks` | Diagnostic override for the hard ceiling on connected blocks requested from the source in one segment. The resource-resolved default is 64. |
| `ZINDER_INGEST__CONSTRUCTION__SOURCE_SEGMENT_TARGET_RESPONSE_BYTES` | zinder-ingest | Optional | `ingest.construction.source_segment_target_response_bytes` | Diagnostic override for adaptive response sizing. The default is `min(node.max_response_bytes, 33554432)`. |
| `ZINDER_INGEST__CONSTRUCTION__SOURCE_FETCH_MAX_IN_FLIGHT_REQUESTS` | zinder-ingest | Optional | `ingest.construction.source_fetch_max_in_flight_requests` | Maximum concurrent source segment requests. Defaults to 12. |
| `ZINDER_INGEST__CONSTRUCTION__SOURCE_FETCH_MAX_IN_FLIGHT_BYTES` | zinder-ingest | Optional | `ingest.construction.source_fetch_max_in_flight_bytes` | Diagnostic override for predicted active source responses plus measured completed reassembly. The default is `max(node.max_response_bytes, clamp(container_memory / 64, 134217728, 402653184))`. |
| `ZINDER_INGEST__CONSTRUCTION__BLOCK_PREPARE_CONCURRENCY` | zinder-ingest | Optional | `ingest.construction.block_prepare_concurrency` | Diagnostic override for parallel canonical block-prepare slots. The default is `min(available_parallelism(), 16)`. |
| `ZINDER_INGEST__CONSTRUCTION__BLOCK_PREPARE_MEMORY_WATERMARK_BYTES` | zinder-ingest | Optional | `ingest.construction.block_prepare_memory_watermark_bytes` | Diagnostic override for the prepare and resident-handoff admission watermark. The default is `clamp(container_memory / 64, 134217728, 536870912)`. |
| `ZINDER_INGEST__CONSTRUCTION__COMMIT_REASSEMBLY_MAX_QUEUED_ARTIFACT_BYTES` | zinder-ingest | Optional | `ingest.construction.commit_reassembly_max_queued_artifact_bytes` | Maximum settled-tip artifact bytes that can accumulate while the previous bulk-catchup batch is attaching metadata, committing, or flushing. Defaults to 536870912. |
| `ZINDER_INGEST__CONSTRUCTION__FLUSH_INTERVAL_EPOCHS` | zinder-ingest | Optional | `ingest.construction.flush_interval_epochs` | Bulk-catchup RocksDB flush cadence in committed epochs. Must be greater than zero. Defaults to 5. |
| `ZINDER_INGEST__FOLLOW__POLL_INTERVAL_MS` | zinder-ingest | Optional | `ingest.follow.poll_interval_ms` | Tip-follow poll cadence in milliseconds. Must be greater than zero. Defaults to 1000. |
| `ZINDER_INGEST__FOLLOW__LAG_THRESHOLD_BLOCKS` | zinder-ingest | Optional | `ingest.follow.lag_threshold_blocks` | Block lag at which tip-follow reports `cause=syncing`. Defaults to 1. |
| `ZINDER_INGEST__RUN_OVERRIDES__TARGET_HEIGHT` | zinder-ingest | Optional | `ingest.run_overrides.target_height` | One-shot stop-at modifier; the loop exits 0 after committing this height. |
| `ZINDER_INGEST__RUN_OVERRIDES__CHECKPOINT_HEIGHT` | zinder-ingest | Optional | `ingest.run_overrides.checkpoint_height` | Pre-seed an empty store from an upstream-supplied checkpoint at this height. |
| `ZINDER_INGEST__RUN_OVERRIDES__ALLOW_REORG_WINDOW_SETTLEMENT` | zinder-ingest | Optional | `ingest.run_overrides.allow_reorg_window_settlement` | Disposable-store override: lets bulk-catchup advance the settled tip inside the reorg window. Invalid combined with `coverage = "wallet-serving"`. |
| `ZINDER_INGEST__RUN_OVERRIDES__COVERAGE` | zinder-ingest | Optional | `ingest.run_overrides.coverage` | Ingest coverage mode: `"explicit"` or `"wallet-serving"`. Defaults to `"explicit"`. |
| `ZINDER_RETENTION__CHAIN_EVENT_RETENTION_HOURS` | zinder-ingest | Optional | `retention.chain_event_retention_hours` | Chain-event retention window in hours, enforced by `zinder-ingest`. Defaults to 168 (7 days). `0` disables eviction. |
| `ZINDER_RETENTION__CHAIN_EVENT_RETENTION_CHECK_INTERVAL_MS` | zinder-ingest | Optional | `retention.chain_event_retention_check_interval_ms` | Chain-event retention sweep cadence in milliseconds. Must be greater than zero. Defaults to 60000 (one minute). |
| `ZINDER_RETENTION__CURSOR_AT_RISK_WARNING_HOURS` | zinder-ingest | Optional | `retention.cursor_at_risk_warning_hours` | Cursor-at-risk warning lead time in hours. Must be ≤ `retention.chain_event_retention_hours`. Defaults to 24. |
| `ZINDER_RETENTION__MEMPOOL_MINED_RETENTION_MINUTES` | zinder-ingest | Optional | `retention.mempool_mined_retention_minutes` | Mined-mempool retention window in minutes, enforced by `zinder-ingest`. Defaults to 60. `0` disables retention. |
| `ZINDER_RETENTION__MEMPOOL_INVALIDATED_RETENTION_HOURS` | zinder-ingest | Optional | `retention.mempool_invalidated_retention_hours` | Invalidated-mempool retention window in hours, enforced by `zinder-ingest`. Defaults to 24. `0` disables retention. |
| `ZINDER_RETENTION__MEMPOOL_EVENT_RETENTION_CHECK_INTERVAL_MS` | zinder-ingest | Optional | `retention.mempool_event_retention_check_interval_ms` | Mempool-event retention sweep cadence in milliseconds. Must be greater than zero. Defaults to 30000. |
| `ZINDER_RETENTION__MEMPOOL_EVENT_RETENTION_MAX_EVENTS_PER_STEP` | zinder-ingest | Optional | `retention.mempool_event_retention_max_events_per_step` | Maximum event rows examined by one bounded mempool-retention step. Must be greater than zero. Defaults to 1024. |
| `ZINDER_RETENTION__MEMPOOL_EVENT_RETENTION_MAX_ENCODED_BYTES_PER_STEP` | zinder-ingest | Optional | `retention.mempool_event_retention_max_encoded_bytes_per_step` | Target maximum encoded event bytes examined by one bounded mempool-retention step. The first row may exceed the target to guarantee progress. Must be greater than zero. Defaults to 16000000. |
| `ZINDER_EXPLORER__BEARER_TOKEN_PATH` | zinder-explorer | Optional | `explorer.bearer_token_path` | Path to the shared-secret bearer token the ExplorerQuery endpoint enforces on cross-service explorer-plane reads (ADR-0006). |
| `ZINDER_EXPLORER__LISTEN_ADDR` | zinder-explorer | Optional | `explorer.listen_addr` | Listen address for the ExplorerQuery gRPC endpoint. Defaults to 127.0.0.1:9068. |
| `ZINDER_EXPLORER__WALLET_QUERY_ENDPOINT` | zinder-explorer | Optional | `explorer.wallet_query_endpoint` | WalletQuery gRPC endpoint backing the explorer's wallet-composed reads (transaction detail, block views, search, mempool activity). Empty/unset disables the explorer capabilities that compose canonical wallet reads. |
<!-- env-var-table:public-interfaces:end -->

## Capabilities

Capability strings are lowercase dotted identifiers with a version suffix:

```text
{surface}.{noun}.{operation}_v{N}
```

Examples include `wallet.read.compact_blocks_v1` and
`explorer.transparent_address.activity_v1`. A server advertises a capability
only when the method, required storage, dependencies, and coverage are ready.
An optional proto method may exist without an advertised capability.

Capability names describe semantics, not implementation. Do not expose
`rocksdb`, `postgres`, `grpc`, or a consumer product name in a native
wallet capability unless it is the actual public boundary.

<!-- capability-list:public-interfaces:start -->
```text
wallet.read.visible_tip_block_v1
wallet.read.settled_tip_block_v1
wallet.read.block_id_by_selector_v1
wallet.read.block_header_by_selector_v1
wallet.read.compact_block_at_v2
wallet.read.compact_block_range_v2
wallet.read.compact_block_ironwood_v2
wallet.read.full_block_at_v1
wallet.read.full_block_range_v1
wallet.read.tree_state_at_height_v2
wallet.read.latest_tree_state_checkpoint_v2
wallet.read.subtree_roots_in_range_v1
wallet.read.subtree_roots_ironwood_v1
wallet.read.transaction_by_id_v2
wallet.read.transaction_bytes_v1
wallet.read.server_info_v2
wallet.read.network_upgrade_activations_v1
wallet.broadcast.transaction_v1
wallet.events.chain_v1
wallet.snapshot.mempool_v3
wallet.events.mempool_v2
wallet.mempool.transparent_outputs_by_address_v1
wallet.mempool.transparent_spends_by_outpoint_v1
wallet.mempool.transparent_outputs_by_outpoint_v1
wallet.read.transparent_outputs_by_outpoint_v1
wallet.read.transparent_spends_by_outpoint_v1
wallet.read.transparent_unspent_outputs_by_outpoint_v1
wallet.read.chain_value_pools_at_tip_v1
wallet.read.transparent_utxo_set_summary_v1
wallet.read.transparent_utxo_set_commitment_v1
wallet.address.transparent_unspent_outputs_v1
wallet.address.transparent_history_v1
wallet.address.transparent_balance_v1
explorer.server_info_v1
explorer.transaction.detail_v4
explorer.block.summary_v1
explorer.block.production_series_v2
explorer.block.production_time_range_v1
explorer.block.detail_v1
explorer.block.transactions_v2
explorer.block.final_note_commitment_roots_v1
explorer.block.activity_distribution_v1
explorer.search_v1
explorer.commitment_root.search_v1
explorer.commitment_root.displaced_matches_v1
explorer.mempool.summary_v1
explorer.mempool.snapshot_v1
explorer.mempool.activity_v1
explorer.transparent_address.activity_v2
explorer.transparent_address.deltas_v1
explorer.fee.summary_v1
explorer.fee.conventional_distribution_v1
explorer.fee.paid_distribution_v1
explorer.value_pool.summary_v1
explorer.network_upgrade.status_v1
explorer.value_pool.flow_history_v1
explorer.value_pool.flow_events_in_range_v1
explorer.value_pool.flow_summary_v1
explorer.value_pool.flow_amount_threshold_summary_v1
explorer.value_pool.flow_rounded_amount_summary_v1
explorer.value_pool.balance_history_v1
explorer.utxo_set.summary_v1
explorer.utxo_set.commitment_v1
explorer.chain.reorg_history_v1
explorer.chain.displaced_block_history_v1
explorer.chain.displaced_block_detail_v1
explorer.mempool.event_counts_v1
explorer.transaction.fees_v1
explorer.transaction.history_v1
explorer.transaction.recent_v1
explorer.transaction.history_v2
explorer.transaction.intrinsic_value_balances_v1
explorer.transaction.component_summary_v2
explorer.transparent_address.ranking_v1
explorer.overview.snapshot_v1
explorer.migration.overview_v1
explorer.migration.cohorts_v1
explorer.migration.denominations_v1
```
<!-- capability-list:public-interfaces:end -->

## Wire conventions

- Internal hashes use Zinder's typed internal byte order. Protocol adapters
  convert at their boundary.
- Protobuf bytes are validated for exact length before becoming typed hashes,
  outpoints, or commitments.
- Heights, positions, timestamps, outpoints, and address script hashes use
  codecs from `zinder-core::wire` at persisted key boundaries.
- Optional fields represent genuine absence or unavailable capability. They are
  not filled with zero values to simplify clients.
- Stable enum scalar values and field tags are never renumbered.

## Operational names

Health answers whether the process is alive. Readiness answers whether it can
serve its contract now. Metrics and readiness causes name the owning subsystem:
`canonical`, `wallet_projection`, `materialized_view`, `source`, `mempool`, or
`serving_pair`.

Metric labels are bounded enumerations. Do not place addresses, transaction
identifiers, paths, error messages, or other unbounded values in labels. Log
events use stable snake-case action names such as
`canonical_live_append_committed` and `materialized_view_replay_caught_up`.

## Review checklist

For every new public name:

1. identify the owning domain and runtime;
2. use the vocabulary above before inventing a synonym;
3. verify that the name remains honest across all callers;
4. keep implementation technology behind the domain API;
5. update protocol, config, error, metric, and documentation names together;
6. search the repository for the retired term; and
7. reject aliases or compatibility shims unless a current external contract
   requires them.
