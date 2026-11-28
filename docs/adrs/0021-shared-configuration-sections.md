# ADR-0021: Unify configuration schema and deploy boundaries

Status: Accepted

## Context

Zinder's runtimes share core settings: the network, the node endpoint and its
credentials, storage paths, security policy, observability, and operational listeners. Previously, each service binary maintained its own partial TOML/env schema with separate `*Toml` projection structs, hand-written TOML/env filtering (`filter.rs`, `prune_toml_table`, `sibling_sections_for_service`), per-binary subprocess probing (`EnvVarContractProbe` with dozens of `ACCEPTED_BUT_UNCONSUMED` exceptions), and implicit empty-string sentinels. That fragmentation let typos in inactive roles pass unnoticed in unified configs, conflated container orchestration variables with `ZINDER_*` runtime settings, and risked type-coercion corrupting secret strings.

## Decision

1. **One strict unified schema (`ZinderConfig`) in `zinder-runtime`.** `zinder-runtime` owns the layered `toml` + `serde` deserializer (`ConfigLoader` with strict `deny_unknown_fields` and zero value-level type coercion) and the unified `ZinderConfig` root composed from shared section structs (`[deployment]`, `[observability]`, `[network]`, `[ops]`, `[security]`, `[storage]`, `[wallet]`, `[retention]`, `[ingest_control]`, `[projector_control]`, `zinder-source`'s `[node]` and `[node_fallbacks.N]`, and the six role sections `[ingest]`, `[projector]`, `[query]`, `[compat]`, `[explorer]`, `[cipherscan]`). Every binary and the unified `zinder` supervisor parse the exact same schema.
2. **Typed opt-out and redaction primitives instead of empty strings and mirror structs.** Default-enabled endpoints and allowlists use `Toggle<T>` (or explicit `"none"` sentinels on `[node]`), `BrowserOrigins`, and `RedactedCidrs`. Empty or whitespace-only `ZINDER_*` environment values fail closed (`InvalidEnvValue`); opt-out uses `"none"` explicitly. Secrets and credentialed URLs use `SecretInput`, `SecretNodeInput`, `RedactedUrl`, and `RedactedCidrs`, which redact on `Debug` and `Serialize` and reject `[REDACTED]` placeholders as input—eliminating per-role `*Toml` projection mirrors.
3. **One precedence order everywhere, zero-TOML by default.** Compiled defaults, then optional `--config` TOML file, then `ZINDER_*` environment variables, then explicit CLI overrides (`--preset`, `--full-blocks`, `--data-dir`). A later layer wins.
4. **Unified `[storage]` and `[wallet]` sections.** `[storage]` owns `data_dir` path derivation (default `/var/lib/zinder`), `path`, `secondary_path`, `full_blocks` / `raw_blob_policy`, catchup timing, and per-role `RocksDB` resource budgets across `zinder-ingest`, `zinder-projector`, and canonical/materialized-view readers. `[wallet]` owns `path`, `secondary_path`, and `wallet.rocksdb`.
5. **In-memory supervision validation and separated secret transport.** The `zinder` supervisor parses `ZinderConfig` once, rejects writer-only secrets (`ingest_control.checkpoint_bearer_token{,_path}`, `projector_control.bearer_token{,_path}`) in `--readers-only` mode, validates active roles in memory without temp files, writes only non-secret TOML (`extract_secrets_for_launch`) to private temp files, and passes scoped secrets cleanly via child environment variables.
6. **In-process schema-to-docs bijection.** Every field in `ZinderConfig` (discovered dynamically via `discover_schema_leaf_paths()`) must match an entry in `zinder_runtime::ENVIRONMENT_VARIABLES` bijectively, and every documented row is verified against `ZinderConfig` and `docs/reference/configuration.md`.

Every key, default, and environment variable is listed in
[Configuration reference](../reference/configuration.md). Node credentials and
indexer TLS are explained in
[Node source boundary](../architecture/node-source-boundary.md).

## Evidence

- Unified `ZinderConfig` deserialization ensures any unknown field or typo in any section fails immediately regardless of which binary or `zinder serve` preset is active, while `--print-config` serializes directly from redacted section projections.
- Explicit `"none"` toggles prevent unset or blank environment substitutions (e.g., Compose `${VAR:-}`) from silently turning off operational endpoints, trace export, or PROXY protocol enforcement.

## Consequences

- Every runtime shares identical field spellings (`storage.path`, `storage.secondary_path`), defaults, redaction behavior, and validation semantics.
- A misspelled key or accidental blank `ZINDER_*` environment variable fails closed before storage opens.
- Adding or renaming any field in `ZinderConfig` forces updating `ENVIRONMENT_VARIABLES` and regenerating the table in `docs/reference/configuration.md`.
- Container orchestration and reverse-proxy knobs (`DEPLOY_*`) stay strictly separated from container `ZINDER_*` environment files (`env_file:`).

## Alternatives rejected

- **A private schema subset per binary with `filter.rs` / `prune_toml_table`.** Duplicated 40+ `*Toml` mirror structs and allowed typos in inactive role sections to bypass `zinder serve` validation.
- **Accepting empty strings (`""`) as disabling toggles.** Shell/Compose empty substitutions silently altered runtime topology and masked accidental blank env values.
- **Per-binary subprocess contract probing (`EnvVarContractProbe`) with `ACCEPTED_BUT_UNCONSUMED` allowlists.** Required manual exception lists across six crates instead of a single in-process bijection check.

## Revision history

- 2026-11-28. Unified `ZinderConfig` schema across all binaries and supervisor, replaced `*Toml` mirrors with redacting/toggle primitives, rejected blank env vars in favor of `"none"`, unified `[storage]` paths for `zinder-projector`, and switched contract verification to an in-process `ZinderConfig` bijection gate.
- 2026-11-27. Documented zero-TOML `ZINDER_*` env-first loading, `storage.data_dir` path derivation, sibling-section tolerance, and native inline `*_pem` + bearer-token redaction.
- 2026-11-23. Record `[deployment]` under the shared section convention.
