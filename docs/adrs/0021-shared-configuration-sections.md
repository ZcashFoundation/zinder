# ADR-0021: Share configuration sections and compose them per runtime

Status: Accepted

## Context

Zinder's runtimes share many settings: the network, the node endpoint and its
credentials, storage paths, security policy, and the ops listener. When each
binary keeps its own copy of these settings, the copies drift in field names,
defaults, environment-variable names, redaction, and validation. An operator
who sets a value for one runtime cannot rely on the same key meaning the same
thing in another.

Forcing every binary to accept every field has the opposite problem. Ownership
becomes unclear, and a reader runtime appears able to configure behavior that
only the writer has.

## Decision

1. **Each shared section has one owning crate.** `zinder-runtime` owns the
   loader and the `[deployment]`, `[network]`, `[storage]`, `[security]`, `[ops]`,
   `[retention]`, `[ingest_control]`, and `[projector_control]` sections.
   `zinder-source` owns `[node]` and `[node_fallbacks.N]`. Each role schema
   composes only the sections it uses and owns its own validation, while
   known sibling service sections are tolerated in combined TOML configs and shared
   `ZINDER_*` environments so one `.env` or `--config` file configures `zinder serve`
   and Compose cleanly without typo tolerance.
2. **One precedence order everywhere, zero-TOML by default.** Compiled defaults,
   then optional `--config` TOML file, then `ZINDER_*` environment variables,
   then explicit command-line overrides (`--preset`, `--full-blocks`, `--data-dir`).
   A later layer wins.
3. **Shared names keep one meaning.** `[deployment]` selects the preset
   (`light-wallet`, `wallet`, or `explorer`) shared by a deployment's runtimes.
   `[network]` selects the network. `[node]` and `[node_fallbacks.N]` own node
   endpoints, authentication, transport limits, indexer TLS, and health probing.
   `[storage]` owns `data_dir` path derivation (default `/var/lib/zinder`),
   `full_blocks` / `raw_blob_policy`, canonical and view store paths, secondary
   catch-up settings, and their resource budgets. `[wallet]` owns the wallet store
   path and its resource budget. `[security]` owns the public-bind policy. `[ops]`
   owns the health and metrics listener. Service sections such as `[ingest]`,
   `[projector]`, `[query]`, `[compat]`, `[explorer]`, and `[cipherscan]` own
   only that runtime's behavior.
4. **Ingest groups its own settings by phase.** Construction limits, tip
   following, phase selection, and one-run overrides each have their own
   subsection under `[ingest]`.
5. **A shared section accepts all of its fields, and known sibling sections do not block unified launches.** A role reading `[node]` accepts ingest-only indexer fields too and ignores what it does not use; the unified `zinder` supervisor filters combined configuration into per-role payloads while rejecting unknown sections, unknown keys, or typos with typo suggestions.
6. **Configuration fails before storage opens.** Unknown fields are
   rejected, and invalid combinations of fields fail at startup.
7. **`--print-config` (and `zinder config print`) shows the resolved configuration.** It uses the same serialization as the loader. Secrets and authorization material—including passwords, inline cookies, inline `*_pem` TLS material, and inline bearer tokens (`bearer_token`, `checkpoint_bearer_token`)—are replaced by redaction markers. Both file paths (`*_path`) and inline values (`*_pem`, `bearer_token`) are accepted directly without external shell materialization scripts.

Every key, default, and environment variable is listed in
[Configuration reference](../reference/configuration.md). Node credentials and
indexer TLS are explained in
[Node source boundary](../architecture/node-source-boundary.md).

## Evidence

- Decisions 1 and 3, the `[deployment]` section: judgment. Ingest and
  lightwalletd compatibility both read the profile, so it follows the rule
  for sections that several runtimes share.

## Consequences

- A shared field has one spelling, one default, one environment mapping, and
  one redaction rule across every runtime.
- A misspelled key fails at startup instead of being silently ignored.
- A new shared field belongs in the crate that owns its section. A
  service-only field stays in that service's configuration module.
- A configuration change updates `--print-config`, the configuration
  reference, examples, and tests in the same change.
- The configuration reference must say which binaries read a variable, and
  must note when a listed binary ignores it, because shared sections accept
  fields that some binaries do not use.

## Alternatives rejected

- **A private configuration schema per binary.** The copies drift, and the
  same key gains different meanings.
- **One shared schema without validation.** Rejecting unknown keys and typos fails fast even while tolerating declared sibling service sections in shared configs.
- **Per-binary subsets of a shared section.** Splitting `[node]` by binary
  would give one concept several schemas, which is the drift this decision
  removes.

## Revision history

- 2026-11-27. Documented zero-TOML `ZINDER_*` env-first loading, `storage.data_dir` path derivation, sibling-section tolerance, and native inline `*_pem` + bearer-token redaction.
- 2026-11-23. Record `[deployment]` under the shared section convention.
