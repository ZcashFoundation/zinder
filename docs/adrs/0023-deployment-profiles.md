# ADR-0023: Name deployment profiles by what they store and run

Status: Accepted

## Context

Zinder deployments serve different consumers. The wallet deployment runs native `WalletQuery`, the wallet projector, and wallet-mode lightwalletd compatibility, with a local transparent index. Transaction bodies can be resolved from a verified node or retained locally. A deployment for light wallets needs shielded history, tree states, subtree roots, and a few transparent reads. Transaction bodies and the wallet projection are most of a Mainnet wallet deployment's disk ([ADR-0004](0004-storage-and-deployment-topologies.md)). Canonical Schema v2 retires duplicated settled-history copies and `storage.raw_blob_policy = "transactions"`, keeping only `none` and `all` (`storage.full_blocks = true`) across the three presets (`light-wallet`, `wallet`, `explorer`).

The writer and the reader of one deployment are separate processes with separate configuration. Settings that must agree, such as the writer's raw-blob retention and the reader's serving mode, otherwise disagree silently until the reader opens the store the writer built.

A name for a deployment shape must not become a second source of truth. Capabilities follow the composed service ([ADR-0015](0015-wallet-runtime-composition.md)), readiness follows each runtime's admission ([ADR-0016](0016-wallet-serving-readiness.md)), and store admission compares persisted evidence ([ADR-0004](0004-storage-and-deployment-topologies.md)).

## Decision

1. **A profile (`--preset`) coordinates the settings of a deployment's processes.** `deployment.profile` (`ZINDER_DEPLOYMENT__PROFILE`, selected conveniently with `--preset` on `zinder serve` / `zinder index`) names one of three presets (`light-wallet`, `wallet`, `explorer`) shared across the runtimes of one deployment, while orthogonal `storage.full_blocks = true` (`--full-blocks`) toggles raw block and transaction blob retention on any preset. Profiles compose optional storage and runtime modules over the shared canonical core:
   - what the canonical writer stores: its coverage, raw-blob retention, and Explorer views;
   - which composition each reader serves; and
   - the minimum data its readers require ([ADR-0004](0004-storage-and-deployment-topologies.md) decision 2).

   A setting the profile fixes may be repeated with the same value. A conflicting value fails at startup. `--print-config` (and `zinder config print`) shows the profile and omits the settings it supplies.
2. **A profile is not a capability, a readiness input, or a store identity.** Capabilities derive from the composition and its admitted evidence ([ADR-0015](0015-wallet-runtime-composition.md) decision 1). Readiness follows each runtime's admission rules ([ADR-0016](0016-wallet-serving-readiness.md)). A store records what it holds, not a profile name, and a reader admits any store that meets its requirements.
3. **The light-wallet profile serves lightwalletd clients without transaction bodies or a wallet projection.** Operators run standalone `zinder-ingest` and `zinder-compat-lightwalletd`, and run no projector and no native query. Ingest builds complete wallet-serving history from height 1, with no raw blobs and no Explorer views. Compatibility serves the light-wallet composition:
   - It requires complete block history and a complete subtree-root prefix, and it also admits a store that retains blobs.
   - Canonical storage certifies coverage and serves compact-block and nullifier-range wire payloads directly from canonical `ZSHRNG02` settled segments and the `live_tail_block` column family ([ADR-0014](0014-consumer-neutral-wallet-data-plane.md) decision 8).
   - Mempool methods go through ingest control.
   - The reads in decision 4 go to one node that keeps transparent address indexes.
4. **Only the light-wallet composition delegates transparent address lookups to the node.** This holds whether the profile or the explicit compatibility serving setting selects the composition. Its node-backed reads follow [ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) decision 7:
   - `GetTransaction` for a mined transaction, when the admitted store retains no transaction bodies;
   - `GetTaddressTxids` and `GetTaddressTransactions`; and
   - `GetTaddressBalance` and `GetAddressUtxos`, with their stream forms.

   Wallet serving gets address membership, balances, UTXOs, and spend status from the wallet projection. With a lean store, the node supplies exact mined bytes for transaction lookups, including the bodies returned for locally selected compatibility history rows. See [ADR-0018](0018-transaction-facts-status-and-broadcast.md).
5. **A profile omits data only when its trust checks still hold.**
   - A composition that forwards mined transaction bytes needs, for every mined transaction in its history, the location and byte digest the writer recorded. The store keeps both.
   - Current certification reads the full replay history. Omitting settled replay requires authenticated evidence for the settled prefix at cold admission and at restore, and enough retained replay for reorg replacement and for every consumer that reads it, as the wallet projector and Explorer views do.

   The encoding of transaction evidence, the construction of prefix evidence, and the length of retained replay belong to the format specification.
6. **Wallet and Explorer presets retain complete history.** `wallet` runs the projector, native query, and wallet compatibility without raw blobs (`raw_blob_policy = "none"` unless `--full-blocks` / `storage.full_blocks = true` is set, requiring a verified primary node byte source). `explorer` enables `ingest.explorer_views = true` alongside the full `wallet` stack plus `zinder-explorer` and `zinder-compat-cipherscan`. Combining `--full-blocks` (`storage.full_blocks = true`) with any preset sets `raw_blob_policy = "all"` cleanly without requiring a separate `wallet-archive` profile.

7. **Upstream retention and the local stored set are independent.** The
   ratified policy admits pruned following for an existing, verified
   `READY` Schema v2 store under any admitted preset or explicit
   composition, including lean and `--full-blocks`, when the horizon and
   recovery conditions in [ADR-0006](0006-node-source-boundary.md) hold.
   Fresh construction still requires archive history. Local full retention
   supplies historical transaction bodies. A lean store backed solely by
   pruned validators cannot promise those bodies. Discovery and errors follow
   [ADR-0015](0015-wallet-runtime-composition.md) and
   [ADR-0018](0018-transaction-facts-status-and-broadcast.md).

8. **Shifted-key hints are outside the canonical stored set.** The
   optional rebuildable sidecar in [ADR-0014](0014-consumer-neutral-wallet-data-plane.md)
   decision 10 adds no default storage cost to any preset. Production
   implementation remains gated on independent client safety, a measured
   end-to-end mobile win, and a committed consumer.

9. **Light-client proofs are an opt-in canonical stored-set module.**
   `storage.light_client_proofs` defaults to off. The minimal `light-wallet`
   stored set stays small and the default `CompactBlock.header` stays empty.
   After [G1 freezes the format](#schema-v2-qualification-gates), the module
   keeps reversible and settled ZIP 221 Merkle mountain range nodes and
   `hashAuthDataRoot`. When `full_blocks`
   is enabled, read 1,487-byte consensus headers from `ZSBLKB01` or
   `live_tail_raw_block`. Persist separate Equihash solutions only when
   `full_blocks` is disabled. The native-first release boundary is in
   [ADR-0014](0014-consumer-neutral-wallet-data-plane.md) decision 11.
   This module is ratified and pending implementation.

[Configuration](../reference/configuration.md) lists the settings, and [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md#runtime-serving-selection) describes each serving mode.

### Schema v2 qualification gates

10. **Qualify Schema v2 before depending on the settled format.** The
    canonical Schema v2 code cutover shipped in `v0.28.0`. Mainnet rebuild
    and consumer qualification remain tracked in
    [#384](https://github.com/ZcashFoundation/zinder/issues/384).

    - **G1 (format freeze).** From-empty Mainnet builds on Zebra and Zakura
      must reach `READY`, pass cold certification, and produce matching
      canonical sequence digests. G1 unblocks
      [#705](https://github.com/ZcashFoundation/zinder/issues/705) and
      format-dependent [#697](https://github.com/ZcashFoundation/zinder/issues/697)
      work.
    - **G2 (consumer qualification).** Representative wallet restores, an
      Explorer rebuild, and epoch and cursor renewals must pass
      qualification. G2 closes #384 and unblocks
      [#385](https://github.com/ZcashFoundation/zinder/issues/385).

## Evidence

- Decision 10: Judgment. Matching cold-certified Mainnet digests establish a common format baseline, and consumer qualification checks the workflows that rely on it. A digest mismatch or a failed consumer workflow reopens the corresponding gate.
- Decision 7: Judgment. [ADR-0006](0006-node-source-boundary.md#evidence) decision 9 records the horizon constraint and recovery evidence that would reopen admission.
- Decision 8: Judgment. Rebuildable hints can evolve without changing the canonical segment format. A qualified client that cannot consume a rebuildable sidecar would reopen placement. The safety and mobile evidence is in [ADR-0014](0014-consumer-neutral-wallet-data-plane.md#evidence).
- Decision 9: Judgment. Opt-in storage bounds the default footprint, and full-block deployments can reuse headers they already store. Reopen placement if format qualification shows reuse cannot supply byte-identical headers or meet measured proof-serving latency.
- Decision 1, one setting that coordinates writer and readers: judgment. The writer's retention, the reader's serving mode, and the reader's minimum data must agree, and one setting turns a mismatch into a startup failure.
- Decision 2, no profile name in the store: judgment. Its cost is that a writer that should store less needs a new store. A need to tell apart two stores that hold the same data by the profile that built them reopens it.
- Decision 3, the light-wallet footprint: measured (historical Schema v1 baseline; Canonical Schema v2 re-measurement pending in [#364](https://github.com/ZcashFoundation/zinder/issues/364)). On Mainnet at height 3,593,347 under Schema v1 (`00030603`), a canonical store built from empty with `light-wallet` settings and empty raw-blob storage allocated 50.13 GiB, reached READY in 1 h 51 min on 8 writer cores, and passed 26 of 26 serving checks against a frozen Zebra 7.1.0 node ([#380](https://github.com/ZcashFoundation/zinder/issues/380)). Canonical Schema v2 replaces those RocksDB replay/compact-block families and the duplicate `ZSHRNG01` catalog with unified `ZSHRNG02`/`ZSTSUP01` settled segments to move toward the 20 GiB target ([#364](https://github.com/ZcashFoundation/zinder/issues/364)).
- Decision 4, transparent address lookup forwarding in the light-wallet composition: judgment on measured cost. On Regtest, a wallet workload with a busy transparent address had a p95 of 13,281 ms with forwarding against 355 ms on the wallet deployment, a 140-row address history made 287 node calls, and four concurrent histories completed 6 of 24; shielded scanning with transaction enhancement stayed within tens of milliseconds of the wallet deployment ([#378](https://github.com/ZcashFoundation/zinder/issues/378)). Mainnet forwarding load is not measured. A Mainnet result in [#378](https://github.com/ZcashFoundation/zinder/issues/378) that misses its gates for shielded scanning reopens it.
- Decision 4, verification of forwarded answers: measured, as [ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) decision 7 records ([#480](https://github.com/ZcashFoundation/zinder/issues/480), [#481](https://github.com/ZcashFoundation/zinder/pull/481)).
- Decision 5, the location and digest of every mined transaction: measured. Node bytes that differ from the recorded digest fail with `DATA_LOSS`, and the wrong-bytes and wrong-block tests fail with their guards disabled ([#481](https://github.com/ZcashFoundation/zinder/pull/481)).
- Decision 5, authenticated settled prefix storage: implemented in Canonical Schema v2. `ZSHRNG02`, `ZSTSUP01`, and optional `ZSBLKB01` settled segments authenticate settled history via per-segment `index_sha256` / payload digests recorded in `settled_segment_index` and bound into `canonical-construction-manifest.v2.json` ([#381](https://github.com/ZcashFoundation/zinder/issues/381)), retiring the ~20 GiB Schema v1 replay redundancy ([#364](https://github.com/ZcashFoundation/zinder/issues/364)).
- Decision 6, fixed stored sets for the wallet profiles: judgment. Mainnet disk and rebuild measurements for Schema v2 are tracked in [#364](https://github.com/ZcashFoundation/zinder/issues/364).

## Consequences

- An operator sets one value for ingest and compatibility instead of several settings that must agree.
- Forwarding suits shielded scanning and not transparent-heavy wallets. Forwarded address history costs node calls that grow with the number of returned rows. Each listener [bounds concurrent requests and the time each can hold a slot](../reference/lightwalletd-compatibility.md#gettaddresstxids-and-gettaddresstransactions). A deployment whose wallets are transparent-heavy runs the wallet deployment instead. [#378](https://github.com/ZcashFoundation/zinder/issues/378) records the measurements.
- Node-backed answers are bound to the served tip, not pinned, and address history is best-effort ([ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) decision 7). [ADR-0016](0016-wallet-serving-readiness.md) describes what a light-wallet reader serves during a node outage.
- All three presets (`light-wallet`, `wallet`, `explorer`) and `--full-blocks` run directly from the unified `zinder` binary and distroless OCI image ([ADR-0022](0022-release-artifacts.md)) via `zinder serve` and `zinder index`, defaulting to `wallet` with auto-persisted `projector.build_owner_hex`. Under Canonical Schema v2, `storage.raw_blob_policy = "transactions"` is removed and refused at configuration parsing; retaining local raw transaction and block bytes uses `--full-blocks` (`storage.full_blocks = true`, `storage.raw_blob_policy = "all"`). Existing Schema v1 stores must be rebuilt afresh.
- Because a store records no profile, a reader that needs less can open a richer store. A writer that should store less needs a new store.

## Alternatives rejected

- **Require raw block retention before admitting any pruned validator.** The recovery interval governs live-follow safety. Body availability has its own contract.
- **Store Equihash solutions twice on full-block deployments.** The raw block already contains the required consensus header.
- **Record the profile name in the store.** It would be a second identity that could disagree with the data the store holds, and a richer store could not serve a reader that needs less.
- **Capability profiles.** A name that declares capabilities keeps a shadow model of the composition ([ADR-0015](0015-wallet-runtime-composition.md)).
- **Ship the light-wallet composition only behind a build feature.** Releases ship one reproducible build of `bin/zinder` ([ADR-0022](0022-release-artifacts.md)), so a feature-gated composition never reaches operators.
- **Forward every read the store lacks.** It is the node fallback that [ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) and [ADR-0014](0014-consumer-neutral-wallet-data-plane.md) reject. Each node-backed method is declared, and each has its own binding to the served tip.
- **Forward transparent reads in the wallet deployment.** Its consumers need exact transparent history and spend status at one epoch, which a local index gives and the node does not.

## Revision history

- 2026-11-28. Decisions 7 through 10 specify pruned following, optional shifted-key hints, the opt-in proof stored set, and Schema v2 qualification gates.
- 2026-11-27. Removed `storage.raw_blob_policy = "transactions"` carry-forward guidance and reconciled evidence with shipped Canonical Schema v2 (`none | all`).
- 2026-11-27. Replaced `wallet-archive` with orthogonal `--full-blocks` (`storage.full_blocks`), elevated `explorer` as a preset across unified `zinder serve` / `zinder index`, and dropped references to the removed `zinder-wallet-serving*` images.
- 2026-11-26. Consequences reflect that `zinder-wallet-serving` defaults `deployment.profile` to `wallet`, accepts `wallet-archive`, omits the profile when `storage.raw_blob_policy` is set for legacy `transactions` stores or when Explorer views are enabled, and rejects `light-wallet`.
- 2026-11-24. Decision 1 defines deployment profiles as compositions of orthogonal optional modules over the shared canonical core.
- 2026-11-24. Decision 3 notes the signed settled-history projection also supplies nullifier-range payloads, which the light-wallet profile's `GetBlockRangeNullifiers` stream reads zero-copy.
- 2026-11-24. Decisions 1, 4, and 6 add the lean wallet and wallet archive profiles.
- 2026-11-23. Decision 1 distinguishes listener and ingest config output from the publisher's effective settings.
- 2026-11-23. Decision 4 distinguishes the wallet deployment's node calls from the Explorer's tip observation.
- 2026-11-23. The consequences describe bounded forwarded history concurrency and request time.
