# ADR-0023: Name deployment profiles by what they store and run

Status: Accepted

## Context

Zinder deployments serve different consumers. The wallet deployment runs native `WalletQuery`, the wallet projector, and wallet-mode lightwalletd compatibility, with a local transparent index. Transaction bodies can be resolved from a verified node or retained locally. A deployment for light wallets needs shielded history, tree states, subtree roots, and a few transparent reads. Transaction bodies and the wallet projection are most of a Mainnet wallet deployment's disk ([ADR-0004](0004-storage-and-deployment-topologies.md)). The light-wallet deployment's approximate Mainnet disk target is 20 GiB ([#364](https://github.com/ZcashFoundation/zinder/issues/364)), and the current store layout does not reach it.

The writer and the reader of one deployment are separate processes with separate configuration. Settings that must agree, such as the writer's raw-blob retention and the reader's serving mode, otherwise disagree silently until the reader opens the store the writer built.

A name for a deployment shape must not become a second source of truth. Capabilities follow the composed service ([ADR-0015](0015-wallet-runtime-composition.md)), readiness follows each runtime's admission ([ADR-0016](0016-wallet-serving-readiness.md)), and store admission compares persisted evidence ([ADR-0004](0004-storage-and-deployment-topologies.md)).

## Decision

1. **A profile coordinates the settings of a deployment's processes.** `deployment.profile`, in the shared `[deployment]` section ([ADR-0021](0021-shared-configuration-sections.md)), names the profile that the runtimes of one deployment read. The operator still launches each process. Profiles compose orthogonal optional storage and runtime modules over the shared canonical core (settled-supplement retention policy, single-copy raw block vault, decoupled transparent wallet projection, and optional isolated Explorer consumer). A profile fixes:
   - what the canonical writer stores: its coverage, raw-blob retention, and Explorer views;
   - which composition each reader serves; and
   - the minimum data its readers require ([ADR-0004](0004-storage-and-deployment-topologies.md) decision 2).

   A setting the profile fixes may be repeated with the same value. A conflicting value fails at startup. For ingest, projector, native query, and the compatibility listener, `--print-config` shows the profile and omits the settings it supplies. `publish-shielded-history --print-config` renders only the publisher's effective settings.
2. **A profile is not a capability, a readiness input, or a store identity.** Capabilities derive from the composition and its admitted evidence ([ADR-0015](0015-wallet-runtime-composition.md) decision 1). Readiness follows each runtime's admission rules ([ADR-0016](0016-wallet-serving-readiness.md)). A store records what it holds, not a profile name, and a reader admits any store that meets its requirements.
3. **The light-wallet profile serves lightwalletd clients without transaction bodies or a wallet projection.** Operators run standalone `zinder-ingest` and `zinder-compat-lightwalletd`, and run no projector and no native query. Ingest builds complete wallet-serving history from height 1, with no raw blobs and no Explorer views. Compatibility serves the light-wallet composition:
   - It requires complete block history and a complete subtree-root prefix, and it also admits a store that retains blobs.
   - Canonical storage certifies the coverage of compact blocks, nullifiers, tree states, subtree roots, and the tip. The adapter's signed settled-history projection can supply compact-range and nullifier-range payloads ([ADR-0014](0014-consumer-neutral-wallet-data-plane.md) decision 8).
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
6. **Wallet profiles retain complete history.** `wallet` runs the projector, native query, and wallet compatibility without raw blobs. Its serving runtimes require a verified primary node byte source. `wallet-archive` runs the same wallet services and keeps all raw blobs. An Explorer profile waits for canonical publication to authenticate daily value-pool evidence and for live writes to supply final note-commitment roots.

[Configuration](../reference/configuration.md) lists the settings, and [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md#runtime-serving-selection) describes each serving mode.

## Evidence

- Decision 1, one setting that coordinates writer and readers: judgment. The writer's retention, the reader's serving mode, and the reader's minimum data must agree, and one setting turns a mismatch into a startup failure.
- Decision 2, no profile name in the store: judgment. Its cost is that a writer that should store less needs a new store. A need to tell apart two stores that hold the same data by the profile that built them reopens it.
- Decision 3, the light-wallet footprint: measured. On Mainnet at height 3,593,347, a canonical store built from empty with this profile's settings, at `00030603`, allocated 50.13 GiB with an empty `transaction_blob` family, reached READY in 1 h 51 min on 8 writer cores, and passed 26 of 26 serving checks against a frozen Zebra 7.1.0 node ([#380](https://github.com/ZcashFoundation/zinder/issues/380)). The profile selects exactly those settings. The approximate 20 GiB target ([#364](https://github.com/ZcashFoundation/zinder/issues/364)) is not met.
- Decision 4, transparent address lookup forwarding in the light-wallet composition: judgment on measured cost. On Regtest, a wallet workload with a busy transparent address had a p95 of 13,281 ms with forwarding against 355 ms on the wallet deployment, a 140-row address history made 287 node calls, and four concurrent histories completed 6 of 24; shielded scanning with transaction enhancement stayed within tens of milliseconds of the wallet deployment ([#378](https://github.com/ZcashFoundation/zinder/issues/378)). Mainnet forwarding load is not measured. A Mainnet result in [#378](https://github.com/ZcashFoundation/zinder/issues/378) that misses its gates for shielded scanning reopens it.
- Decision 4, verification of forwarded answers: measured, as [ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) decision 7 records ([#480](https://github.com/ZcashFoundation/zinder/issues/480), [#481](https://github.com/ZcashFoundation/zinder/pull/481)).
- Decision 5, the location and digest of every mined transaction: measured. Node bytes that differ from the recorded digest fail with `DATA_LOSS`, and the wrong-bytes and wrong-block tests fail with their guards disabled ([#481](https://github.com/ZcashFoundation/zinder/pull/481)).
- Decision 5, the precondition for omitting settled replay: judgment about a format that is not built. Replay is about 20 GiB of a Mainnet canonical store ([#364](https://github.com/ZcashFoundation/zinder/issues/364)), and current cold certification reads all of it. The format specification ([#381](https://github.com/ZcashFoundation/zinder/issues/381)) reopens it if it authenticates the settled prefix another way.
- Decision 6, fixed stored sets for the wallet profiles: judgment. Mainnet disk and rebuild measurements are tracked in [#364](https://github.com/ZcashFoundation/zinder/issues/364).

## Consequences

- An operator sets one value for ingest and compatibility instead of several settings that must agree.
- Forwarding suits shielded scanning and not transparent-heavy wallets. Forwarded address history costs node calls that grow with the number of returned rows. Each listener [bounds concurrent requests and the time each can hold a slot](../reference/lightwalletd-compatibility.md#gettaddresstxids-and-gettaddresstransactions). A deployment whose wallets are transparent-heavy runs the wallet deployment instead. [#378](https://github.com/ZcashFoundation/zinder/issues/378) records the measurements.
- Node-backed answers are bound to the served tip, not pinned, and address history is best-effort ([ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) decision 7). [ADR-0016](0016-wallet-serving-readiness.md) describes what a light-wallet reader serves during a node outage.
- The profile runs from the release's standalone binaries and needs no new executable ([ADR-0022](0022-release-artifacts.md)). The bundled single-volume image (`zinder-wallet-serving`) defaults `ZINDER_DEPLOYMENT__PROFILE` to `wallet` when Explorer views and explicit `storage.raw_blob_policy` overrides are unset, accepts `wallet-archive`, omits the profile when `ZINDER_STORAGE__RAW_BLOB_POLICY` is set for a pre-profile `transactions` volume or when Explorer views are enabled (`zinder-wallet-serving-explorer`), and rejects `light-wallet` because it always starts `zinder-projector` and `zinder-query`.
- Because a store records no profile, a reader that needs less can open a richer store. A writer that should store less needs a new store.

## Alternatives rejected

- **Record the profile name in the store.** It would be a second identity that could disagree with the data the store holds, and a richer store could not serve a reader that needs less.
- **Capability profiles.** A name that declares capabilities keeps a shadow model of the composition ([ADR-0015](0015-wallet-runtime-composition.md)).
- **Ship the light-wallet composition only behind a build feature.** Releases ship one reproducible build of each binary ([ADR-0022](0022-release-artifacts.md)), so a feature-gated composition never reaches operators.
- **Forward every read the store lacks.** It is the node fallback that [ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) and [ADR-0014](0014-consumer-neutral-wallet-data-plane.md) reject. Each node-backed method is declared, and each has its own binding to the served tip.
- **Forward transparent reads in the wallet deployment.** Its consumers need exact transparent history and spend status at one epoch, which a local index gives and the node does not.

## Revision history

- 2026-11-26. Consequences reflect that `zinder-wallet-serving` defaults `deployment.profile` to `wallet`, accepts `wallet-archive`, omits the profile when `storage.raw_blob_policy` is set for legacy `transactions` stores or when Explorer views are enabled, and rejects `light-wallet`.
- 2026-11-24. Decision 1 defines deployment profiles as compositions of orthogonal optional modules over the shared canonical core.
- 2026-11-24. Decision 3 notes the signed settled-history projection also supplies nullifier-range payloads, which the light-wallet profile's `GetBlockRangeNullifiers` stream reads zero-copy.
- 2026-11-24. Decisions 1, 4, and 6 add the lean wallet and wallet archive profiles.
- 2026-11-23. Decision 1 distinguishes listener and ingest config output from the publisher's effective settings.
- 2026-11-23. Decision 4 distinguishes the wallet deployment's node calls from the Explorer's tip observation.
- 2026-11-23. The consequences describe bounded forwarded history concurrency and request time.
