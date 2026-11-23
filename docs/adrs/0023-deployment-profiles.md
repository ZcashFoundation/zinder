# ADR-0023: Name deployment profiles by what they store and run

Status: Accepted

## Context

Zinder deployments serve different consumers. The wallet deployment runs native `WalletQuery`, the wallet projector, and wallet-mode lightwalletd compatibility, so it keeps transaction bodies and a local transparent index. A deployment for light wallets needs shielded history, tree states, subtree roots, and a few transparent reads. On Mainnet at height 3,593,347, transaction bodies are about 251 GiB of the canonical store, and the wallet projection adds about 47 GiB.

Each runtime used to be configured separately. A light-wallet deployment had to set ingest coverage, raw-blob retention, Explorer views, and the compatibility serving mode, and keep them consistent across processes by hand. Settings that disagreed surfaced only when the reader opened the store the writer had built.

A name for a deployment shape must not become a second source of truth. Capabilities follow the composed service ([ADR-0015](0015-wallet-runtime-composition.md)), readiness follows each runtime's admission ([ADR-0016](0016-wallet-serving-readiness.md)), and store admission compares persisted evidence ([ADR-0004](0004-storage-and-deployment-topologies.md)).

## Decision

1. **A profile fixes what a deployment stores and runs.** `deployment.profile`, in the shared `[deployment]` section ([ADR-0021](0021-shared-configuration-sections.md)), names the profile for every runtime of one deployment. A profile fixes:
   - what the canonical writer stores: its coverage, raw-blob retention, and Explorer views;
   - which runtimes run and which composition each serves; and
   - the minimum data its readers require ([ADR-0004](0004-storage-and-deployment-topologies.md) decision 2).

   A setting the profile fixes may be repeated with the same value. A conflicting value fails at startup. `--print-config` shows the profile and omits the settings it supplies.
2. **A profile is not a capability, a readiness input, or a store identity.** Capabilities derive from the composition and its admitted evidence ([ADR-0015](0015-wallet-runtime-composition.md) decision 1). Readiness follows each runtime's admission rules ([ADR-0016](0016-wallet-serving-readiness.md)). A store records what it holds, not a profile name, and a reader admits any store that meets its requirements.
3. **The light-wallet profile serves lightwalletd clients without transaction bodies or a wallet projection.** Ingest builds complete wallet-serving history from height 1, with no raw blobs and no Explorer views. The deployment runs the standalone `zinder-ingest` and `zinder-compat-lightwalletd` and runs no projector or native query. The compatibility reader requires complete block history and a complete subtree-root prefix, and it also admits a store that retains blobs. It serves compact blocks, nullifiers, tree states, subtree roots, and the tip from the canonical store, and mempool methods through ingest control. It sends the reads in decision 4 to one node that keeps transparent address indexes.
4. **Only the light-wallet profile has node-backed reads.** Its node-backed methods are `GetTransaction` for a mined transaction whose bytes the store does not hold, `GetTaddressTxids`, `GetTaddressTransactions`, `GetTaddressBalance`, and `GetAddressUtxos`, with their stream forms. They follow [ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) decision 7. The wallet deployment, compact-blocks serving, and the Explorer never forward reads. Native `WalletQuery` offers no node-backed reads ([ADR-0014](0014-consumer-neutral-wallet-data-plane.md) decision 4).
5. **A profile omits data only when its trust checks still hold.**
   - A profile that forwards mined transaction bytes keeps, for every mined transaction in its history, the location and byte digest the writer recorded. Decision 4's transaction lookups depend on them.
   - Replay covers all retained history unless the store carries a writer-certified commitment over the settled prefix. Construction derives that commitment, and cold admission and restore verify it. With it, replay may shrink to the reorg window ([ADR-0010](0010-reorg-window-and-displaced-blocks.md)) plus the settled block below the window, which a replacement of the whole window reads as its parent. A profile whose runtimes read settled replay keeps it, as the wallet projector and Explorer views do.
6. **A profile states its measured cost.** A profile is offered to operators only after its Mainnet disk and construction time are measured. A named wallet is supported on a profile only after certification on it ([ADR-0015](0015-wallet-runtime-composition.md) decision 10).
7. **Other deployments keep their settings until they are named.** The wallet deployment, compact-blocks serving, and the Explorer are configured by their individual settings and have no profile name. Zinder intends to name them the wallet, block-feed, and Explorer profiles. Each is named only when decision 1 fixes its stored data, runtimes, and reader requirements, and decision 6 holds for it.

[Configuration](../reference/configuration.md) lists the settings, and [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md#runtime-serving-selection) describes each serving mode.

## Consequences

- An operator sets one value for ingest and compatibility instead of several settings that must agree.
- On Mainnet at height 3,593,347, the light-wallet canonical store measured 50.1 GiB and reached READY from an empty store in 1 h 51 min. Its largest parts are compact blocks (27.2 GiB) and replay (20.1 GiB).
- Forwarding suits shielded scanning and not transparent-heavy wallets. Transaction enhancement during a shielded scan added tens of milliseconds at p95 over the wallet deployment. Forwarded address history costs about two node calls per returned row, and on Regtest a wallet workload with a busy transparent address had a p95 more than 30 times the wallet deployment's. A deployment whose wallets are transparent-heavy runs the wallet deployment instead.
- Node-backed answers are bound to the served tip, not pinned, and address history is best-effort ([ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) decision 7). During a node outage, the node-backed methods fail and shielded scanning continues ([ADR-0016](0016-wallet-serving-readiness.md)).
- The profile runs from the release's standalone binaries and needs no new executable ([ADR-0022](0022-release-artifacts.md)). The bundled wallet-serving image runs the wallet deployment and does not take a profile.
- Because a store records no profile, a reader that needs less can open a richer store. A writer that should store less needs a new store.

## Alternatives rejected

- **Record the profile name in the store.** It would be a second identity that could disagree with the data the store holds, and a richer store could not serve a reader that needs less.
- **Capability profiles.** A name that declares capabilities keeps a shadow model of the composition ([ADR-0015](0015-wallet-runtime-composition.md)).
- **Keep the light-wallet composition behind a build feature.** Releases ship one reproducible build of each binary ([ADR-0022](0022-release-artifacts.md)), so a feature-gated composition never reaches operators.
- **Forward every read the store lacks.** It is the node fallback that [ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) and [ADR-0014](0014-consumer-neutral-wallet-data-plane.md) reject. Each node-backed method is declared, and each has its own binding to the served tip.
- **Forward transparent reads in the wallet deployment.** Its consumers need exact transparent history and spend status at one epoch, which a local index gives and the node does not.
