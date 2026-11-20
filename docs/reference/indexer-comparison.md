# Zcash Indexer Comparison

This page compares Zebra and Zakura, two validators that also serve chain
data, with four indexers that serve it from a validator: ztreamer, Zingod, Go
lightwalletd, and Zinder. Use the tables to check which project has a
capability, what constrains a deployment, and which consumers one deployment
can serve.

Each matrix cell holds one of four values:

- **Yes**: the reviewed source provides the capability in a default build. A
  listener may still need a listen address in its configuration, and failover
  needs more than one validator endpoint listed there.
- **Optional**: the capability needs a non-default build feature, mode, index,
  flag, setting, or runtime.
- **Experimental**: the project provides the capability and labels it
  experimental.
- **No**: the reviewed source does not provide the capability.

A few cells read **n/a**, where a row cannot apply to that project, or
**Unknown**, where the source review could not settle the answer. A footnote
after a value names the exact condition and links to the source line.

On this page:

- [What each project is](#what-each-project-is)
- [Validator requirements](#validator-requirements)
- [Interfaces](#interfaces)
- [Chain data](#chain-data)
- [Lightwalletd methods](#lightwalletd-methods)
- [When the validator is down](#when-the-validator-is-down)
- [On a pruned validator](#on-a-pruned-validator)
- [Operations](#operations) and [Deployment facts](#deployment-facts)
- [Consumer fit](#consumer-fit)
- [Strengths and limits](#strengths-and-limits)
- [Source revisions](#source-revisions)

## What each project is

| Fact | Zebra | Zakura | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- | --- | --- |
| Kind | Validator with built-in serving | Validator with built-in serving | Indexer on a Zakura validator, which it can also run in its own process (embedded mode, the default) | Indexer on a validator | Indexer on a validator | Indexer on a validator |
| Version reviewed | zebrad 7.1.0 | zakurad 1.8.9 | 0.1.0, bundling Zakura 1.8.4 | 1.1.0 | 0.6.1 | 0.19.0 |

ztreamer runs in one of two modes. Embedded mode, the default, runs a Zakura
validator inside the ztreamer process. Remote mode follows one separate Zakura
node. The ztreamer column describes what the indexer itself provides, as a
component that follows a validator, and a footnote names any difference in
embedded mode. In embedded mode, the bundled Zakura serves its own node
JSON-RPC and Indexer gRPC once their listen addresses are set, and those APIs
appear in the Zakura column
([README.md](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/README.md#L52-L57),
[Cargo.toml](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/Cargo.toml#L40)).

## Validator requirements

Zebra and Zakura are the validators, so they have no column here.

| Requirement | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- |
| Runs on Zebra | Unknown[^ztr-zebra] | Yes | Yes | Yes[^zinder-certified] |
| Runs on Zakura | Yes | Yes[^zingod-zakura] | Yes | Yes[^zinder-certified] |
| Runs on zcashd | Unknown[^ztr-zcashd] | Yes[^zingod-zcashd] | Yes[^lwd-zcashd] | No |
| Fails over between several validators | No[^ztr-embedded] | Yes[^zingod-failover] | No | Yes[^zinder-failover] |
| Broadcast fails over between validators | No[^ztr-embedded] | Yes[^zingod-broadcast-failover] | No | Yes[^zinder-broadcast-failover] |
| Builds its index from a pruned validator | No[^ztr-pruned] | No[^zingod-pruned] | No[^lwd-pruned] | No[^zinder-archive] |

ztreamer and Go lightwalletd each follow one validator, and ztreamer is the
only indexer here that can also run that validator in its own process. Zingod
accepts 1 to 64 validators.
zinder-ingest follows one validator at a time from an ordered list of `[node]`
and optional `[node_fallbacks.N]` endpoints. zinder-query and
zinder-compat-lightwalletd use their own node list only to broadcast, and
zinder-explorer reads only `[node]`.

## Interfaces

Each column counts only the APIs that the project itself serves.

| API | Zebra | Zakura | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- | --- | --- |
| Node JSON-RPC | Yes | Yes | No | No | No | No |
| Node Indexer gRPC | Yes | Yes | No | No | No | No |
| Lightwalletd gRPC (`CompactTxStreamer`) | Experimental[^zebra-lwd-exp] | No | Yes[^ztr-p2p] | Yes | Yes | Yes |
| Native wallet API beyond lightwalletd gRPC | No | No | No | No | No | Yes |
| Explorer API | No | No | No | No | No | Optional[^zinder-explorer][^zinder-explorer-auth] |
| Resumable reorg-aware event cursor | No[^zebra-events] | No[^zakura-events] | No | No | No | Yes[^zinder-retention] |
| Pinned reads across requests | No | No | No | No[^zingod-pinned] | No | Yes[^zinder-pinned] |
| Index replication or snapshot serving | No | No | Yes[^ztr-replicator] | No | No | No |

Zebra and Zakura start with node JSON-RPC and Indexer gRPC off. Each listener
serves once you set its listen address.

## Chain data

| Data | Zebra | Zakura | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- | --- | --- |
| Compact blocks | Experimental[^zebra-lwd-exp] | No | Yes | Yes | Yes | Yes |
| Full block by height or hash | Yes | Yes | No | No | No | Optional[^zinder-raw-all] |
| Full block range stream | No | Optional[^zakura-range] | No | No | No | Optional[^zinder-raw-all] |
| Tree states and subtree roots | Yes | Yes | Yes[^forwarded] | Yes | Yes[^forwarded] | Yes[^zinder-tree-local] |
| Mined raw transaction by ID | Yes | Yes | Yes[^forwarded] | Yes[^forwarded] | Yes[^forwarded] | Yes |
| Transparent address history, UTXOs, and balance | Yes | Yes | Yes[^forwarded] | Yes[^forwarded] | Yes[^forwarded] | Yes[^zinder-taddr-stream] |
| Mined spender of a transparent outpoint | No | No | No | No | No | Yes[^zinder-outpoint] |
| Mempool snapshot and stream | Yes | Yes | Yes[^forwarded] | Yes | Yes[^forwarded] | Yes |
| Broadcast a signed transaction | Yes | Yes | Yes[^forwarded] | Yes[^forwarded] | Yes[^forwarded] | Yes[^forwarded] |
| Block templates and block submission | Optional[^zebra-miner] | Optional[^zakura-miner] | No | No | No | No |
| Explorer search by transaction ID, height, hash, or address | No | No | No | No | No | Optional[^zinder-search] |
| Aggregate views across blocks (fees, pools, supply) | No[^zebra-derived] | No[^zakura-derived] | No | No | No | Optional[^zinder-explorer] |

Shielded data covers the Sapling, Orchard, and Ironwood pools in every
project.

## Lightwalletd methods

Zakura has no lightwalletd gRPC listener and no column here; its Docker
Compose example runs a separate lightwalletd beside it
([docker-compose.lwd.yml](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/docker/docker-compose.lwd.yml#L20-L23)).

Each row is a `CompactTxStreamer` method, except the last row, which shows
whether compact blocks can carry transparent inputs and outputs.

| Method | Zebra | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- | --- |
| `GetLatestBlock` | Experimental | Yes | Yes | Yes | Yes |
| `GetBlock` | Experimental | Yes | Yes | Yes[^lwd-height] | Yes |
| `GetBlockNullifiers` | Experimental | Yes | No[^zingod-unimplemented] | Yes[^lwd-height] | Yes |
| `GetBlockRange` | Experimental | Yes | Yes | Yes | Yes |
| `GetBlockRangeNullifiers` | Experimental | Yes | Yes | Yes | Yes |
| `GetTransaction` | Experimental | Yes | Yes | Yes | Yes |
| `SendTransaction` | Experimental | Yes | Yes | Yes | Yes |
| `GetTaddressTxids` | Experimental | Yes | Yes | Yes | Yes |
| `GetTaddressTransactions` | Experimental | Yes | Yes | Yes | Yes |
| `GetTaddressBalance` | Experimental | Yes | Yes | Yes | Yes |
| `GetTaddressBalanceStream` | Experimental | Yes | Yes | Yes | Yes |
| `GetMempoolTx` | Experimental | Yes | Yes | Yes | Yes |
| `GetMempoolStream` | Experimental | Yes | Yes | Yes | Yes |
| `GetTreeState` | Experimental | Yes | Yes | Yes | Yes |
| `GetLatestTreeState` | Experimental | Yes | Yes | Yes | Yes |
| `GetSubtreeRoots` | Experimental | Yes | Yes | Yes | Yes |
| `GetAddressUtxos` | Experimental | Yes | Yes | Yes | Yes |
| `GetAddressUtxosStream` | Experimental | Yes | Yes | Yes | Yes |
| `GetLightdInfo` | Experimental | Yes | Yes | Yes | Yes |
| `GetStandardFee` | No | Yes | Yes | Yes | No[^zinder-standard-fee] |
| `Ping` | No | Optional[^ping] | No[^zingod-unimplemented] | Optional[^ping] | Optional[^zinder-ping] |
| Transparent inputs and outputs in compact blocks | No | Yes[^transparent-pool] | Yes[^transparent-pool] | Yes[^transparent-pool] | Yes[^transparent-pool] |

Zebra labels its whole lightwalletd gRPC listener experimental.[^zebra-lwd-exp]
ztreamer serves the same methods in both modes. In its default `wallet` serving
mode, Zinder serves every method except `GetStandardFee` (which returns
`UNIMPLEMENTED`), and answers `Ping` only with `compat.ping_very_insecure`.
[Runtime serving selection](lightwalletd-compatibility.md#runtime-serving-selection)
describes Zinder's narrower `compact-blocks` mode.

## When the validator is down

Zebra and Zakura serve from the validator process, so all their serving stops
with the node, and this table omits them. For Zingod and Zinder, which can fail
over, each row assumes that every configured validator is down.

| Capability | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- |
| Serves compact blocks it already indexed | Yes[^ztr-embedded] | Yes | Yes[^lwd-cached] | Yes[^zinder-down] |
| Reports the current tip to wallets | No[^ztr-tip] | Optional[^zingod-p2p] | No | No[^zinder-tip-down] |
| Serves tree state | Yes[^ztr-tree-down][^ztr-embedded] | Yes | No | Yes[^zinder-down] |
| Serves transparent address queries | No | Yes[^zingod-taddr-down] | No | Yes[^zinder-down] |
| Broadcasts transactions | No | Optional[^zingod-p2p] | No | No[^zinder-broadcast-down] |
| Readiness endpoint stays ready | No | Optional[^zingod-ready] | No[^lwd-no-ready] | Yes[^zinder-down][^zinder-ready] |
| Readiness reports a stale chain tip | Yes[^ztr-stale] | Yes[^zingod-stale] | No[^lwd-stale] | Yes[^zinder-stale] |
| Recovers without a restart when the validator returns | Yes[^ztr-embedded] | Yes | Yes[^lwd-retries] | Yes[^zinder-down] |
| Serving starts while the validator is down | No[^ztr-remote-start][^ztr-embedded] | Optional[^zingod-start-down] | No[^lwd-start-down] | No[^zinder-reader-start] |

A project with validator failover keeps advancing its tip and broadcasting
while a configured validator that it accepts answers. The failover footnotes
list each project's conditions.

## On a pruned validator

Zakura is the only reviewed validator with a pruned mode. You turn it on with
`state.storage_mode = pruned`, and the default is `archive`
([config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-state/src/config.rs#L210-L229)). Zebra has
no pruned mode, so this table omits it
([methods.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/methods.rs#L4046-L4047)).

Each indexer column assumes that the validator is a pruned Zakura. The Zakura
column describes the pruned node itself, and reads n/a for rows about an
indexer's own index. ztreamer behaves the same whether the pruned Zakura runs
separately or inside the ztreamer process.

| Capability | Zakura | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- | --- |
| Builds a fresh index from the pruned validator | n/a | No[^ztr-pruned] | No[^zingod-pruned] | No[^lwd-pruned] | No[^zinder-archive] |
| Keeps serving an index built before the validator pruned | n/a | Yes[^ztr-pruned] | Yes[^zingod-built-before] | Yes[^lwd-built-before] | No[^zinder-pruned-later] |
| Shielded sync from heights indexed before pruning | n/a | Yes[^ztr-built-before] | Yes[^zingod-built-before] | Yes[^lwd-built-before] | No[^zinder-pruned-later] |
| Complete transparent address history | Optional[^zakura-checkpoint] | No[^ztr-taddr-pruned] | No[^zingod-taddr-pruned] | No[^lwd-taddr-pruned] | No[^zinder-pruned-later] |
| Historical raw transactions | No[^zakura-tx-retention] | No[^raw-forwarded] | No[^raw-forwarded] | No[^raw-forwarded] | No[^zinder-pruned-later] |
| Historical tree state | Optional[^zakura-vct] | Yes[^ztr-built-before][^ztr-tree-index] | Yes[^zingod-built-before] | Optional[^lwd-tree-pruned] | No[^zinder-pruned-later] |

## Operations

| Capability | Zebra | Zakura | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- | --- | --- |
| Built-in TLS on the lightwalletd gRPC listener | No[^zebra-tls] | No[^zakura-tls] | Optional[^ztr-tls] | Yes[^zingod-tls] | Yes[^lwd-tls] | No[^zinder-tls] |
| Client authentication on node JSON-RPC | Yes[^zebra-cookie] | Yes[^zakura-cookie] | n/a | n/a | n/a | n/a |
| Client authentication on node Indexer gRPC | No[^zebra-grpc-no-auth] | Optional[^zakura-indexer-tls] | n/a | n/a | n/a | n/a |
| Client authentication on lightwalletd gRPC | No[^zebra-grpc-no-auth] | n/a | No[^ztr-auth] | No | No | No |
| Health or readiness endpoint | Yes | Yes | Yes | Yes | No | Yes |
| Prometheus metrics | Yes | Yes | Yes | Optional[^zingod-metrics] | Yes | Yes |
| Serving restarts without restarting the validator | No | No | Yes[^ztr-embedded] | Yes | Yes[^lwd-retries] | Yes[^zinder-reader-start] |
| Several serving processes over one index | No | No | No | No | No | Yes[^zinder-secondaries] |
| Read replicas on other hosts | No | No | Yes[^ztr-replica] | No | No | No[^zinder-single-host] |
| Bootstrap from a state or index snapshot | Yes[^zebra-snap] | Yes[^zakura-snap] | Yes[^ztr-snap] | Optional[^zingod-snap] | No | No[^zinder-snap] |
| Published release binaries or images | Yes | Yes | No | Yes[^zingod-image] | Yes[^lwd-image] | Yes |

All six enforce built-in request limits. ztreamer and Go lightwalletd serve
metrics by default, Zinder serves them by default on a loopback ops address,
and Zebra and Zakura serve them once you set a metrics listen address
([public-interfaces.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/public-interfaces.md#L361)).

### Deployment facts

| Fact | Zebra | Zakura | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- | --- | --- |
| Processes, including the validator | 1: zebrad | 1: zakurad | 2: ztreamerd and its Zakura[^ztr-embedded] | 2: zingod and its validator | 2: lightwalletd and its validator | 5, or 6 with zinder-explorer[^zinder-processes] |
| Validators per deployment | 1, built in | 1, built in | 1 | 1 to 64[^zingod-failover] | 1 | 1 active, plus any ordered fallbacks[^zinder-node-targets] |
| Mainnet disk | About 300 GB[^zebra-disk] | 275 GiB minimum[^zakura-disk] | 17.3 GiB index, plus Zakura state[^ztr-disk] | Not stated[^zingod-disk] | Not stated | About 465 GB, including Explorer views[^zinder-disk] |
| First sync | Days from genesis, or a snapshot[^zebra-sync] | Checkpoint fast sync or snapshot; time not stated[^zakura-sync] | Imports a signed snapshot; time not stated[^ztr-sync] | From genesis or snapshot; time not stated[^zingod-snap] | From height 0, about an hour[^lwd-sync] | About 16 hours from genesis[^zinder-sync] |
| Built-in request limits | Stream, size, and count caps; no rate limit[^zebra-limits] | Size, batch, connection, and stream caps[^zakura-limits] | Stream, range, address, and size caps[^ztr-limits] | Connection, stream, and admission caps; no rate limit[^zingod-limits] | Stream, range, address, span, and size caps[^lwd-limits] | Stream, response, address, and rebuild caps; capped requests wait in line[^zinder-limits] |
| What a client sees on a reorg | No reorg event; some reads return `ABORTED`[^zebra-reorg] | New tip and fork blocks; no revert event[^zakura-reorg] | Affected streams fail with `UNAVAILABLE`[^ztr-reorg] | Tip drops to the fork point; no event[^zingod-reorg] | No reorg event; `GetBlockRange` may return `ABORTED`[^lwd-reorg] | `ChainReorged` event; `GetBlockRange` ends with `ABORTED`[^zinder-reorg] |

## Consumer fit

Each cell answers one question: does one deployment of this project serve this
consumer without the application building its own index?

| Consumer | Zebra | Zakura | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- | --- | --- |
| Light wallet over lightwalletd gRPC | Experimental[^zebra-lwd-exp] | No | Yes | Yes | Yes | Yes |
| Wallet backend needing pinned reads across requests | No | No | No | No | No | Yes[^zinder-pinned] |
| Explorer needing block, transaction, and address lookups | Yes | Yes | No | No | No | Optional[^zinder-raw-all] |
| Explorer needing search and aggregate views across blocks (fees, pools, supply) | No | No | No | No | No | Optional[^zinder-explorer] |
| Deposit or payment tracking needing a resumable reorg-aware event cursor | No | No | No | No | No | Yes[^zinder-retention] |
| Miner or mining faucet needing block templates | Optional[^zebra-miner] | Optional[^zakura-miner] | No | No | No | No |
| Light wallet, explorer with aggregate views, and deposit tracking from one deployment | No | No | No | No | No | Optional[^zinder-explorer] |

Each row takes the weakest value among the table rows it depends on, ranked
No, Experimental, Optional, Yes. Experimental ranks below Optional because
Zebra warns that its listener may break clients in any release.

- The light wallet row repeats Lightwalletd gRPC.
- The wallet backend row repeats Pinned reads across requests.
- The explorer lookup row combines Full block by height or hash, Mined raw
  transaction by ID, and Transparent address history, UTXOs, and balance.
- The explorer search row combines Explorer search by transaction ID, height,
  hash, or address with Aggregate views across blocks (fees, pools, supply).
- The deposit row repeats Resumable reorg-aware event cursor.
- The miner row repeats Block templates and block submission.
- The last row combines the light wallet, explorer search, and deposit rows.

For every project, wallet keys, scanning, signing, and invoice logic stay in
the application.

## Strengths and limits

### Zebra

Strengths:

- The validator and every listener run in one process, with no separate indexer to operate.
- Node JSON-RPC serves full blocks, tree states, and transparent address data.
- Health, readiness, and Prometheus metrics endpoints are built in.
- The project publishes release binaries, images, and state snapshots.

Limits:

- Lightwalletd gRPC is experimental, and its compact blocks omit transparent inputs and outputs.
- The lightwalletd gRPC listener has no built-in TLS, and neither gRPC listener authenticates clients.
- Serving stops whenever the validator stops.
- No pinned reads across requests, resumable reorg-aware event cursor, or read replicas on other hosts.

### Zakura

Strengths:

- Node JSON-RPC serves full blocks, tree states, subtree roots, and transparent address data.
- Node Indexer gRPC streams tip, block, and mempool changes, and can authenticate clients by certificate.
- Has a pruned mode, and the project publishes state snapshots for bootstrap.
- Health, readiness, and Prometheus metrics endpoints are built in.

Limits:

- No lightwalletd gRPC or compact blocks, so light wallets need a separate server.
- The full block range stream needs the non-default `indexer` build feature.
- Serving stops whenever the validator stops.
- When pruned, it deletes historical raw transactions, and complete address history and, on Mainnet, historical tree states need non-default sync settings.

### ztreamer

Strengths:

- Serves every lightwalletd gRPC method, including transparent inputs and outputs in compact blocks; `Ping` needs a testing-only flag.
- Embedded mode, the default, runs the indexer and its Zakura validator as one process.
- Remote mode keeps serving indexed compact blocks and tree states while Zakura is down, and recovers without a restart.
- Index replication and signed snapshots let remote instances bootstrap on other hosts.

Limits:

- Embedded mode runs only Zakura, and remote mode is tested only against Zakura; Zebra and zcashd are unverified.
- Forwards transparent address queries and broadcasts to Zakura, so they fail while it is down, and in embedded mode all serving stops with Zakura.
- No full blocks or explorer API of its own, and the lightwalletd gRPC listener does not authenticate clients.
- Version 0.1.0 has no published binaries or images.

### Zingod

Strengths:

- Fails over between up to 64 validators in priority tiers.
- Keeps serving indexed compact blocks, tree states, and transparent address queries while every validator is down.
- The lightwalletd gRPC listener has built-in TLS, which non-loopback addresses require.
- Behind a pruned validator, it serves shielded sync and historical tree states from its own indexes once they exist.

Limits:

- No full blocks: `GetBlock` and `GetBlockRange` return compact blocks only.
- `GetBlockNullifiers` and `Ping` return `UNIMPLEMENTED`.
- No client authentication, no index shared across serving processes, and no read replicas on other hosts.
- Raw transaction bytes come from validators, so a pruned validator cannot supply complete transparent address history.

### Go lightwalletd

Strengths:

- Runs on Zebra, Zakura, and deprecated zcashd.
- The lightwalletd gRPC listener has built-in TLS, required by default.
- Serves compact blocks already in its cache while the validator is down, and recovers without a restart.
- Serves Prometheus metrics by default, and the project publishes Docker images.

Limits:

- The current tip, tree state, transparent address queries, and broadcasts fail while the validator is down.
- No full blocks, explorer API, or block lookup by hash.
- No health or readiness endpoint, and no client authentication.
- Follows one validator without failover, and a fresh cache cannot build from a pruned validator.

### Zinder

Strengths:

- In its default `wallet` mode, serves every method of its vendored lightwalletd protocol, with `Ping` behind `compat.ping_very_insecure`, plus a native wallet API with pinned reads, a resumable reorg-aware event cursor, and the mined spender of a transparent outpoint.
- The native wallet API, lightwalletd gRPC, and the optional zinder-explorer, with search and aggregate views for fees, pools, and supply, run as separate processes over one index.
- Keeps serving indexed compact blocks, tree states, and transparent address queries, and stays ready while reporting the served chain's age, while every validator is down and zinder-ingest keeps running.
- When more than one validator is configured, zinder-ingest fails over between them while it follows the chain, and a broadcast moves to the next verified validator within the request.

Limits:

- Alpha: its APIs, storage layouts, and deployment topology will change.
- zinder-ingest needs an archive validator and is the only writer, and the runtimes share one host with no read replicas or snapshot bootstrap.
- Serving depends on zinder-ingest, which cannot start without a validator: readers stop five minutes after ingest stops, and while every validator is down the served tip stops advancing and broadcasts fail.
- No built-in TLS, no client authentication on the wallet listeners, and no `GetStandardFee`, while full blocks need a non-default setting and Explorer views need the Explorer image or a non-default setting.

## Source revisions

The tables are source evidence from the revisions below. Zinder was
re-inspected on 2026-11-18, and its tree-state, request-limit, and
transparent-streaming footnotes on 2026-11-19. The other five revisions were
inspected on 2026-11-17. They are not an interoperability test. Published
builds and enabled features can differ. Check the target build, and exercise your client's sync,
restore, and send flows before you rely on a compatibility claim.
[Zinder compatibility evidence](lightwalletd-compatibility.md#claim-boundary)
describes the levels of evidence behind Zinder's own compatibility claims.

- [Zinder 0.19.0](https://github.com/ZcashFoundation/zinder/releases/tag/v0.19.0): the tree-state, request-limit, and transparent-streaming footnotes at [`ddd90e8`](https://github.com/ZcashFoundation/zinder/commit/ddd90e89def3376cc77ccff3b5806383585718fc), whose code `v0.19.0` releases unchanged, and the other cells at [`c293095`](https://github.com/ZcashFoundation/zinder/commit/c293095dfab045e731cbe3e6e7f8b5dce4576fdb)
- [Zebra `main` at `86f3ac7`](https://github.com/ZcashFoundation/zebra/commit/86f3ac7e4dcf7644a88222b8678790ed68734e63)
- [Zakura `main` at `dd25aa4`](https://github.com/zakura-core/zakura/commit/dd25aa429cd97eb1af903b7bc6375fe208132015)
- [ztreamer `master` at `39b2d12`](https://github.com/zcashlabs/ztreamer/commit/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4)
- [Zingod `main` at `3664ce0`](https://github.com/zingolabs/zingod/commit/3664ce0bbea3f1945c84c8730359795b84959ed2)
- [Go lightwalletd `master` at `faaf7aa`](https://github.com/zcash/lightwalletd/commit/faaf7aa1f917f41ad41def61d7095330078c8a8d)

[^ztr-zebra]: Unverified: remote mode calls 14 node RPCs that all exist in Zebra 7.1.0, but ztreamer does not check the node's identity, and no test runs it against Zebra. Embedded mode runs only its bundled Zakura ([methods.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/remote/src/methods.rs#L66-L85), [config.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/config.rs#L39-L51)).

[^ztr-zcashd]: Unverified: ztreamer does not document or test zcashd and does not check zcashd's response shapes. Embedded mode runs only its bundled Zakura ([README.md](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/README.md#L10), [methods.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/remote/src/methods.rs#L1-L5), [config.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/config.rs#L39-L51)).

[^zinder-certified]: Zinder's live and parity lanes certify Zebra 7.1.0 and Zakura 1.8.6 ([lightwalletd-compatibility.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/reference/lightwalletd-compatibility.md#L647-L663)).

[^zingod-zakura]: Zingod's documentation and configuration name Zakura, but its live tests run only against Zebra ([README.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/README.md#L3-L5)).

[^zingod-zcashd]: The default `legacy_rpc` source claims zcashd support, but the README names only Zebra and Zakura, and no live test runs zcashd ([configuration.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/operators/configuration.md#L43), [legacy_rpc.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-source/src/legacy_rpc.rs#L1)).

[^lwd-zcashd]: zcashd support is deprecated and needs zcashd's experimental `lightwalletd` or `insightexplorer` feature ([root.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/cmd/root.go#L285-L307)).

[^zingod-failover]: When more than one validator is configured, a failed request goes to the next member of up to 64 `[[trusted_validators]]` in priority tiers. A member is marked Down after 10 consecutive failures and probed every 30 seconds, and the tiers restore the preferred validator once it answers ([balancer.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/src/balancer.rs#L204-L264), [core.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/src/core.rs#L8), [core.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/src/core.rs#L323), [core.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/src/core.rs#L447), [member.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/src/member.rs#L113), [usage.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/usage.md#L91-L121), [usage.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/usage.md#L33-L35), [member.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/src/member.rs#L14), [configuration.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/operators/configuration.md#L35)).

[^zinder-failover]: When more than one validator is configured, through `[node_fallbacks.N]` after `[node]`, zinder-ingest switches to another qualifying endpoint after three consecutive recoveries of one class, unreachable or disconnected, or after its validator has reported not ready for five minutes. It switches only while it follows the chain, so a first sync does not switch, there is no timed failback, and a fallback that advertises another network-upgrade table is excluded. zinder-explorer reads only `[node]` ([chain-ingestion.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/chain-ingestion.md#L238-L321), [ADR-0013](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/adrs/0013-source-failure-recovery-topology.md#L27-L35), [node-source-boundary.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/node-source-boundary.md#L138-L143)).

[^zingod-broadcast-failover]: When more than one validator is configured and `[p2p]` is off, the default, a push that cannot reach its validator moves to another untried validator, chosen at random, for up to 4 attempts (`max_attempts`). With `[p2p]` on, Zingod allows only one fallback push to a trusted validator ([policy.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-chainview/src/submit/policy.rs#L183-L206), [policy.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-chainview/src/submit/policy.rs#L36), [driver.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-chainview/src/submit/driver.rs#L108), [driver.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-chainview/src/submit/driver.rs#L165), [config.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/config.rs#L485), [architecture.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/contributors/architecture.md#L249)).

[^zinder-broadcast-failover]: When more than one validator is configured, zinder-query and wallet-mode zinder-compat-lightwalletd send a transaction on to the next verified endpoint when one is unreachable, including after its `broadcast_timeout_secs`, or rejects the transaction's consensus branch. An endpoint is used only after it passes a first-contact check of its methods, network-upgrade table, and chain, and the endpoint that answered stays preferred, with no timed failback ([service-operations.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/service-operations.md#L452-L477), [node-source-boundary.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/node-source-boundary.md#L209-L223)).

[^ztr-embedded]: In embedded mode, the default, ztreamerd runs its Zakura validator as a task in its own process, so a deployment is one process, with no separate validator to switch to or to start without, and transactions go to that node's own mempool. When Zakura or any other task stops, the whole daemon shuts down, so in that mode the indexer serves nothing while its validator is down and restarts together with it ([embedded.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/embedded.rs#L1-L3), [embedded.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/embedded.rs#L78-L100), [config.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/config.rs#L39-L51), [submission.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/embedded/src/submission.rs#L10-L16), [lifecycle/mod.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/lifecycle/mod.rs#L109-L132)).

[^ztr-pruned]: ztreamer refuses to start unless its existing index already reaches Zakura's prune height ([pipeline/mod.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/sync/src/pipeline/mod.rs#L293-L295), [replicate/mod.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/replicate/mod.rs#L138-L149)).

[^zingod-pruned]: Zingod does not detect pruning, so a fresh index stalls at heights the validator lacks ([balancer.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/src/balancer.rs#L230-L264)).

[^lwd-pruned]: Go lightwalletd does not check for pruning, so a fresh cache starts at height 0 and stalls where blocks are missing ([root.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/cmd/root.go#L345-L349), [common.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/common/common.go#L556-L562)).

[^zinder-archive]: zinder-ingest needs historical block bodies, so at startup it refuses a lone `[node]` that reports pruned storage or does not report its storage mode, and with `[node_fallbacks.N]` it excludes an endpoint that reports pruned storage until restart ([admission.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-ingest/src/node_failover/admission.rs#L18-L26), [admission.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-ingest/src/node_failover/admission.rs#L148-L165), [main.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-ingest/src/main.rs#L1222-L1230)).

[^zebra-lwd-exp]: Zebra labels its lightwalletd gRPC listener experimental and warns that it may break clients in any release ([rpc.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/config/rpc.rs#L58-L62)).

[^ztr-p2p]: Embedded mode also serves `CompactTxStreamer` from its index to Zakura's v2 P2P peers, through a Zakura custom service ([README.md](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/README.md#L54-L56), [embedded.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/embedded.rs#L81-L84), [lib.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/p2p/src/lib.rs#L1)).

[^zebra-events]: The Indexer `NonFinalizedStateChange` stream resumes from known tips, but it covers only non-finalized blocks and sends no undo events ([indexer.proto](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/proto/indexer.proto#L73-L78)).

[^zakura-events]: The Indexer `NonFinalizedStateChange` stream resumes from known tips, but it sends no revert events and keeps no durable cursor ([indexer.proto](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/proto/indexer.proto#L74-L87)).

[^zinder-retention]: Through `WalletQuery.ChainEvents`, which keeps events for 168 hours by default ([chain-events.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/chain-events.md#L87-L91), [retention.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/crates/zinder-runtime/src/sections/retention.rs#L21)).

[^zingod-pinned]: Zingod pins one snapshot per request or stream only ([service.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc/src/service.rs#L5-L6)).

[^zinder-pinned]: A pinned chain view stays readable for one minute after a newer one replaces it ([wallet_serving_pair_publisher.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-query/src/wallet_serving_pair_publisher.rs#L86-L92)).

[^zinder-explorer]: Needs the optional zinder-explorer runtime, and most views, including fees, pools, and supply, also need `ingest.explorer_views = true`, which is off by default except in the `zinder-wallet-serving-explorer` image ([explorer-plane.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/explorer-plane.md#L1-L9), [public-interfaces.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/public-interfaces.md#L415), [ingest.toml](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/deploy/config/ingest.toml#L23), [Dockerfile](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/deploy/Dockerfile#L314-L325)).

[^ztr-replicator]: Replicator gRPC on the lightwalletd gRPC listener, plus the `export-snapshot` subcommand ([launcher.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/launcher.rs#L183-L192), [replicate.proto](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/protocol/proto/ztreamer/replicate.proto#L9-L16)).

[^zakura-range]: Indexer `GetBlockRange` needs the `indexer` build feature, which default and Docker builds leave out ([methods.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/indexer/methods.rs#L415-L423), [Cargo.toml](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakurad/Cargo.toml#L62-L97)).

[^zinder-raw-all]: Needs `storage.raw_blob_policy = "all"`, while the shipped configurations use `"transactions"` ([native_wallet_endpoint_capabilities.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-query/src/native_wallet_endpoint_capabilities.rs#L108-L113), [ingest.toml](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/deploy/config/ingest.toml#L19)).

[^zinder-taddr-stream]: Served from Zinder's own index with no validator call, at one chain epoch per request. `GetTaddressTxids` and `GetTaddressTransactions` read history 1,000 entries at a time and send each transaction as they read it, at most 8 ahead of the client. `GetAddressUtxosStream` sends unspent outputs as it merges them, at most 64 ahead, and reads each address in pages that double as rows are sent, from a first page of an even share of 8,192 outputs, from 16 to 1,024. One response returns at most 100,000 outputs ([`GetTaddressTxids`](lightwalletd-compatibility.md#gettaddresstxids-and-gettaddresstransactions), [`GetAddressUtxos`](lightwalletd-compatibility.md#getaddressutxos-and-getaddressutxosstream), [grpc.rs](https://github.com/ZcashFoundation/zinder/blob/ddd90e89def3376cc77ccff3b5806383585718fc/services/zinder-compat-lightwalletd/src/grpc.rs#L89-L98)).

[^zinder-outpoint]: Through the native `WalletQuery.TransparentOutpointSpendStatus` method only ([wallet.proto](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/crates/zinder-proto/proto/zinder/v1/wallet/wallet.proto#L1553)).

[^zebra-miner]: `getblocktemplate` needs `mining.miner_address`, which is unset by default, and `submitblock` works without it ([get_block_template.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/methods/types/get_block_template.rs#L513-L517), [mining.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/config/mining.rs#L34-L41)).

[^zakura-miner]: `getblocktemplate` needs `mining.miner_address`, which is unset by default ([methods.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/methods.rs#L3560-L3563), [mining.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/config/mining.rs#L103)).

[^zebra-derived]: `getblock` returns per-block value pools, chain supply, and, at verbosity 3, transaction fees, but no aggregates across blocks ([methods.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/methods.rs#L1843-L1871)).

[^zakura-derived]: `getblock` returns per-block value pools and chain supply, but no fees or aggregates ([methods.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/methods.rs#L2660-L2688)).

[^zinder-search]: Needs the optional zinder-explorer runtime with a `wallet_query_endpoint`, and it refuses shielded search inputs ([explorer_search.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/crates/zinder-core/src/explorer_search.rs#L50-L108)).

[^lwd-height]: By height only, and a block hash returns `InvalidArgument` ([GetBlock](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/frontend/service.go#L243-L264), [GetBlockNullifiers](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/frontend/service.go#L290-L312)).

[^zingod-unimplemented]: Zingod's vendored `service.proto` omits this method, so it returns `UNIMPLEMENTED` ([methods.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/wallets/methods.md#L113)).

[^ping]: Only with `--ping-very-insecure`, which ztreamer and Go lightwalletd both label for testing only ([ztreamer cli.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/bin/ztreamerd/src/cli.rs#L142-L144), [Go lightwalletd service.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/frontend/service.go#L1178-L1185)).

[^zinder-ping]: Needs `compat.ping_very_insecure = true` or `--ping-very-insecure`, which Zinder says to enable only on a server under load testing, and otherwise both serving modes refuse `Ping` with `FAILED_PRECONDITION` ([lightwalletd-compatibility.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/reference/lightwalletd-compatibility.md#L162-L175), [main.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-compat-lightwalletd/src/bin/zinder-compat-lightwalletd/main.rs#L101-L103)).

[^zinder-standard-fee]: Zinder vendors `lightwallet-protocol` v0.6.0 and returns `UNIMPLEMENTED` from `GetStandardFee` as specified for backends without a `getstandardfee` estimator ([lightwalletd-compatibility.md](lightwalletd-compatibility.md#methods-served)).

[^transparent-pool]: In `GetBlockRange`, only when the request names the transparent pool type, while Zingod's `GetBlock` always includes it and ztreamer fetches each full block from its validator ([ztreamer](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/service/src/compact/blocks/mod.rs#L92-L137), [Zingod](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/wallets/methods.md#L25-L26), [Go lightwalletd](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/common/common.go#L617-L668), [Zinder](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/reference/lightwalletd-compatibility.md#L194-L204)).

[^lwd-cached]: Only heights already in its cache, and other heights fail ([common.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/common/common.go#L590-L612)).

[^zinder-down]: Only while zinder-ingest keeps running, which it does through a validator outage: zinder-query and zinder-compat-lightwalletd stop serving five minutes (`storage.serving_pair_staleness_ceiling_ms`) after ingest stops, and after an ingest restart they refuse traffic until ingest reaches a validator. The bundled single-volume container terminates after 24 hours of validator outage and then serves nothing until a validator answers, because zinder-ingest needs one to start ([service-operations.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/service-operations.md#L326-L344), [ADR-0041](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/adrs/0041-reorg-window-follows-the-node-bound.md#L251-L254), [deploying-wallet-serving.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/runbooks/deploying-wallet-serving.md#L495-L517)).

[^ztr-tip]: `GetLatestBlock` returns `UNAVAILABLE` after 30 seconds without a successful head poll ([compact/mod.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/service/src/compact/mod.rs#L168-L174), [follower.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/sync/src/head/follower.rs#L98-L104)).

[^zinder-tip-down]: `GetLatestBlock` and `GetLightdInfo` answer with the last indexed tip while `estimatedHeight` keeps rising from it, and once that tip is 25 minutes old they fail with `UNAVAILABLE`, as lightwalletd's do, while stored data keeps serving. A tip that a reachable, in-sync node still reports is never refused ([Stale tip](lightwalletd-compatibility.md#stale-tip)).

[^zingod-p2p]: Needs `[p2p] enabled = true`, which is off by default ([config.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/config.rs#L482-L490)).

[^zinder-broadcast-down]: Broadcasts fail with `UNAVAILABLE` and `NODE_UNAVAILABLE` once every configured validator is unreachable, and readiness does not change ([service-operations.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/service-operations.md#L452-L469)).

[^ztr-tree-down]: From local trees up to the durable tip, while `GetLatestTreeState` fails ([tree.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/service/src/compact/tree.rs#L20-L35)).

[^zingod-taddr-down]: Balances and UTXOs come from local indexes, but address history also needs transaction bytes, which then need `[p2p] enabled = true` ([transactions.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc/src/routes/transactions.rs#L103-L126)).

[^zingod-ready]: With `[p2p] enabled = true`, `/readyz` stays at 200 and reports degraded, and by default it returns 503 once the tip is 24 blocks stale ([grpc_snapshot.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc-snapshot/src/grpc_snapshot.rs#L132-L152)).

[^lwd-no-ready]: Go lightwalletd has no readiness endpoint, and the README's suggested check, `GetLightdInfo`, needs the node ([README.md](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/README.md#L80-L88)).

[^zinder-ready]: zinder-query and zinder-compat-lightwalletd answer 200 and list the validator under `degraded`, zinder-explorer readiness does not depend on the validator, and zinder-ingest's own `/readyz` reports `node_unavailable` ([deploying-wallet-serving.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/runbooks/deploying-wallet-serving.md#L290-L296), [service-operations.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/service-operations.md#L586-L599)).

[^ztr-stale]: The `grpc.health.v1` service on the gRPC listener reports `NOT_SERVING` when the tip header is older than `--max-tip-age`, 300 seconds by default, or when no head poll has succeeded for 30 seconds ([health.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/health.rs#L121-L144), [config.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/config.rs#L166-L187), [cli.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/bin/ztreamerd/src/cli.rs#L123-L132), [launcher.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/launcher.rs#L185-L203), [follower.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/sync/src/head/follower.rs#L25-L32), [serving.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/sync/src/head/serving.rs#L63-L69)).

[^zingod-stale]: `/readyz` returns 503 (`down`, `tip_unvouched`) when the best block is more than 24 block intervals old and the tip is unvouched or every validator is Down. It also returns 503 for an unvouched tip while Zingod is not synced. A synced, unvouched tip that is still fresh returns 200 `degraded`. `/status` reports the tip, the served height, and stale-validator alarms ([grpc_snapshot.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc-snapshot/src/grpc_snapshot.rs#L132-L156), [alarm_thresholds.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-chainview/src/alarm_thresholds.rs#L9), [health.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/health.rs#L260-L261), [health.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/health.rs#L452), [report.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc-snapshot/src/report.rs#L63), [transport.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc/src/transport.rs#L137), [monitoring.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/operators/monitoring.md#L11-L12)).

[^lwd-stale]: The only HTTP endpoint is `/metrics`, with no health or readiness route. Its gauge records the time of the last successful `getbestblockhash`, which shows whether the validator answers, not how old the served tip is ([root.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/cmd/root.go#L553-L556), [common.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/common/common.go#L489-L492), [common.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/common/common.go#L529), [service.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/frontend/service.go#L548-L553)).

[^zinder-stale]: zinder-query and zinder-compat-lightwalletd stay ready and report `served_epoch_age_seconds` on `/readyz`, the time since zinder-ingest committed the chain epoch they serve, which grows while the chain stops advancing and is also exported as `zinder_readiness_served_epoch_age_seconds`. zinder-ingest's own `/readyz` goes unready with `node_unavailable` ([service-operations.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/service-operations.md#L84-L87), [readiness.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/crates/zinder-runtime/src/readiness.rs#L496-L503), [deploying-wallet-serving.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/runbooks/deploying-wallet-serving.md#L290-L296)).

[^lwd-retries]: At runtime the ingestor retries without limit, but startup exits after about 10 failed retries ([common.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/common/common.go#L255-L277)).

[^ztr-remote-start]: At startup, remote mode polls `getblockchaininfo` every second until the node answers, and opens its gRPC listener only after it catches up ([remote/mod.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/remote/mod.rs#L145-L199), [launcher.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/launcher.rs#L68-L125), [launcher.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/launcher.rs#L171-L206)).

[^zingod-start-down]: Needs `[p2p] enabled = true`, which is off by default, and the `tree_state` index, so that a restart anchors on the indexes' own chain through peers. By default, Zingod starts and answers health checks but serves no indexed data ([header_sync.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-chainview/src/header_sync.rs#L328-L336), [nfs_core.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-nfs/src/nfs_core.rs#L217), [daemon.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/daemon.rs#L75), [daemon.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/daemon.rs#L312), [config.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/config.rs#L485), [architecture.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/contributors/architecture.md#L110)).

[^lwd-start-down]: Startup blocks in `FirstRPC`, which retries `getblockchaininfo` 10 times over about six minutes and then exits. The gRPC listener opens only after that ([common.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/common/common.go#L255-L276), [root.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/cmd/root.go#L242-L249), [root.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/cmd/root.go#L380-L402)).

[^zinder-reader-start]: zinder-query and zinder-compat-lightwalletd start without contacting a validator, and a reader restarted during a validator outage serves again while zinder-ingest keeps running. zinder-ingest exits at startup when no validator answers, and the readers exit at startup when they cannot reach it, so a start of the whole deployment, such as the bundled single-volume container, serves nothing until a validator answers ([service-operations.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/service-operations.md#L622-L633), [zinder-query main.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-query/src/bin/zinder-query/main.rs#L151-L167), [zinder-ingest main.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-ingest/src/main.rs#L1162-L1181), [deploying-wallet-serving.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/runbooks/deploying-wallet-serving.md#L495-L499)).

[^ztr-built-before]: Only from an index built before the validator pruned those heights, which ztreamer serves from its local LMDB store ([blocks/mod.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/service/src/compact/blocks/mod.rs#L143-L165)).

[^zingod-built-before]: Only from `compact_block` and `tree_state` indexes that were built, or restored from a snapshot, before the validator pruned those heights ([architecture.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/contributors/architecture.md#L18-L19)).

[^lwd-built-before]: From its on-disk block cache, which it reloads at startup unless `--redownload` is set, while a height missing from the cache needs a block body that the pruned node may no longer have ([cache.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/common/cache.go#L155-L192), [root.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/cmd/root.go#L339-L345), [common.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/common/common.go#L590-L612)).

[^zinder-pruned-later]: zinder-ingest checks storage mode only when it starts or evaluates a standby validator, not on the validator it follows, so it refuses the pruned validator at its next start. zinder-query and zinder-compat-lightwalletd then serve their last view, tree states included, only until `storage.serving_pair_staleness_ceiling_ms` passes, five minutes by default ([node-source-boundary.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/node-source-boundary.md#L199-L205), [monitor.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-ingest/src/node_failover/monitor.rs#L172-L178), [service-operations.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/service-operations.md#L563-L569)).

[^zakura-tx-retention]: A pruned Zakura deletes raw transactions outside its `tx_retention` window, which is at least 10,000 blocks ([config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-state/src/config.rs#L482-L505)).

[^raw-forwarded]: The indexer fetches raw transactions from the validator, which deletes them outside its `tx_retention` window ([ztreamer state_backend.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/remote/src/backend/state_backend.rs#L80-L92), [Zingod transactions.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc/src/routes/transactions.rs#L64-L88), [Go lightwalletd service.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/frontend/service.go#L507-L528)).

[^zakura-checkpoint]: Only with `consensus.checkpoint_sync = false`, because under the default `true` a pruned node stops its address indexes for good ([config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-state/src/config.rs#L224-L228), [consensus config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-consensus/src/config.rs#L129)).

[^ztr-taddr-pruned]: ztreamer forwards address history to Zakura, whose address index stops under the default checkpoint sync and which deletes transaction bytes outside its retention window ([address.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/service/src/compact/address.rs#L16), [Zakura config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-state/src/config.rs#L482-L505)).

[^zingod-taddr-pruned]: Zingod forwards transaction bytes to the validator, and a `NotFound` answer ends the stream with `NOT_FOUND` ([transactions.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc/src/routes/transactions.rs#L74-L80)).

[^lwd-taddr-pruned]: Go lightwalletd fetches each transaction with `getrawtransaction`, which fails for transactions that the pruned node deleted ([service.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/frontend/service.go#L217-L225)).

[^zakura-vct]: On Mainnet, only with `consensus.vct_fast_sync = false`, because the default fast sync leaves those tree states unavailable ([service.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-state/src/service.rs#L2719-L2738), [consensus config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-consensus/src/config.rs#L41-L51)).

[^ztr-tree-index]: From its local tree index by default, and with `--no-tree-index` it forwards to the validator ([tree.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/service/src/compact/tree.rs#L18-L35), [cli.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/bin/ztreamerd/src/cli.rs#L150-L154)).

[^lwd-tree-pruned]: Go lightwalletd forwards to the validator's `z_gettreestate`, which a pruned Zakura on Mainnet serves only with `consensus.vct_fast_sync = false` ([service.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/frontend/service.go#L450-L454), [Zakura service.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-state/src/service.rs#L2719-L2738)).

[^zebra-tls]: The lightwalletd gRPC listener is plaintext HTTP/2, so TLS needs a reverse proxy ([rpc.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/config/rpc.rs#L64-L71)).

[^zakura-tls]: Zakura has no lightwalletd gRPC listener, while its JSON-RPC offers `rpc.tls` and its Indexer gRPC offers mutual TLS through `rpc.indexer_tls` ([rpc.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/config/rpc.rs#L96-L102)).

[^ztr-tls]: Set `--tls-cert` and `--tls-key`, because the listener is plaintext by default ([cli.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/bin/ztreamerd/src/cli.rs#L134-L140)).

[^zingod-tls]: Zingod refuses a non-loopback listen address unless `[serve] tls` is set, and serves plaintext only on loopback ([serve_tls.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/config/serve_tls.rs#L71-L86)).

[^lwd-tls]: Required by default, and `--no-tls-very-insecure` turns it off ([root.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/cmd/root.go#L89-L99)).

[^zinder-tls]: The listeners speak plaintext h2c, and a public bind needs `security.allow_public_bind` with a TLS proxy in front ([bind_guard.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/crates/zinder-runtime/src/bind_guard.rs#L97-L125)).

[^zebra-cookie]: Cookie authentication is on by default ([rpc.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/config/rpc.rs#L125-L126), [server.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/server.rs#L133-L139)).

[^zebra-grpc-no-auth]: Zebra's gRPC servers add no authentication layer ([Indexer server.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/indexer/server.rs#L71-L77), [lightwalletd server.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/lightwalletd/server.rs#L95-L102)).

[^zakura-cookie]: Cookie authentication is on by default ([rpc.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/config/rpc.rs#L208-L209)).

[^zakura-indexer-tls]: Needs `rpc.indexer_tls`, which is unset by default and required for a non-loopback listen address. Clients must then present a certificate signed by `client_ca_file` ([rpc.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/config/rpc.rs#L98-L102), [server.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/indexer/server.rs#L212-L224)).

[^ztr-auth]: The lightwalletd gRPC listener, which also serves Replicator, has no client authentication ([README.md](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/README.md#L81-L95)).

[^zinder-explorer-auth]: zinder-explorer requires a bearer token when `explorer.bearer_token_path` is set. The zinder-query and zinder-compat-lightwalletd listeners do not authenticate clients ([config.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-explorer/src/bin/zinder-explorer/config.rs#L27), [explorer-plane.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/explorer-plane.md#L405)).

[^forwarded]: The indexer forwards all or part of this call to its validator. [When the validator is down](#when-the-validator-is-down) shows what each indexer still serves without it ([ztreamer state_backend.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/remote/src/backend/state_backend.rs#L123-L253), [Zingod transactions.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc/src/routes/transactions.rs#L22-L88), Go lightwalletd service.go [addresses](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/frontend/service.go#L136-L231) and [other calls](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/frontend/service.go#L403-L632), [Zinder lib.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-query/src/lib.rs#L3-L6)).

[^zinder-tree-local]: Served from Zinder's own store with no validator call. A height between stored checkpoints is rebuilt from stored compact blocks, at most one rebuild per CPU at once. A rebuild that finds every slot busy waits up to 2 seconds, in a line of 64 places per slot by default, then fails with `UNAVAILABLE` on the lightwalletd API and `RESOURCE_EXHAUSTED` on the native one, and fails at once when the line is full. While it follows the chain, zinder-ingest adds stored checkpoints to settled history in the background, by default one each time 1,024 note commitments accumulate, or one per block with `ingest.tree_state_densification.selection = "every-block"`, so fewer heights need a rebuild ([`GetTreeState`](lightwalletd-compatibility.md#gettreestate-and-getlatesttreestate), [Tree-state densification](../architecture/chain-ingestion.md#tree-state-densification), [tree_state_replay_limit.rs](https://github.com/ZcashFoundation/zinder/blob/ddd90e89def3376cc77ccff3b5806383585718fc/services/zinder-query/src/tree_state_replay_limit.rs#L48-L64), [tree_state_densification.rs](https://github.com/ZcashFoundation/zinder/blob/ddd90e89def3376cc77ccff3b5806383585718fc/services/zinder-ingest/src/tree_state_densification.rs#L46-L55)).

[^zingod-metrics]: Needs `[metrics] listen_address`, because Zingod's admin listener is off by default ([config.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/config.rs#L525-L531)).

[^zinder-secondaries]: zinder-query, zinder-compat-lightwalletd, and zinder-explorer each open their own RocksDB secondary over shared primaries on one host ([ADR-0035](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/adrs/0035-canonical-storage-topologies.md#L169-L174)).

[^ztr-replica]: A remote-mode instance imports sealed ranges from a peer once, then follows its own Zakura. Embedded mode does not import from a peer ([replicate/mod.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/replicate/mod.rs#L121-L160), [README.md](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/README.md#L59-L66)).

[^zinder-single-host]: Only the `rocksdb-single-host` topology exists, and `postgres-horizontal` is decided but not implemented ([ADR-0035](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/adrs/0035-canonical-storage-topologies.md#L64-L70)).

[^zebra-snap]: The Zcash Foundation publishes unsigned state snapshots that you extract into `cache_dir` by hand ([snapshots.md](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/book/src/user/snapshots.md#L52-L57)).

[^zakura-snap]: The project publishes state snapshots, about 11 GB for pruned state, which you download outside zakurad ([README.md](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/README.md#L26)).

[^ztr-snap]: Remote mode fetches a signed Mainnet snapshot by default, and Testnet needs `--replicate-from`. Embedded mode does not import a snapshot and indexes from its own Zakura ([replicate/mod.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/replicate/mod.rs#L75-L80), [README.md](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/README.md#L59-L63)).

[^zingod-snap]: Needs a `[snapshot]` manifest, from which aria2c fills empty index directories ([config.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/config.rs#L533-L545)).

[^zinder-snap]: Zinder can capture a state bundle, but no restore path is implemented ([state_bundle.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-projector/src/state_bundle.rs#L1-L9)).

[^zingod-image]: The `zingodevops/zingod` Docker image, for linux/amd64 only ([release.yaml](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/.github/workflows/release.yaml#L120-L135)).

[^lwd-image]: Docker images only, without standalone binaries ([RELEASING.md](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/RELEASING.md#L77-L86)).

[^zinder-processes]: zinder-ingest, zinder-projector, zinder-query, zinder-compat-lightwalletd, and a Zebra or Zakura validator, plus the optional zinder-explorer and any fallback validators ([ADR-0035](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/adrs/0035-canonical-storage-topologies.md#L169-L174), [release-images.json](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/deploy/release-images.json#L1-L7)).

[^zinder-node-targets]: zinder-ingest follows one validator at a time from `[node]` and any `[node_fallbacks.N]` endpoints, numbered from 1 without a gap. zinder-query and wallet-mode zinder-compat-lightwalletd read their own list only to broadcast, and the bundled container gives zinder-query the same endpoints as zinder-ingest. zinder-explorer reads only `[node]`, and zinder-projector and compact-blocks zinder-compat-lightwalletd refuse any node setting ([node-source-boundary.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/node-source-boundary.md#L105-L143), [wallet-serving-runtime-entrypoint](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/deploy/wallet-serving-runtime-entrypoint#L78-L80)).

[^zebra-disk]: The Zebra book's figure, and extracting a snapshot needs twice that for a while ([requirements.md](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/book/src/user/requirements.md#L23-L26)).

[^zakura-disk]: The zcashd-compat preflight checks for 275 GiB, and the README lists a pruned snapshot of about 11 GB ([preflight.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakurad/src/components/zcashd_compat/preflight.rs#L47), [README.md](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/README.md#L26)).

[^ztr-disk]: Measured at height 3,532,489 on 2026-10-27, and the size of the Zakura state is not stated ([README.md](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/README.md#L120)).

[^zingod-disk]: The live sync test provisions 160 GiB for indexes and marks that figure UNMEASURED ([zingod_index_construction.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/live-tests/sync/tests/zingod_index_construction.rs#L36-L37)).

[^zinder-disk]: Volume used by the Zinder Mainnet deployment on v0.16.3, and the runbook asks for about 130 GB free when wallet construction starts ([ADR-0035](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/adrs/0035-canonical-storage-topologies.md#L33-L35), [initial-sync.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/runbooks/initial-sync.md#L141-L144)).

[^zebra-sync]: A Zcash Foundation snapshot starts the node near the tip ([snapshots.md](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/book/src/user/snapshots.md#L3-L5)).

[^zakura-sync]: Checkpoint sync and verified-commitment-tree fast sync are on by default ([config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-consensus/src/config.rs#L129-L130)).

[^ztr-sync]: Remote mode imports sealed ranges from the published Mainnet snapshot or the `--replicate-from` source, then indexes the blocks above them from Zakura, and the import time is not stated. Embedded mode imports nothing: it waits until its Zakura is near the tip, and indexing then took 92 seconds in the project's warm-cache benchmark ([README.md](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/README.md#L63), [embedded.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/embedded.rs#L41-L65), [README.md](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/README.md#L134-L144)).

[^lwd-sync]: The README's estimate, and the server answers requests while the cache fills ([README.md](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/README.md#L131-L133)).

[^zinder-sync]: Canonical and wallet construction from an archive node, combined from builds on earlier releases, with no rebuild timed on the current release ([ADR-0035](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/adrs/0035-canonical-storage-topologies.md#L30-L32)).

[^zebra-limits]: 20 HTTP/2 streams per gRPC connection, 50 MiB JSON-RPC responses, 10,000 addresses, and 20,000 mempool exclude entries ([server.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/lightwalletd/server.rs#L24), [rpc.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/config/rpc.rs#L128-L129), [methods.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/methods.rs#L4186)).

[^zakura-limits]: JSON-RPC requests up to twice the maximum block size, 50 MiB responses, and batches of 1,000, plus Indexer gRPC limits of 64 connections, 64 streams, and 4 KiB requests ([server.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/server.rs#L127-L142), [indexer server.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/indexer/server.rs#L37-L49)).

[^ztr-limits]: 100 HTTP/2 streams per connection, 16 block-range streams with a 10-second wait, 10,000 balance-stream addresses, and 2,000,000-byte transactions ([config.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/daemon/src/config.rs#L150), [range_limit.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/service/src/compact/range_limit.rs#L10-L15)).

[^zingod-limits]: 4,096 connections, 128 per IP address, 100 streams per connection, 2,048 work streams, adaptive admission, and 100,000 address rows ([limits.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc/src/limits.rs#L61-L91), [methods.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/wallets/methods.md#L83-L84)).

[^lwd-limits]: 100 streams per connection, 200 concurrent block ranges with a 10-second queue, 10,000 addresses, a 10-million-block address span, 2 MB transactions, and a 60-second stalled-stream timeout ([root.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/cmd/root.go#L427-L428), [service.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/frontend/service.go#L42-L79), [sendtimeout.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/cmd/sendtimeout.go#L16)).

[^zinder-limits]: 100 concurrent block-range streams by default, with up to as many more requests waiting 10 seconds for a slot, 200 HTTP/2 streams per connection, 100,000 UTXOs per response, 256 distinct addresses per balance request, 64 bytes per address, 100,000 transactions per `GetMempoolStream` between tip changes, and, in wallet serving, one tree-state rebuild per CPU at once, with 64 waiting places per rebuild slot for up to 2 seconds, while rate limits and other per-client limits are left to the edge ([`GetBlockRange`](lightwalletd-compatibility.md#getblockrange-and-getblockrangenullifiers), [block_range.rs](https://github.com/ZcashFoundation/zinder/blob/ddd90e89def3376cc77ccff3b5806383585718fc/services/zinder-compat-lightwalletd/src/block_range.rs#L63), [tree_state_replay_limit.rs](https://github.com/ZcashFoundation/zinder/blob/ddd90e89def3376cc77ccff3b5806383585718fc/services/zinder-query/src/tree_state_replay_limit.rs#L48-L64), [lightwalletd-compatibility.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/reference/lightwalletd-compatibility.md#L430-L504), [lightwalletd-compatibility.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/reference/lightwalletd-compatibility.md#L581-L586), [lib.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-query/src/lib.rs#L366-L372), [transparent_address.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/crates/zinder-source/src/transparent_address.rs#L14-L19), [grpc.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-compat-lightwalletd/src/grpc.rs#L71-L77), [tree_state_replay_limit.rs](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/services/zinder-query/src/tree_state_replay_limit.rs#L29-L38)).

[^zebra-reorg]: Side-chain transactions report height `u64::MAX`, and compact reads return `ABORTED` when their tree was reorged away ([transaction lookup](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/lightwalletd/methods.rs#L208-L214), [compact reads](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/lightwalletd/methods.rs#L686-L694)).

[^zakura-reorg]: `ChainTipChange` sends the new tip, and `NonFinalizedStateChange` streams fork blocks ([methods.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/indexer/methods.rs#L65-L110)).

[^ztr-reorg]: ztreamer reconciles reorgs internally, and the error asks the client to restart from a fresh tip ([compact/mod.rs](https://github.com/zcashlabs/ztreamer/blob/39b2d1248cba0e2df3ecf1af3f419553a5dbadd4/crates/service/src/compact/mod.rs#L301-L318)).

[^zingod-reorg]: `GetMempoolStream` also ends ([served tip](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/contributors/architecture.md#L146-L180), [mempool stream](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/contributors/architecture.md#L300-L308)).

[^lwd-reorg]: The cache is truncated internally, and `GetBlockRange` can return `ABORTED` during the repair ([common.go](https://github.com/zcash/lightwalletd/blob/faaf7aa1f917f41ad41def61d7095330078c8a8d/common/common.go#L717-L727)).

[^zinder-reorg]: The event arrives through `WalletQuery.ChainEvents`, and a reorg deeper than the 1,000-block window parks the writer, after which query and compat refuse new requests with `REORG_WINDOW_EXCEEDED` ([chain-events.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/architecture/chain-events.md#L42-L65), [lightwalletd-compatibility.md](https://github.com/ZcashFoundation/zinder/blob/c293095dfab045e731cbe3e6e7f8b5dce4576fdb/docs/reference/lightwalletd-compatibility.md#L316-L329)).
