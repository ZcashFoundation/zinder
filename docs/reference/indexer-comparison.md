# Zcash Indexer Comparison

This page compares Zebra and Zakura, two validators that also serve chain data, with four indexers that serve it from a validator: ztreamer, Zingod, Go lightwalletd, and Zinder. Use the tables to check capabilities, deployment constraints, consumer fit, and measured Mainnet workload behavior.

Matrix cells use these values:

- **Yes**: the reviewed source provides the capability in a default build. A listener may still need a listen address in its configuration, and failover needs more than one validator endpoint.
- **Optional**: the capability needs a non-default build feature, mode, index, flag, setting, or runtime.
- **Experimental**: the project provides the capability and labels it experimental.
- **No**: the reviewed source does not provide the capability.

Cells marked **n/a** do not apply to that project; **Unknown** means the source review could not settle the answer. Footnotes state exact conditions and link to source evidence.

On this page:

- [At a glance: choosing an option](#at-a-glance-choosing-an-option)
- [What each project is](#what-each-project-is)
- [Validator requirements](#validator-requirements)
- [Interfaces](#interfaces)
- [Chain data](#chain-data)
- [Lightwalletd methods](#lightwalletd-methods)
- [When the validator is down](#when-the-validator-is-down)
- [On a pruned validator](#on-a-pruned-validator)
- [Operations](#operations) and [Deployment facts](#deployment-facts)
- [Consumer fit](#consumer-fit)
- [Measured Mainnet workload behavior (Zebra 7.1.0, height 3,593,347)](#measured-mainnet-workload-behavior-zebra-710-height-3593347)
- [Strengths and limits](#strengths-and-limits)
- [Source revisions](#source-revisions)

## At a glance: choosing an option

| Option | Role | Pick it when | Key constraint |
| --- | --- | --- | --- |
| **Zebra** (`zebrad`) | Validator with built-in serving | You want one process with node JSON-RPC and Indexer gRPC and can tolerate experimental lightwalletd gRPC. | Lightwalletd gRPC omits transparent inputs/outputs in compact blocks and has no TLS or client auth; serving stops with the node. |
| **Zakura** (`zakurad`) | Validator with built-in serving | You want a validator with pruned storage, mTLS Indexer gRPC, and published state snapshots. | Serves no lightwalletd gRPC or compact blocks; wallet clients need a separate server. |
| **Go lightwalletd** | Single-binary lightwalletd cache | You need a simple `CompactTxStreamer` cache with built-in TLS on Zebra, Zakura, or deprecated zcashd. | Follows one node with no failover; node-bound restores (`1.83 wallets/s` in Zebra benchmark) and fails at high concurrency. |
| **Zingod** | Failover lightwalletd indexer | You need failover across up to 64 validators, built-in TLS, and cached shielded sync behind a pruned node. | Lacks `GetBlockNullifiers` and `Ping`; fails at `128–256` clients per IP; no full blocks or shared cross-process index. |
| **ztreamer** | Zakura-coupled indexer | You run Zakura and want embedded single-process serving, maximum range throughput, or index replication. | Embedded mode and tests target Zakura only (Zebra/zcashd unverified, no failover, rejects Regtest in remote mode, no published v0.2.0 binaries/images). |
| **Zinder** (`v0.26.0`) | Multi-protocol indexer (alpha) | You want lightwalletd gRPC + native `gRPC-Web`, native `WalletQuery` (pinned epochs, resumable reorg events, outpoint spend status), optional Explorer views, validator failover, or verified edge snapshots. | Single-host `rocksdb` multi-process topology today; no built-in TLS (front with a reverse proxy); non-resumable initial build before publication; alpha stability. |

## What each project is

| Fact | Zebra | Zakura | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- | --- | --- |
| Kind | Validator with built-in serving | Validator with built-in serving | Indexer on Zakura (embedded by default) | Indexer on a validator | Indexer on a validator | Indexer on a validator |
| Version reviewed | zebrad 7.1.0 | zakurad 1.8.9 | 0.2.0, bundling Zakura 1.8.9 | 1.1.0 | 0.6.2 | 0.26.0 |

ztreamer runs in embedded mode (default, running Zakura inside `ztreamerd`) or remote mode (following one separate Zakura node). The ztreamer column covers the indexer itself; in embedded mode, its bundled Zakura also serves node JSON-RPC and Indexer gRPC once configured, as listed in the Zakura column ([README.md](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/README.md#L52-L57), [Cargo.toml](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/Cargo.toml#L41)).

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

ztreamer and Go lightwalletd follow one validator. Zingod accepts 1 to 64 validators. `zinder-ingest` follows one validator at a time from `[node]` and optional `[node_fallbacks.N]` endpoints. `zinder-query` and wallet-mode `zinder-compat-lightwalletd` fail over across that list for broadcasts and fee reads, while light-wallet compatibility uses one node for [forwarded methods](lightwalletd-compatibility.md#runtime-serving-selection) and `zinder-explorer` observes only `[node]`.

## Interfaces

Each column counts only the APIs that the project itself serves.

| API | Zebra | Zakura | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- | --- | --- |
| Node JSON-RPC | Yes | Yes | No | No | No | No |
| Node Indexer gRPC | Yes | Yes | No | No | No | No |
| Lightwalletd gRPC (`CompactTxStreamer`) | Experimental[^zebra-lwd-exp] | No | Yes[^ztr-p2p] | Yes | Yes | Yes[^zinder-grpc-web] |
| Native wallet API beyond lightwalletd gRPC | No | No | No | No | No | Yes |
| Explorer API | No | No | No | No | No | Optional[^zinder-explorer][^zinder-explorer-auth] |
| Resumable reorg-aware event cursor | No[^zebra-events] | No[^zakura-events] | No | No | No | Yes[^zinder-retention] |
| Pinned reads across requests | No | No | No | No[^zingod-pinned] | No | Yes[^zinder-pinned] |
| Index replication or snapshot serving | No | No | Yes[^ztr-replicator] | No | No | No |

Zebra and Zakura start with node JSON-RPC and Indexer gRPC off until you set their listen addresses. Zinder's `zinder-compat-lightwalletd` also serves native `gRPC-Web` on the same port when `compat.browser_origins` is configured.[^zinder-grpc-web]

## Chain data

| Data | Zebra | Zakura | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- | --- | --- |
| Compact blocks | Experimental[^zebra-lwd-exp] | No | Yes | Yes | Yes | Yes |
| Full block by height or hash | Yes | Yes | No | No | No | Optional[^zinder-raw-all] |
| Full block range stream | No | Optional[^zakura-range] | No | No | No | Optional[^zinder-raw-all] |
| Tree states and subtree roots | Yes | Yes | Yes[^forwarded] | Yes | Yes[^forwarded] | Yes[^zinder-tree-local] |
| Mined raw transaction by ID | Yes | Yes | Yes[^forwarded] | Yes[^forwarded] | Yes[^forwarded] | Yes[^zinder-tx-body] |
| Transparent address history, UTXOs, and balance | Yes | Yes | Yes[^forwarded] | Yes[^forwarded] | Yes[^forwarded] | Yes[^zinder-taddr-mode] |
| Mined spender of a transparent outpoint | No | No | No | No | No | Yes[^zinder-outpoint] |
| Mempool snapshot and stream | Yes | Yes | Yes[^forwarded] | Yes | Yes[^forwarded] | Yes |
| Broadcast a signed transaction | Yes | Yes | Yes[^forwarded] | Yes[^forwarded] | Yes[^forwarded] | Yes[^forwarded] |
| Block templates and block submission | Optional[^zebra-miner] | Optional[^zakura-miner] | No | No | No | No |
| Explorer search by transaction ID, height, hash, or address | No | No | No | No | No | Optional[^zinder-search] |
| Aggregate views across blocks (fees, pools, supply) | No[^zebra-derived] | No[^zakura-derived] | No | No | No | Optional[^zinder-explorer] |

Every project covers Sapling, Orchard, and Ironwood shielded data.

## Lightwalletd methods

Zakura has no lightwalletd gRPC listener; its Docker Compose example runs a separate `lightwalletd` beside it ([docker-compose.lwd.yml](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/docker/docker-compose.lwd.yml#L20-L23)).

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
| `GetStandardFee` | No | Yes | Yes | Yes | Yes[^zinder-standard-fee] |
| `Ping` | No | Optional[^ping] | No[^zingod-unimplemented] | Optional[^ping] | Optional[^zinder-ping] |
| Transparent inputs and outputs in compact blocks | No | Yes[^transparent-pool] | Yes[^transparent-pool] | Yes[^transparent-pool] | Yes[^transparent-pool] |

Zebra labels its lightwalletd listener experimental.[^zebra-lwd-exp] ztreamer serves the same methods in both modes. In its default `wallet` and `light-wallet` modes, Zinder serves every method and answers `Ping` only with `compat.ping_very_insecure`; see [Runtime serving selection](lightwalletd-compatibility.md#runtime-serving-selection) for `compact-blocks` mode.

## When the validator is down

Zebra and Zakura serve from the validator process, so all serving stops with the node. For Zingod and Zinder, each row assumes every configured validator is down.

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

With validator failover, Zingod and Zinder keep advancing their tip and broadcasting while at least one configured validator answers.

## On a pruned validator

Zakura is the only reviewed validator with a pruned mode (`state.storage_mode = "pruned"`, default `"archive"`; [config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-state/src/config.rs#L210-L229)). Zebra has no pruned mode ([methods.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/methods.rs#L4046-L4047)). Each indexer column assumes a pruned Zakura validator.

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
| Bootstrap from a state or index snapshot | Yes[^zebra-snap] | Yes[^zakura-snap] | Yes[^ztr-snap] | Optional[^zingod-snap] | No | Yes[^zinder-snap] |
| Published release binaries or images | Yes | Yes | No | Yes[^zingod-image] | Yes[^lwd-image] | Yes |

All six enforce built-in request limits. ztreamer and Go lightwalletd serve metrics by default; Zinder serves them by default on a loopback ops address ([Readiness](readiness.md)); Zebra and Zakura serve them once you configure a metrics listen address.

### Deployment facts

| Fact | Zebra | Zakura | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- | --- | --- |
| Processes, including the validator | 1: `zebrad` | 1: `zakurad` | 2: `ztreamerd` and its Zakura[^ztr-embedded] | 2: `zingod` and its validator | 2: `lightwalletd` and its validator | 3–4 in `light-wallet` (2–3 Zinder + validator); 6 in `wallet` Compose (5 Zinder + validator), or 7 with `zinder-explorer`[^zinder-processes] |
| Validators per deployment | 1, built in | 1, built in | 1 | 1 to 64[^zingod-failover] | 1 | 1 active, plus any ordered fallbacks[^zinder-node-targets] |
| Mainnet disk | About 300 GB[^zebra-disk] | 275 GiB minimum[^zakura-disk] | 17.7 GiB index, plus Zakura state[^ztr-disk] | Not stated[^zingod-disk] | Not stated | 71.8 GB (`light-wallet`) or 122.6 GB (`wallet`); +251.1 GiB with `raw_blob_policy = "transactions"` or larger with `wallet-archive` (`"all"`)[^zinder-disk] |
| First sync | Days from genesis, or a snapshot[^zebra-sync] | Checkpoint fast sync or snapshot; time not stated[^zakura-sync] | 11.6 min importing signed snapshot (remote); 2.8 min indexing (5.0 min with trees) in embedded mode on synced Zakura state[^ztr-sync] | From genesis or snapshot; time not stated[^zingod-snap] | From height 0, about an hour[^lwd-sync] | 49–60 min (`light-wallet`) or 76 min (`wallet`) on 8 writer cores[^zinder-sync] |
| Built-in request limits | Stream, size, and count caps; no rate limit[^zebra-limits] | Size, batch, connection, and stream caps[^zakura-limits] | Stream, range, address, and size caps[^ztr-limits] | Connection, stream, and admission caps; no rate limit[^zingod-limits] | Stream, range, address, request size, and admission caps[^lwd-limits] | Stream, response, address, and rebuild caps[^zinder-limits] |
| What a client sees on a reorg | No reorg event; some reads return `ABORTED`[^zebra-reorg] | New tip and fork blocks; no revert event[^zakura-reorg] | Affected streams fail with `UNAVAILABLE`[^ztr-reorg] | Tip drops to the fork point; no event[^zingod-reorg] | No reorg event; `GetBlockRange` may return `ABORTED`[^lwd-reorg] | `ChainReorged` event; `GetBlockRange` ends with `ABORTED`[^zinder-reorg] |

## Consumer fit

Does one deployment serve each consumer without the application building its own index? Each row takes the weakest value (`No < Experimental < Optional < Yes`) of its underlying requirements in the tables above.

| Consumer | Zebra | Zakura | ztreamer | Zingod | Go lightwalletd | Zinder |
| --- | --- | --- | --- | --- | --- | --- |
| Light wallet over lightwalletd gRPC (or browser gRPC-Web) | Experimental[^zebra-lwd-exp] | No | Yes | Yes | Yes | Yes[^zinder-grpc-web] |
| Wallet backend needing pinned reads across requests | No | No | No | No | No | Yes[^zinder-pinned] |
| Explorer needing block, transaction, and address lookups | Yes | Yes | No | No | No | Optional[^zinder-raw-all] |
| Explorer needing search and aggregate views across blocks (fees, pools, supply) | No | No | No | No | No | Optional[^zinder-explorer] |
| Deposit or payment tracking needing a resumable reorg-aware event cursor | No | No | No | No | No | Yes[^zinder-retention] |
| Miner or mining faucet needing block templates | Optional[^zebra-miner] | Optional[^zakura-miner] | No | No | No | No |
| Light wallet, explorer with aggregate views, and deposit tracking from one deployment | No | No | No | No | No | Optional[^zinder-explorer] |

Across all six projects, wallet keys, shielded scanning, signing, and invoice logic remain in the application.

## Measured Mainnet workload behavior (Zebra 7.1.0, height 3,593,347)

These observations come from public Zinder benchmarks against a frozen archive `zebrad 7.1.0` node at Mainnet height `3,593,347`: a five-reader comparison (`v0.25.0`, 2 reader cores on `c4-standard-96`, uncapped memory unless noted, which also tested `zainod` rc1 alongside Zinder, `ztreamer`, `Zingod`, and Go `lightwalletd`) and `v0.26.0` (`f40fa20`) validation runs (8 writer cores on `c4-standard-32`, `MemoryMax=64G`, `ext4 -O verity`, plus reader spot checks).

| Reader | Classic TLS 16-wallet restore | High-concurrency compact ranges (`100` & `10k` blocks, up to 256 clients) | Peak reader `VmHWM` (`v0.26.0` spot check) | Observed limits |
| --- | --- | --- | --- | --- |
| **Zinder `light-wallet`** | `24.06 wallets/s` | `483k–692k blocks/s` (`0` errors when cgroup covers ~21 GB catalog) | `370.2 MB` | Capping cgroup at `4G` drops `10k`-block throughput to `~90k blocks/s` with `wait_timeout` refusals at `256` clients; transparent history/UTXOs forward to the node. |
| **Zinder `wallet`** | `24.06 wallets/s` | Same signed `ZSHRNG01` catalog scaling | `402.4 MB` | Same cgroup v2 page-cache requirement; local wallet store keeps quiet `GetTaddressTxids` at `221.3 µs` warm p50. |
| **ztreamer** (`0.2.0`) | `24.05 wallets/s` | `594k–745k blocks/s` (`0` errors) | `1,020.9 MB` | Couples embedded mode and tests to Zakura only; rejects Regtest in remote `--remote-rpc` mode; no validator failover; omits transparent txs in unary `GetBlock`; returns `completingBlockHash` in internal byte order; no published v0.2.0 binaries/images. |
| **Zingod** (`1.1.0`) | `24.03 wallets/s` | Fails at `128–256` clients | `1,376.8 MB` | Hits `max_connections_per_ip=128` / `work_share_per_ip=1/16`; `GetBlockNullifiers` and `Ping` return `UNIMPLEMENTED`; failed `GetMempoolTx`, `GetTransaction`, and quiet address history on the zero-peer frozen node. |
| **zainod** (rc1) | `22.34 wallets/s` | `154k–170k blocks/s` at `256` clients | `1,822.4 MB` | ~`41 ms` unary trailer latency (`TCP_NODELAY` unset, delayed ACK); `35.24 GiB` own index reading Zebra state directly (`0.10.2` did not decode NU7). |
| **Go lightwalletd** (`v0.6.2`) | `1.83 wallets/s` (`8.48 s` p50) | Fails at `256` clients on `10k` blocks (`~56` `UNAVAILABLE`) | Not in `v0.26.0` spot set | Node-RPC bound; fails at `128` direct clients on Zebra's `100`-connection RPC limit (mapping HTTP 429s to `INVALID_ARGUMENT` on `GetTreeState`). |

Key takeaways from the Zebra 7.1.0 runs:

- **Rust indexers eliminate the restore bottleneck.** In the 16-wallet TLS restore test (2,000 blocks + 20 tree states), Zinder (`light-wallet` and `wallet`), `ztreamer`, and `Zingod` all cluster near `24.1 wallets/s`, over `13x` faster than Go `lightwalletd` (`1.83 wallets/s`), which blocks on per-request Zebra RPCs.
- **Zinder's `v0.26.0` builds and spot latencies.** On 8 writer cores, `light-wallet` builds in `60.4 min` (`71.76 GB` disk; `48.6 min` / `70.97 GB` with `commitment-budget = 1024`; `zebra-indexer-grpc` cuts indexer CPU by `5.5%` and Zebra CPU by `9.1%`), and `wallet` builds in `75.8 min` (`122.64 GB` final / `131.80 GB` peak). In `v0.26.0` spot checks, `GetTreeState` `c1` warm p50 measured `98.9–102.6 µs` and `cbb-tree 100` reached `65.7k–70.6k req/s`.
- **Zinder's operational tradeoffs.** Zinder runs on a single-host `rocksdb` multi-process topology today (`postgres` horizontal serving in [ADR-0004](../adrs/0004-storage-and-deployment-topologies.md) is decided, not implemented). Public high-concurrency `zinder-compat-lightwalletd` endpoints must size `ZINDER_COMPAT_MEMORY_LIMIT` above the ~`21 GB` `ZSHRNG01` catalog plus working set (`32g+` on `64+ GiB` hosts) to prevent cgroup v2 page-cache thrashing. Initial construction is non-resumable if stopped before publication, listeners speak plaintext h2c behind your own TLS proxy, and as an alpha project, storage layout bumps require a fresh rebuild or restoring a same-schema snapshot.

## Strengths and limits

### Zebra

Strengths:

- Runs the validator and every listener in one process, with no separate indexer.
- Node JSON-RPC serves full blocks, tree states, and transparent address data.
- Built-in health, readiness, and Prometheus metrics, with published release binaries, images, and state snapshots.

Limits:

- Lightwalletd gRPC is experimental, and its compact blocks omit transparent inputs and outputs.
- No built-in TLS on lightwalletd gRPC, and no client authentication on gRPC listeners.
- All serving stops whenever the validator stops, with no pinned reads, resumable reorg-aware event cursor, or cross-host replicas.

### Zakura

Strengths:

- Node JSON-RPC serves full blocks, tree states, subtree roots, and transparent address data.
- Node Indexer gRPC streams tip, block, and mempool changes and supports mTLS.
- Supports a pruned storage mode, published state snapshots, health/readiness, and Prometheus metrics.

Limits:

- No lightwalletd gRPC or compact blocks; wallet serving needs a separate server.
- Full block range streaming requires the non-default `indexer` build feature.
- All serving stops with the validator; pruned mode deletes historical raw transactions and needs non-default sync settings for complete address history and Mainnet historical tree states.

### ztreamer

Strengths:

- Serves `CompactTxStreamer` (including transparent inputs/outputs in compact block ranges), with `Ping` behind a testing flag.
- Embedded mode runs the indexer and Zakura in one process; remote mode keeps serving indexed compact blocks and tree states while Zakura is down and recovers without a restart.
- Supports index replication and signed snapshots for remote bootstrap, and scales to 256 concurrent compact-range clients cleanly.

Limits:

- Embedded mode runs Zakura only, remote mode is tested only against Zakura (Zebra and zcashd unverified; rejects Regtest in `--remote-rpc` mode), and there is no validator failover.
- Forwards transparent address queries and broadcasts to Zakura (failing when it is down); omits transparent txs in unary `GetBlock` and returns `completingBlockHash` in internal byte order.
- No full blocks, explorer API, client authentication, or published v0.2.0 binaries or images.

### Zingod

Strengths:

- Fails over across up to 64 validators in priority tiers.
- Keeps serving indexed compact blocks, tree states, and transparent address queries while every validator is down.
- Built-in TLS on lightwalletd gRPC (required on non-loopback addresses), and serves shielded sync from existing indexes behind a pruned validator.

Limits:

- No full blocks (`GetBlock` and `GetBlockRange` return compact blocks only), and `GetBlockNullifiers` and `Ping` return `UNIMPLEMENTED`.
- Fails under `128–256` concurrent clients per IP in Mainnet load tests.
- No client authentication, no cross-process shared index, and a pruned validator cannot supply complete transparent address history.

### Go lightwalletd

Strengths:

- Runs on Zebra, Zakura, and deprecated zcashd.
- Built-in TLS on lightwalletd gRPC (required by default) and default Prometheus metrics, with published Docker images.
- Serves cached compact blocks while the validator is down and recovers without a restart.

Limits:

- Tip calls, tree states, transparent address queries, and broadcasts fail while the validator is down; node-bound restores are slow (`1.83 wallets/s`) and drop at high concurrency.
- No full blocks, explorer API, block lookup by hash, health/readiness endpoint, or client authentication.
- Follows one validator with no failover, and a fresh cache cannot build from a pruned validator.

### Zinder

Strengths:

- Serves every vendored lightwalletd method (plus native `gRPC-Web` when `compat.browser_origins` is set), alongside native `WalletQuery` (epoch-pinned reads, resumable reorg-aware `ChainEvents`, and `TransparentOutpointSpendStatus`) and optional `zinder-explorer` search and aggregate views.
- Scales to `256` concurrent range clients at `483k–692k blocks/s` and restores at `24.1 wallets/s` across multiple serving processes sharing one host's stores.
- Fails over across ordered validators (`[node]` + `[node_fallbacks.N]`), stays ready during node outages while reporting `served_epoch_age_seconds`, and supports verified edge-snapshot bootstrap ([ADR-0024](../adrs/0024-verified-edge-snapshot-bootstrap.md)) and coherent private backups ([private-backups.md](../runbooks/private-backups.md)).

Limits:

- Alpha stability: APIs, storage layouts, and topology will evolve, and layout bumps require a fresh rebuild or same-schema snapshot restore.
- Single-host `rocksdb` multi-process topology today (`postgres` horizontal serving is decided in [ADR-0004](../adrs/0004-storage-and-deployment-topologies.md), not implemented); high-concurrency public range endpoints need `ZINDER_COMPAT_MEMORY_LIMIT` sized above the ~21 GB `ZSHRNG01` catalog.
- Initial builds restart from scratch if interrupted before publication; `zinder-ingest` requires an archive validator and readers stop 5 minutes after ingest stops.
- No built-in TLS or wallet-listener client auth (front with a reverse proxy); full blocks require `storage.raw_blob_policy = "all"` and Explorer views require `zinder-explorer` and `ingest.explorer_views = true`.

## Source revisions

The tables reflect Zinder `0.26.0` (`f40fa20`, reviewed 2026-11-26) alongside Go lightwalletd (reviewed 2026-11-20) and the other four projects (reviewed 2026-11-17). Published builds and enabled features can differ; exercise your client's sync, restore, and send flows before relying on a compatibility claim. [Zinder compatibility evidence](lightwalletd-compatibility.md#claim-boundary) defines Zinder's own claim levels.

- [Zinder 0.26.0 at `f40fa20`](https://github.com/ZcashFoundation/zinder/commit/f40fa20)
- [Zebra `main` at `86f3ac7`](https://github.com/ZcashFoundation/zebra/commit/86f3ac7e4dcf7644a88222b8678790ed68734e63)
- [Zakura `main` at `dd25aa4`](https://github.com/zakura-core/zakura/commit/dd25aa429cd97eb1af903b7bc6375fe208132015)
- [ztreamer `master` at `4982581`, after v0.2.0](https://github.com/zcashlabs/ztreamer/commit/498258159d868d5592ff44c75e9b311353d51e35)
- [Zingod `main` at `3664ce0`](https://github.com/zingolabs/zingod/commit/3664ce0bbea3f1945c84c8730359795b84959ed2)
- [Go lightwalletd v0.6.2 at `f8fe2cc`, released 2026-11-18](https://github.com/zcash/lightwalletd/commit/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d)

[^ztr-zebra]: Unverified: remote mode calls 14 node RPCs present in Zebra 7.1.0, but ztreamer does not check node identity and no test runs against Zebra. Embedded mode runs only bundled Zakura ([methods.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/remote/src/methods.rs#L66-L85), [config.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/config.rs#L39-L51)).

[^ztr-zcashd]: Unverified: ztreamer does not document or test zcashd and does not check zcashd response shapes. Embedded mode runs only bundled Zakura ([README.md](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/README.md#L10), [methods.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/remote/src/methods.rs#L1-L5), [config.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/config.rs#L39-L51)).

[^zinder-certified]: Zinder's live and parity lanes certify Zebra 7.1.0 and Zakura 1.8.6 ([lightwalletd-compatibility.md](lightwalletd-compatibility.md#reference-inputs)).

[^zingod-zakura]: Zingod's documentation and configuration name Zakura, while its live tests run only against Zebra ([README.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/README.md#L3-L5)).

[^zingod-zcashd]: The default `legacy_rpc` source claims zcashd support, while the README names only Zebra and Zakura and no live test runs zcashd ([configuration.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/operators/configuration.md#L43), [legacy_rpc.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-source/src/legacy_rpc.rs#L1)).

[^lwd-zcashd]: zcashd support is deprecated and requires zcashd's experimental `lightwalletd` or `insightexplorer` feature ([root.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/cmd/root.go#L312-L334)).

[^zingod-failover]: Failed requests rotate across up to 64 `[[trusted_validators]]` in priority tiers. A member goes Down after 10 consecutive failures, is probed every 30 s, and restores when it answers ([balancer.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/src/balancer.rs#L204-L264), [core.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/src/core.rs#L8), [member.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/src/member.rs#L113), [usage.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/usage.md#L91-L121), [configuration.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/operators/configuration.md#L35)).

[^zinder-failover]: With `[node_fallbacks.N]` after `[node]`, `zinder-ingest` switches after three consecutive transport recoveries or 5 minutes of active-node unready status while following (not during first sync), with no timed failback; endpoints with differing activation tables are excluded. `zinder-explorer` reads `[node]` only ([chain-ingestion.md](../architecture/chain-ingestion.md#node-endpoint-failover), [ADR-0007](../adrs/0007-node-failure-recovery-and-transport-policy.md), [node-source-boundary.md](../architecture/node-source-boundary.md#ordered-node-list)).

[^zingod-broadcast-failover]: With `[p2p]` off (default), a failed push retries up to 4 untried validators at random (`max_attempts`); with `[p2p]` on, Zingod allows one fallback push ([policy.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-chainview/src/submit/policy.rs#L183-L206), [driver.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-chainview/src/submit/driver.rs#L108), [config.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/config.rs#L485), [architecture.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/contributors/architecture.md#L249)).

[^zinder-broadcast-failover]: `zinder-query` and wallet-mode `zinder-compat-lightwalletd` fail over to the next verified endpoint when one is unreachable (including `broadcast_timeout_secs`) or rejects the consensus branch, keeping the responding endpoint preferred without timed failback ([node-source-boundary.md](../architecture/node-source-boundary.md#capabilities), [lightwalletd-compatibility.md](lightwalletd-compatibility.md#sendtransaction)).

[^ztr-embedded]: Embedded mode runs Zakura inside `ztreamerd` as one process with no separate validator to switch to; if any task stops, the daemon exits ([embedded.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/embedded.rs#L78-L100), [config.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/config.rs#L39-L51), [submission.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/embedded/src/submission.rs#L10-L16), [lifecycle/mod.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/lifecycle/mod.rs#L109-L132)).

[^ztr-pruned]: ztreamer refuses to start unless its existing index already reaches Zakura's prune height ([pipeline/mod.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/sync/src/pipeline/mod.rs#L293-L295), [replicate/mod.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/replicate/mod.rs#L138-L149)).

[^zingod-pruned]: Zingod does not detect pruning, so a fresh index stalls where the validator lacks blocks ([balancer.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-traffic/src/balancer.rs#L230-L264)).

[^lwd-pruned]: Go lightwalletd does not check for pruning and stalls where blocks are missing ([root.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/cmd/root.go#L372-L376), [common.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/common/common.go#L563-L569)).

[^zinder-archive]: `zinder-ingest` refuses a lone `[node]` that reports pruned storage or omits its storage mode at startup, and excludes pruned `[node_fallbacks.N]` endpoints until restart ([admission.rs](../../services/zinder-ingest/src/node_failover/admission.rs#L18-L26), [admission.rs](../../services/zinder-ingest/src/node_failover/admission.rs#L148-L165), [chain-ingestion.md](../architecture/chain-ingestion.md#what-ingest-checks-before-it-writes)).

[^zebra-lwd-exp]: Zebra marks its lightwalletd gRPC listener experimental and warns it may break clients in any release ([rpc.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/config/rpc.rs#L58-L62)).

[^ztr-p2p]: Embedded mode also serves `CompactTxStreamer` to Zakura's v2 P2P peers ([README.md](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/README.md#L54-L56), [embedded.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/embedded.rs#L81-L84), [lib.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/p2p/src/lib.rs#L1)).

[^zinder-grpc-web]: `zinder-compat-lightwalletd` serves native `gRPC-Web` (binary and text) on its existing listener when `compat.browser_origins` is configured ([integration-surfaces.md](integration-surfaces.md#browser-wallets), [configuration.md](configuration.md#environment-variables)).

[^zebra-events]: `NonFinalizedStateChange` resumes from known tips for non-finalized blocks only and sends no undo events ([indexer.proto](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/proto/indexer.proto#L73-L78)).

[^zakura-events]: `NonFinalizedStateChange` resumes from known tips, sends no revert events, and keeps no durable cursor ([indexer.proto](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/proto/indexer.proto#L74-L87)).

[^zinder-retention]: Through `WalletQuery.ChainEvents`, retaining 168 hours of events by default ([chain-events.md](../architecture/chain-events.md#retention), [retention.rs](../../crates/zinder-runtime/src/sections/retention.rs#L21)).

[^zingod-pinned]: Zingod pins one snapshot per request or stream only ([service.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc/src/service.rs#L5-L6)).

[^zinder-pinned]: A pinned chain view stays readable for one minute after a newer epoch replaces it ([wallet_serving_pair_publisher.rs](../../services/zinder-query/src/wallet_serving_pair_publisher.rs#L92-L100)).

[^zinder-explorer]: Needs the optional `zinder-explorer` runtime and `ingest.explorer_views = true` (off by default except in `zinder-wallet-serving-explorer`; [explorer-plane.md](../architecture/explorer-plane.md#turn-it-on), [ingest.toml](../../deploy/config/ingest.toml#L23), [Dockerfile](../../deploy/Dockerfile#L399-L416)).

[^ztr-replicator]: Replicator gRPC on the lightwalletd gRPC listener plus `export-snapshot` ([launcher.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/launcher.rs#L183-L192), [replicate.proto](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/protocol/proto/ztreamer/replicate.proto#L9-L16)).

[^zakura-range]: Indexer `GetBlockRange` requires the non-default `indexer` build feature ([methods.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/indexer/methods.rs#L415-L423), [Cargo.toml](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakurad/Cargo.toml#L62-L97)).

[^zinder-raw-all]: Needs `storage.raw_blob_policy = "all"` (`wallet-archive` sets `"all"`; `light-wallet` and `wallet` set `"none"`; the shipped `deploy/config/ingest.toml` defaults to `"transactions"` without a profile) ([native_wallet_endpoint_capabilities.rs](../../services/zinder-query/src/native_wallet_endpoint_capabilities.rs#L116-L121), [ingest.toml](../../deploy/config/ingest.toml#L19), [configuration.md](configuration.md#deployment-profiles)).

[^zinder-tx-body]: Local when retaining transaction blobs (`raw_blob_policy = "transactions"` or `wallet-archive` / `"all"`); otherwise fetched from a verified node and checked against the stored SHA-256 digest ([lightwalletd-compatibility.md](lightwalletd-compatibility.md#node-forwarded-reads)).

[^zinder-taddr-mode]: Local in `wallet` and `wallet-archive` profiles (`zinder-projector`); forwarded to the node in `light-wallet` profile ([ADR-0023](../adrs/0023-deployment-profiles.md), [lightwalletd-compatibility.md](lightwalletd-compatibility.md#node-forwarded-reads)).

[^zinder-outpoint]: Through native `WalletQuery.TransparentOutpointSpendStatus` ([wallet.proto](../../crates/zinder-proto/proto/zinder/v1/wallet/wallet.proto#L1558-L1565)).

[^zebra-miner]: `getblocktemplate` needs `mining.miner_address` (unset by default); `submitblock` works without it ([get_block_template.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/methods/types/get_block_template.rs#L513-L517), [mining.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/config/mining.rs#L34-L41)).

[^zakura-miner]: `getblocktemplate` needs `mining.miner_address`, unset by default ([methods.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/methods.rs#L3560-L3563), [mining.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/config/mining.rs#L103)).

[^zebra-derived]: `getblock` returns per-block value pools, chain supply, and (at verbosity 3) transaction fees, with no cross-block aggregates ([methods.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/methods.rs#L1843-L1871)).

[^zakura-derived]: `getblock` returns per-block value pools and chain supply, with no fees or cross-block aggregates ([methods.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/methods.rs#L2660-L2688)).

[^zinder-search]: Needs optional `zinder-explorer` with `explorer.wallet_query_endpoint`; refuses shielded inputs ([explorer_search.rs](../../crates/zinder-core/src/explorer_search.rs#L50-L108), [explorer-plane.md](../architecture/explorer-plane.md#search-and-privacy-refusals)).

[^lwd-height]: By height only; a block hash returns `InvalidArgument` ([GetBlock](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/frontend/service.go#L247-L268), [GetBlockNullifiers](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/frontend/service.go#L294-L316)).

[^zingod-unimplemented]: Zingod's vendored `service.proto` omits this method, returning `UNIMPLEMENTED` ([methods.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/wallets/methods.md#L113)).

[^ping]: Only with `--ping-very-insecure`, labeled for testing only ([ztreamer cli.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/bin/ztreamerd/src/cli.rs#L142-L144), [Go lightwalletd service.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/frontend/service.go#L1251-L1258)).

[^zinder-ping]: All serving modes require `compat.ping_very_insecure = true` or `--ping-very-insecure`. See [Runtime serving selection](lightwalletd-compatibility.md#runtime-serving-selection).

[^zinder-standard-fee]: Wallet and light-wallet serving return the fee and estimator version reported by a verified node ([GetStandardFee](lightwalletd-compatibility.md#getstandardfee)).

[^transparent-pool]: In `GetBlockRange`, only when the request names the transparent pool type; Zingod's `GetBlock` always includes it and ztreamer fetches full blocks from its validator ([ztreamer](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/service/src/compact/blocks/mod.rs#L92-L137), [Zingod](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/wallets/methods.md#L25-L26), [Go lightwalletd](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/common/common.go#L645-L696), [Zinder](lightwalletd-compatibility.md#getblockrange-and-getblockrangenullifiers)).

[^lwd-cached]: Serves heights already in its cache; other heights fail ([common.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/common/common.go#L597-L619)).

[^zinder-down]: While `zinder-ingest` keeps running through a node outage: `zinder-query` and `zinder-compat-lightwalletd` stop serving 5 minutes (`storage.serving_pair_staleness_ceiling_ms`) after ingest stops, and after an ingest restart refuse traffic until ingest reaches a node. The single-volume container exits after 24 hours of node outage ([service-operations.md](../architecture/service-operations.md#recovery), [ADR-0010](../adrs/0010-reorg-window-and-displaced-blocks.md), [deploying-single-volume.md](../runbooks/deploying-single-volume.md)).

[^ztr-tip]: `GetLatestBlock` returns `UNAVAILABLE` after 30 seconds without a successful head poll ([compact/mod.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/service/src/compact/mod.rs#L168-L174), [follower.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/sync/src/head/follower.rs#L98-L104)).

[^zinder-tip-down]: `GetLatestBlock` and `GetLightdInfo` answer with the last indexed tip while `estimatedHeight` rises from it, then fail with `UNAVAILABLE` once that tip is 25 minutes old, while stored data keeps serving ([Stale tip](lightwalletd-compatibility.md#stale-tip)).

[^zingod-p2p]: Needs `[p2p] enabled = true`, off by default ([config.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/config.rs#L482-L490)).

[^zinder-broadcast-down]: Broadcasts fail with `UNAVAILABLE` (`NODE_UNAVAILABLE`) when every configured validator is unreachable; readiness does not change ([node-source-boundary.md](../architecture/node-source-boundary.md#capabilities), [lightwalletd-compatibility.md](lightwalletd-compatibility.md#sendtransaction)).

[^ztr-tree-down]: Serves local trees up to the durable tip while `GetLatestTreeState` fails ([tree.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/service/src/compact/tree.rs#L20-L35)).

[^zingod-taddr-down]: Balances and UTXOs come from local indexes; address history also needs transaction bytes via `[p2p] enabled = true` ([transactions.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc/src/routes/transactions.rs#L103-L126)).

[^zingod-ready]: With `[p2p] enabled = true`, `/readyz` stays 200 (`degraded`), returning 503 once the tip is 24 blocks stale ([grpc_snapshot.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc-snapshot/src/grpc_snapshot.rs#L132-L152)).

[^lwd-no-ready]: No readiness endpoint; the README's suggested `GetLightdInfo` check needs the node ([README.md](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/README.md#L80-L88)).

[^zinder-ready]: `zinder-query` and `zinder-compat-lightwalletd` answer 200 with the node under `degraded`, `zinder-explorer` readiness ignores the node, and `zinder-ingest` `/readyz` reports `node_unavailable` ([readiness.md](readiness.md#response-bodies), [explorer-plane.md](../architecture/explorer-plane.md#failure-isolation-and-operations)).

[^ztr-stale]: `grpc.health.v1` reports `NOT_SERVING` when the tip header exceeds `--max-tip-age` (300 s default) or head polls fail for 30 s ([health.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/health.rs#L121-L144), [config.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/config.rs#L166-L187), [follower.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/sync/src/head/follower.rs#L25-L32)).

[^zingod-stale]: `/readyz` returns 503 (`tip_unvouched`) when the best block is >24 intervals old and unvouched or all validators are Down, or while unsynced with an unvouched tip ([grpc_snapshot.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc-snapshot/src/grpc_snapshot.rs#L132-L156), [alarm_thresholds.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-chainview/src/alarm_thresholds.rs#L9), [monitoring.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/operators/monitoring.md#L11-L12)).

[^lwd-stale]: Only `/metrics` exists, recording the last successful `getbestblockhash`, not served tip age ([root.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/cmd/root.go#L596-L605), [common.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/common/common.go#L496-L499)).

[^zinder-stale]: `zinder-query` and `zinder-compat-lightwalletd` stay 200 and report `served_epoch_age_seconds` on `/readyz` and `zinder_readiness_served_epoch_age_seconds` in metrics while `zinder-ingest` reports `node_unavailable` ([readiness.md](readiness.md#response-bodies), [readiness.rs](../../crates/zinder-runtime/src/readiness.rs#L488-L514)).

[^lwd-retries]: Runtime ingestor retries indefinitely, while startup exits after ~10 failed retries ([common.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/common/common.go#L258-L280)).

[^ztr-remote-start]: Remote mode polls `getblockchaininfo` every second until the node answers and opens gRPC only after catching up ([remote/mod.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/remote/mod.rs#L145-L199), [launcher.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/launcher.rs#L68-L125)).

[^zingod-start-down]: Needs `[p2p] enabled = true` (off by default) and `tree_state` index; by default, Zingod starts and passes health checks but serves no indexed data ([header_sync.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-chainview/src/header_sync.rs#L328-L336), [nfs_core.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-nfs/src/nfs_core.rs#L217), [daemon.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/daemon.rs#L75), [architecture.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/contributors/architecture.md#L110)).

[^lwd-start-down]: `FirstRPC` retries `getblockchaininfo` 10 times (~6 minutes) then exits before opening gRPC ([common.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/common/common.go#L258-L279), [root.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/cmd/root.go#L407-L429)).

[^zinder-reader-start]: Readers start without contacting a node and recover on restart while `zinder-ingest` runs; `zinder-ingest` exits at startup if no node answers, so a full-stack start serves nothing until a node answers ([service-operations.md](../architecture/service-operations.md#startup-phases), [zinder-query main.rs](../../services/zinder-query/src/bin/zinder-query/main.rs#L172-L180), [deploying-single-volume.md](../runbooks/deploying-single-volume.md)).

[^ztr-built-before]: Only from an index built before the validator pruned those heights ([blocks/mod.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/service/src/compact/blocks/mod.rs#L143-L165)).

[^zingod-built-before]: Only from `compact_block` and `tree_state` indexes built or restored before pruning ([architecture.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/contributors/architecture.md#L18-L19)).

[^lwd-built-before]: From its on-disk block cache reloaded at startup unless `--redownload` is set; missing heights fail ([cache.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/common/cache.go#L155-L192), [common.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/common/common.go#L597-L619)).

[^zinder-pruned-later]: `zinder-ingest` checks storage mode at startup and on standby candidates; if its active node prunes later, ingest refuses it on next start and readers stop after `storage.serving_pair_staleness_ceiling_ms` (5 minutes default) ([node-source-boundary.md](../architecture/node-source-boundary.md#capabilities), [monitor.rs](../../services/zinder-ingest/src/node_failover/monitor.rs#L172-L178), [configuration.md](configuration.md#environment-variables)).

[^zakura-tx-retention]: A pruned Zakura deletes raw transactions outside `tx_retention` (at least 10,000 blocks) ([config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-state/src/config.rs#L482-L505)).

[^raw-forwarded]: The indexer fetches raw transactions from the validator, which deletes them outside `tx_retention` ([ztreamer state_backend.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/remote/src/backend/state_backend.rs#L80-L92), [Zingod transactions.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc/src/routes/transactions.rs#L64-L88), [Go lightwalletd service.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/frontend/service.go#L511-L532)).

[^zakura-checkpoint]: Only with `consensus.checkpoint_sync = false`; default `true` stops address indexes on a pruned node ([config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-state/src/config.rs#L224-L228), [consensus config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-consensus/src/config.rs#L129)).

[^ztr-taddr-pruned]: Forwards address history to Zakura, whose address index stops under default checkpoint sync and deletes old tx bytes ([address.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/service/src/compact/address.rs#L16), [Zakura config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-state/src/config.rs#L482-L505)).

[^zingod-taddr-pruned]: Forwards transaction bytes to the validator; `NotFound` ends the stream with `NOT_FOUND` ([transactions.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc/src/routes/transactions.rs#L74-L80)).

[^lwd-taddr-pruned]: Calls `getrawtransaction`, failing on transactions the pruned node deleted ([service.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/frontend/service.go#L221-L229)).

[^zakura-vct]: On Mainnet, requires `consensus.vct_fast_sync = false` ([service.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-state/src/service.rs#L2719-L2738), [consensus config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-consensus/src/config.rs#L41-L51)).

[^ztr-tree-index]: Local tree index by default; `--no-tree-index` forwards to the validator ([tree.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/service/src/compact/tree.rs#L18-L35), [cli.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/bin/ztreamerd/src/cli.rs#L150-L154)).

[^lwd-tree-pruned]: Forwards to `z_gettreestate`, which pruned Mainnet Zakura serves only with `consensus.vct_fast_sync = false` ([service.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/frontend/service.go#L454-L458), [Zakura service.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-state/src/service.rs#L2719-L2738)).

[^zebra-tls]: Plaintext HTTP/2; put a TLS reverse proxy in front ([rpc.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/config/rpc.rs#L64-L71)).

[^zakura-tls]: No lightwalletd gRPC listener; JSON-RPC offers `rpc.tls` and Indexer gRPC offers mTLS via `rpc.indexer_tls` ([rpc.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/config/rpc.rs#L96-L102)).

[^ztr-tls]: Plaintext by default; set `--tls-cert` and `--tls-key` ([cli.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/bin/ztreamerd/src/cli.rs#L134-L140)).

[^zingod-tls]: Refuses non-loopback binds unless `[serve] tls` is set ([serve_tls.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/config/serve_tls.rs#L71-L86)).

[^lwd-tls]: Required by default; `--no-tls-very-insecure` disables it ([root.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/cmd/root.go#L96-L106)).

[^zinder-tls]: Listeners speak plaintext h2c; public binds require `security.allow_public_bind = true` behind a TLS proxy ([bind_guard.rs](../../crates/zinder-runtime/src/bind_guard.rs#L97-L125), [service-operations.md](../architecture/service-operations.md#security-boundary)).

[^zebra-cookie]: Cookie authentication is on by default ([rpc.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/config/rpc.rs#L125-L126), [server.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/server.rs#L133-L139)).

[^zebra-grpc-no-auth]: Zebra's gRPC servers add no authentication layer ([Indexer server.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/indexer/server.rs#L71-L77), [lightwalletd server.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/lightwalletd/server.rs#L95-L102)).

[^zakura-cookie]: Cookie authentication is on by default ([rpc.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/config/rpc.rs#L208-L209)).

[^zakura-indexer-tls]: Requires `rpc.indexer_tls` (unset by default; required off loopback) with client certificates signed by `client_ca_file` ([rpc.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/config/rpc.rs#L98-L102), [server.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/indexer/server.rs#L212-L224)).

[^ztr-auth]: No client authentication on the lightwalletd/Replicator listener ([README.md](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/README.md#L81-L95)).

[^zinder-explorer-auth]: `zinder-explorer` checks `explorer.bearer_token_path` when set; `zinder-query` and `zinder-compat-lightwalletd` have no built-in client authentication ([config.rs](../../services/zinder-explorer/src/bin/zinder-explorer/config.rs#L30-L31), [explorer-plane.md](../architecture/explorer-plane.md#turn-it-on)).

[^forwarded]: Forwards all or part of this call to its validator; see [When the validator is down](#when-the-validator-is-down) ([ztreamer state_backend.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/remote/src/backend/state_backend.rs#L123-L253), [Zingod transactions.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc/src/routes/transactions.rs#L22-L88), Go lightwalletd service.go [addresses](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/frontend/service.go#L140-L235) and [other calls](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/frontend/service.go#L407-L642), [Zinder lib.rs](../../services/zinder-query/src/lib.rs#L3-L6)).

[^zinder-tree-local]: Served from Zinder's store with no validator call; heights between checkpoints rebuild from compact blocks up to one per CPU concurrently ([lightwalletd-compatibility.md](lightwalletd-compatibility.md#gettreestate-and-getlatesttreestate), [tree_state_replay_limit.rs](../../services/zinder-query/src/tree_state_replay_limit.rs#L58-L60)).

[^zingod-metrics]: Needs `[metrics] listen_address`, off by default ([config.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/config.rs#L525-L531)).

[^zinder-secondaries]: `zinder-query`, `zinder-compat-lightwalletd`, and `zinder-explorer` open separate RocksDB secondaries over shared primaries on one host ([ADR-0004](../adrs/0004-storage-and-deployment-topologies.md)).

[^ztr-replica]: Remote mode imports sealed ranges once at startup from `--replicate-from`, then follows `--remote-rpc` ([replicate/mod.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/replicate/mod.rs#L121-L160), [README.md](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/README.md#L59-L66)).

[^zinder-single-host]: Only the single-host `rocksdb` topology is implemented; `postgres` horizontal serving is decided but not implemented ([ADR-0004](../adrs/0004-storage-and-deployment-topologies.md)).

[^zebra-snap]: Unsigned state snapshots extracted into `cache_dir` by hand ([snapshots.md](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/book/src/user/snapshots.md#L52-L57)).

[^zakura-snap]: Published state snapshots (~11 GB for pruned state) downloaded outside `zakurad` ([README.md](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/README.md#L26)).

[^ztr-snap]: Remote mode fetches a signed Mainnet snapshot by default (Testnet needs `--replicate-from`); embedded mode indexes from Zakura ([replicate/mod.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/replicate/mod.rs#L75-L80), [README.md](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/README.md#L59-L63)).

[^zingod-snap]: Uses a `[snapshot]` manifest and `aria2c` to populate empty index directories ([config.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod/src/config.rs#L533-L545)).

[^zinder-snap]: `zinder-ingest export-edge-snapshot` and `zinder-ingest bootstrap-from-snapshot` verify Ed25519-signed canonical checkpoints (with optional `sealed-history/`) against validator consensus anchors ([ADR-0024](../adrs/0024-verified-edge-snapshot-bootstrap.md)). For coherent canonical + wallet recovery on private deployments, use `zinder-projector backup` and `zinder-projector restore` ([private-backups.md](../runbooks/private-backups.md)).

[^zingod-image]: `zingodevops/zingod` Docker image (`linux/amd64` only) ([release.yaml](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/.github/workflows/release.yaml#L120-L135)).

[^lwd-image]: Docker images only, no standalone binaries ([RELEASING.md](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/RELEASING.md#L77-L86)).

[^zinder-processes]: `light-wallet` runs `zinder-ingest` and `zinder-compat-lightwalletd` (plus optional `zinder-history-publisher`) beside a validator ([ADR-0023](../adrs/0023-deployment-profiles.md)). Shipped `wallet` Compose runs 5 Zinder processes (`zinder-ingest`, `zinder-projector`, `zinder-history-publisher`, `zinder-query`, `zinder-compat-lightwalletd`) plus a validator = 6 processes, or 7 with `zinder-explorer` ([docker-compose.yml](../../deploy/docker-compose.yml), [release-images.json](../../deploy/release-images.json), [ADR-0004](../adrs/0004-storage-and-deployment-topologies.md)).

[^zinder-node-targets]: `zinder-ingest` follows `[node]` and `[node_fallbacks.N]`. `zinder-query` and wallet-mode `zinder-compat-lightwalletd` use the list for broadcasts and fee reads; `zinder-explorer` reads `[node]` only; `zinder-projector` and `compact-blocks` reject node settings ([node-source-boundary.md](../architecture/node-source-boundary.md#ordered-node-list), [wallet-serving-runtime-entrypoint](../../deploy/wallet-serving-runtime-entrypoint#L86-L90), [lightwalletd-compatibility.md](lightwalletd-compatibility.md#getstandardfee)).

[^zebra-disk]: Zebra book figure; extracting a snapshot temporarily needs twice that ([requirements.md](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/book/src/user/requirements.md#L23-L26)).

[^zakura-disk]: `zcashd-compat` preflight checks 275 GiB; README lists an ~11 GB pruned snapshot ([preflight.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakurad/src/components/zcashd_compat/preflight.rs#L47), [README.md](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/README.md#L26)).

[^ztr-disk]: 17.72 GiB index at Mainnet ~3,587,000 in v0.2.0 benchmark (17.3 GiB at 3,532,489); Zakura state size not stated ([REPORT.md](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/benchmarks/2026-11-v0.2.0-blog/REPORT.md#L17), [README.md](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/README.md#L120)).

[^zingod-disk]: Live sync test provisions 160 GiB and marks it UNMEASURED ([zingod_index_construction.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/live-tests/sync/tests/zingod_index_construction.rs#L36-L37)).

[^zinder-disk]: Measured on `v0.26.0` (`f40fa20`) at Mainnet height 3,593,347 (`ext4 -O verity`, validator disk separate): `light-wallet` `71.76 GB` (`70.97 GB` with `commitment-budget = 1024`); `wallet` `122.64 GB` final / `131.80 GB` peak (~50.65 GB canonical + ~50.86 GB wallet + ~20.92 GB signed `ZSHRNG01` catalog, plus ~130 GB free recommended for sorter spill during wallet build); retaining raw transaction blobs via `storage.raw_blob_policy = "transactions"` adds `~251.1 GiB`, and `wallet-archive` (`raw_blob_policy = "all"`) retains raw block blobs as well ([ADR-0004](../adrs/0004-storage-and-deployment-topologies.md), [ADR-0023](../adrs/0023-deployment-profiles.md), [deploying.md](../runbooks/deploying.md#what-you-need), [initial-sync.md](../runbooks/initial-sync.md)).

[^zebra-sync]: A Zcash Foundation snapshot starts the node near the tip ([snapshots.md](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/book/src/user/snapshots.md#L3-L5)).

[^zakura-sync]: Checkpoint sync and `vct_fast_sync` are on by default ([config.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-consensus/src/config.rs#L129-L130)).

[^ztr-sync]: Indicative single-run v0.2.0 benchmark times: remote mode imported 18.2 GB in 695 s (11.6 min) at ~26 MB/s; embedded mode indexed in 165 s (5.0 min with trees) on synced Zakura state (92 s on a warm-cache host per README) ([REPORT.md](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/benchmarks/2026-11-v0.2.0-blog/REPORT.md#L12-L13), [README.md](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/README.md#L134-L144)).

[^lwd-sync]: README estimate; serves requests while the cache fills ([README.md](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/README.md#L131-L133)).

[^zinder-sync]: Measured on `v0.26.0` (`f40fa20`) at Mainnet height 3,593,347 on 8 writer cores (`c4-standard-32`, `MemoryMax=64G`) against Zebra 7.1.0: `light-wallet` `60.4 min` (`58.4 min` canonical + `123.2 s` 4-worker catalog publish), or `48.6 min` with `commitment-budget = 1024`; `wallet` `75.8 min` (`59.4 min` canonical + `14.4 min` wallet projection + `123.2 s` publish) ([ADR-0004](../adrs/0004-storage-and-deployment-topologies.md), [ADR-0023](../adrs/0023-deployment-profiles.md), [deploying.md](../runbooks/deploying.md#what-you-need)).

[^zebra-limits]: 20 HTTP/2 streams/connection, 50 MiB JSON-RPC responses, 10,000 addresses, 20,000 mempool exclude entries ([server.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/lightwalletd/server.rs#L24), [rpc.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/config/rpc.rs#L128-L129), [methods.rs](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/methods.rs#L4186)).

[^zakura-limits]: JSON-RPC up to 2x max block size, 50 MiB responses, batches of 1,000; Indexer gRPC caps 64 connections, 64 streams, 4 KiB requests ([server.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/server.rs#L127-L142), [indexer server.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/indexer/server.rs#L37-L49)).

[^ztr-limits]: 100 HTTP/2 streams/connection, 16 block-range streams (10 s wait), 10,000 balance-stream addresses, 2,000,000-byte txs ([config.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/daemon/src/config.rs#L150), [range_limit.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/service/src/compact/range_limit.rs#L10-L15)).

[^zingod-limits]: 4,096 connections (128/IP), 100 streams/connection, 2,048 work streams, adaptive admission, 100,000 address rows ([limits.rs](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/packages/zingod-grpc/src/limits.rs#L61-L91), [methods.md](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/wallets/methods.md#L83-L84)).

[^lwd-limits]: 100 streams/connection, 200 concurrent block ranges (10 s queue), 10,000 addresses, 10M-block address span, 2 MB txs, 60 s stalled-send timeout, 32 concurrent receives/pool, 30 s idle timeout, 16 MiB concurrent send budget ([v0.6.2 release notes](https://github.com/zcash/lightwalletd/releases/tag/v0.6.2), [root.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/cmd/root.go#L454-L455), [service.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/frontend/service.go#L46-L85), [sendtimeout.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/cmd/sendtimeout.go#L16)).

[^zinder-limits]: 100 concurrent block-range streams by default (300 queued), 200 HTTP/2 streams/connection, 100,000 UTXOs/response, 256 distinct addresses/balance request (64 bytes max), 100,000 txs per `GetMempoolStream`, 1 tree-state rebuild/CPU concurrently; per-client rate limits belong at the proxy ([configuration.md](configuration.md#listener-and-request-limits), [lightwalletd-compatibility.md](lightwalletd-compatibility.md#request-limits), [transparent_address.rs](../../crates/zinder-source/src/transparent_address.rs#L14-L19), [grpc.rs](../../services/zinder-compat-lightwalletd/src/grpc.rs#L65-L97), [tree_state_replay_limit.rs](../../services/zinder-query/src/tree_state_replay_limit.rs#L58-L60)).

[^zebra-reorg]: Side-chain txs report height `u64::MAX`; compact reads return `ABORTED` when their tree was reorged away ([transaction lookup](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/lightwalletd/methods.rs#L208-L214), [compact reads](https://github.com/ZcashFoundation/zebra/blob/86f3ac7e4dcf7644a88222b8678790ed68734e63/zebra-rpc/src/lightwalletd/methods.rs#L686-L694)).

[^zakura-reorg]: `ChainTipChange` sends the new tip; `NonFinalizedStateChange` streams fork blocks ([methods.rs](https://github.com/zakura-core/zakura/blob/dd25aa429cd97eb1af903b7bc6375fe208132015/crates/zakura-rpc/src/indexer/methods.rs#L65-L110)).

[^ztr-reorg]: Reconciles internally and ends affected streams with `UNAVAILABLE` ([compact/mod.rs](https://github.com/zcashlabs/ztreamer/blob/498258159d868d5592ff44c75e9b311353d51e35/crates/service/src/compact/mod.rs#L301-L318)).

[^zingod-reorg]: `GetMempoolStream` also ends ([served tip](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/contributors/architecture.md#L146-L180), [mempool stream](https://github.com/zingolabs/zingod/blob/3664ce0bbea3f1945c84c8730359795b84959ed2/book/src/contributors/architecture.md#L300-L308)).

[^lwd-reorg]: Cache truncates internally; `GetBlockRange` may return `ABORTED` during repair ([common.go](https://github.com/zcash/lightwalletd/blob/f8fe2cca7ede03b1f06b1d637a2ab467d151b79d/common/common.go#L746-L756)).

[^zinder-reorg]: `ChainReorged` arrives on `WalletQuery.ChainEvents`; reorgs deeper than the 1,000-block window park `zinder-ingest` and readers refuse requests with `REORG_WINDOW_EXCEEDED` ([chain-events.md](../architecture/chain-events.md#event-contents), [ADR-0010](../adrs/0010-reorg-window-and-displaced-blocks.md), [lightwalletd-compatibility.md](lightwalletd-compatibility.md#getblockrange-and-getblockrangenullifiers)).
