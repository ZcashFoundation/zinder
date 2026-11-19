# Performance

This page gives measured performance figures for Zinder and the conditions
behind each one. Use it to size a wallet-serving deployment, or to compare
Zinder releases with each other and with Go lightwalletd on the same
validator.

On this page:

- [Wallet serving under load](#wallet-serving-under-load)
- [Tree-state checkpoint pass](#tree-state-checkpoint-pass)
- [Not yet measured](#not-yet-measured)

## Wallet serving under load

Many light wallets syncing at once from one `zinder-compat-lightwalletd` in
`wallet` serving mode, on Regtest and Testnet. The figures compare three
servers:
- Zinder 0.19.0 with its default settings;
- Zinder 0.18.0;
- Go lightwalletd v0.6.0.

All three followed the same Zebra node.

In summary, at 32 concurrent wallets on Testnet:
- No Zinder 0.19.0 wallet failed, and no call returned an error.
- Time to first block is 1.5 to 58 times shorter than on 0.18.0.
- Block throughput is 6.4 to 16 times higher than on 0.18.0, and 0.5 to 0.9
  times Go lightwalletd's.
- Under the public Compact Block Bench at 50 to 200 wallets, Zinder 0.19.0
  refuses no request on either tier.

### How it was measured

| | |
|---|---|
| Servers | Zinder 0.19.0 and Zinder 0.18.0, each built with `cargo build --release --locked`, and Go lightwalletd v0.6.0 built from its repository's Dockerfile. Zinder ran zinder-ingest, zinder-projector, and zinder-compat-lightwalletd with `compat.serving = "wallet"`, `storage.raw_blob_policy = "transactions"` as in the shipped wallet-serving configuration, and explorer views off. Every 0.19.0 serving limit, wait, and the tree-state checkpoint pass ran at its default. |
| Validator | Zebra 7.1.0, one node per tier, shared by every server on that tier. |
| Host | 32 CPUs, 125 GiB of memory, ext4 on md RAID, Linux 7.0. The host was shared: the 1-minute load average from other work is given with each table. |
| CPU pinning | The server under test on 8 CPUs, so Zinder runs 8 tree-state rebuilds at once. zinder-ingest and zinder-projector on 4 CPUs. The load client on 8 CPUs. The nodes on the remaining 12. |
| Repeats | Each wallet-load case ran 3 times. Tables give the median, and a range in parentheses where repeats differ by more than 10 percent. |

#### Tiers

- **Regtest:** a Zebra Regtest chain mined to 7,400 blocks. Each 28-block
  cycle has 20 blocks that pay shielded coinbase notes and 8 that each pay
  one of 8 transparent addresses, so tree-state rebuilds replay real note commitments
  and each address holds about 260 coinbase outputs. Wallets sync heights 37
  to 7,400. Regtest blocks are small, so throughput there measures request
  handling, not data volume.
- **Testnet:** Zebra at the Testnet tip, near height 4,638,000. Wallets sync
  4,580,011 to 4,600,000, 19,990 blocks. Zinder serves a store built to
  4,600,000 and then followed to the tip. Testnet point reads ran without
  transparent addresses.

On Regtest, Zinder 0.19.0 builds a fresh store up to one reorg window
(1,000 blocks) below the tip and follows the rest, storing a tree-state
checkpoint at each of those blocks. So a Regtest tree state near the tip is
stored on 0.19.0 and rebuilt on 0.18.0. On Testnet, both Zinder stores start
from the same copy, and the checkpoint pass adds no checkpoint in the
wallet-load range, so the Zinder tree-state figures compare like with like.

#### Client workloads

The load client is the `zinder-bench lightwalletd-wallet-load` command,
described in
[section 7 of the zinder-bench README](../../services/zinder-bench/README.md#7-drive-wallet-shaped-load-at-an-endpoint).
Each simulated wallet follows the request pattern of one wallet engine:

- **Slipstream**, as in the ZODL apps: parallel block-range streams with a
  tree state after each 8 MiB piece, and transaction lookups.
- **Vizor:** prefetched block ranges, each paired with a tree state, with
  quick retries.
- **Classic SDK**, as in the Zcash Android SDK: 1,000-block batches, each
  followed by a tree state, retried with backoff.
- **Point reads:** `GetLightdInfo`, `GetLatestBlock`, `GetTreeState` at a
  stored checkpoint and at the height one below it, and, on Regtest,
  `GetTaddressTxids` and `GetAddressUtxosStream` for the 8 transparent
  addresses.

The **Compact Block Bench** (cbb) ran at its published settings: 50, 100,
150, and 200 concurrent wallets, 50 steps of 100-block ranges each, a tree
state every 5 ranges, and a cold and a warm pass, from height 2,000,000 on
Testnet and 37 on Regtest. The revision used was `6dffa2e9`. Its published
results are for Mainnet and are not comparable with these.

### Testnet results

Load average 6.2-11.9 for Zinder 0.19.0, 1.7-14.0 for Zinder 0.18.0, and
5.1-6.4 for Go lightwalletd.

At 32 concurrent wallets:

| Workload | Measure | Zinder 0.19.0 | Zinder 0.18.0 | Go lightwalletd v0.6.0 |
|---|---|---:|---:|---:|
| Classic SDK | Failed wallets | 0 | 0 | 0 |
| Classic SDK | Calls with an error | 0 | 46 | 0 |
| Classic SDK | Time to first block p50 | 33.9 ms | 542.0 ms | 6.1 ms |
| Classic SDK | Blocks per second | 1,001,324 | 156,167 | 1,361,479 |
| Slipstream | Failed wallets | 0 | 2 (0-4) | 0 |
| Slipstream | Calls with an error | 0 | 39 | 0 |
| Slipstream | Time to first block p50 | 17.4 ms | 1,008.7 ms | 4.1 ms |
| Slipstream | Blocks per second | 1,402,767 | 144,896 | 1,537,982 |
| Vizor | Failed wallets | 0 | 0 | 0 |
| Vizor | Calls with an error | 0 | 382 | 0 |
| Vizor | Time to first block p50 | 6.4 ms | 9.5 ms | 0.8 ms |
| Vizor | Blocks per second | 695,355 | 42,383 | 1,407,389 |

The errors on Zinder 0.18.0 are refused tree-state rebuilds. For Vizor they
also include the block ranges it cancels when a paired tree state is
refused.

`GetTreeState` latency, p50 / p99:

| Workload | Zinder 0.19.0 | Zinder 0.18.0 | Go lightwalletd v0.6.0 |
|---|---:|---:|---:|
| Classic SDK, 1 wallet | 9.5 / 10.6 ms | 13.5 / 16.8 ms | 0.5 / 0.7 ms |
| Classic SDK, 32 wallets | 21.6 / 39.0 ms | 12.0 / 31.0 ms | 3.0 / 32.9 ms |
| Slipstream, 32 wallets | 30.0 / 204.7 ms | 19.8 / 51.6 ms | 4.7 / 72.0 ms |
| Vizor, 32 wallets | 166.8 / 239.7 ms | 19.6 / 63.8 ms | 12.2 / 130.7 ms |

At 32 wallets, Zinder 0.19.0 answers tree states that Zinder 0.18.0 refused,
and a request that finds every rebuild slot busy waits for one. The slowest
tree state in any 0.19.0 run took 259 ms. How the wait and its limits work
is in [`GetTreeState`](lightwalletd-compatibility.md#gettreestate-and-getlatesttreestate).

At 1 wallet:

| Workload, 1 wallet | Zinder 0.19.0 | Zinder 0.18.0 | Go lightwalletd v0.6.0 |
|---|---:|---:|---:|
| Classic SDK, blocks per second | 76,460 | 49,883 | 269,438 |
| Slipstream, blocks per second | 281,263 | 282,911 | 331,673 |
| Vizor, blocks per second | 282,272 | 290,856 | 943,961 |

For one wallet on Zinder 0.19.0, 72 to 94 percent of the server's
block-range time is reading stored compact blocks. Converting them to
lightwalletd messages takes 3 to 15 percent, and waiting for the client to
accept a chunk takes 0 to 16 percent. The
`zinder_compat_block_range_stage_duration_seconds` series in
[Metrics](metrics.md#zinder-compat-lightwalletd) reports these stages.

Point reads, p50:

| Call | Wallets | Zinder 0.19.0 | Zinder 0.18.0 | Go lightwalletd v0.6.0 |
|---|---:|---:|---:|---:|
| `GetLightdInfo` | 1 | 0.09 ms | 0.58 ms | 0.64 ms |
| `GetLightdInfo` | 32 | 0.07 ms | 0.33 ms | 2.51 ms |
| `GetLatestBlock` | 1 | 0.07 ms | 0.38 ms | 0.51 ms |
| `GetLatestBlock` | 32 | 0.05 ms | 0.29 ms | 1.88 ms |
| `GetTreeState`, stored checkpoint | 1 | 0.12 ms | 0.58 ms | 0.30 ms |
| `GetTreeState`, rebuilt height | 1 | 12.57 ms | 18.39 ms | 0.29 ms |
| `GetTreeState`, rebuilt height | 32 | 54.14 ms | 16.86 ms | 0.71 ms |

At 32 wallets, Zinder 0.18.0 refused 556 rebuilt-height tree states per
run, and Zinder 0.19.0 refused none.

#### Compact Block Bench on Testnet

`GetTreeState` errors per pass, and overall p50 / p99 latency at 200
wallets:

| Concurrency | Zinder 0.19.0 | Zinder 0.18.0 | Go lightwalletd v0.6.0 |
|---:|---:|---:|---:|
| 50 | 0 | 25-42 | 0 |
| 100 | 0 | 95-109 | 0 |
| 150 | 0 | 129-163 | 0 |
| 200 | 0 | 142-209 | 0 |
| 200, p50 / p99 | 2.9-3.1 / 52.8-62.1 ms | 7.0-7.3 / 26.3-28.7 ms | 3.0-3.5 / 35.7-51.2 ms |

Zinder 0.18.0 also refused 92-134 block ranges per pass at 200 wallets, and
Zinder 0.19.0 refused none. Zinder 0.19.0 ran each pass 3 times, and the
other two servers once. cbb load average was 2.7-7.0 for Zinder 0.19.0.

### Regtest results

Load average 8.3-9.1 for Zinder 0.19.0, 3.2-14.1 for Zinder 0.18.0, and
1.8-4.4 for Go lightwalletd.

At 32 concurrent wallets:

| Workload | Measure | Zinder 0.19.0 | Zinder 0.18.0 | Go lightwalletd v0.6.0 |
|---|---|---:|---:|---:|
| Classic SDK | Calls with an error | 0 | 48 | 0 |
| Classic SDK | Time to first block p50 | 27.7 ms | 517.9 ms | 5.5 ms |
| Classic SDK | Blocks per second | 1,236,574 | 65,376 | 1,412,955 |
| Slipstream | Failed wallets | 0 | 7 (7-8) | 0 |
| Slipstream | Time to first block p50 | 31.0 ms | 1,011.8 ms | 8.0 ms |
| Slipstream | Blocks per second | 2,212,583 | 45,182 | 775,765 |
| Vizor | Calls with an error | 0 | 377 | 0 |
| Vizor | Time to first block p50 | 0.9 ms | 2,005.2 ms | 2.0 ms |
| Vizor | Blocks per second | 981,597 | 13,351 | 1,248,651 |

Go lightwalletd answered 142 to 232 Slipstream `GetTransaction` calls per
run with `NOT_FOUND`. No Slipstream wallet failed on it.

Transparent reads, p50:

| Call | Wallets | Zinder 0.19.0 | Zinder 0.18.0 | Go lightwalletd v0.6.0 |
|---|---:|---:|---:|---:|
| `GetTaddressTxids` | 1 | 2.35 ms | 4.55 ms | 160.32 ms |
| `GetTaddressTxids` | 32 | 20.57 ms | 35.46 ms | 3,777.45 ms |
| `GetAddressUtxosStream` | 1 | 2.51 ms | 2.60 ms | 8.41 ms |
| `GetAddressUtxosStream` | 4 | 2.80 ms | 2.71 ms | 9.50 ms |
| `GetAddressUtxosStream` | 16 | 4.36 ms | 3.91 ms | 9.33 ms |
| `GetAddressUtxosStream` | 32 | 5.28 ms | 5.51 ms | 9.76 ms |

Go lightwalletd sends each unspent output in its own small HTTP/2 frame. The
load client's HTTP/2 library closes a connection after a burst of such
frames, so some of its Go lightwalletd UTXO streams ended early on the client
side, and their latencies cover the streams that completed. A Go gRPC client
receives every output.

#### Compact Block Bench on Regtest

`GetTreeState` errors per pass, and overall p50 / p99 latency at 200
wallets:

| Concurrency | Zinder 0.19.0 | Zinder 0.18.0 | Go lightwalletd v0.6.0 |
|---:|---:|---:|---:|
| 50 | 0 | 360-365 | 0 |
| 100 | 0 | 754-801 | 0 |
| 150 | 0 | 1,177-1,201 | 0 |
| 200 | 0 | 1,611-1,658 | 0 |
| 200, p50 / p99 | 1.7-1.9 / 219.3-224.1 ms | 8.7-9.1 / 60.1-60.2 ms | 1.8 / 62.8-69.4 ms |

Zinder 0.18.0 also refused 1-773 block ranges per pass at 150-200 wallets.
On Regtest every cbb tree state is a rebuild of 3 to 30 ms, because each
Regtest block carries shielded coinbase notes. So at 150-200 wallets nearly
every rebuild on Zinder 0.19.0 waits 0.1 to 0.3 s for one of its 8 slots, and
no wait reaches 1 s. cbb load average was 1.3-8.9 for Zinder 0.19.0 and
4.2-9.5 for Zinder 0.18.0.

## Tree-state checkpoint pass

While it follows the chain, zinder-ingest adds tree-state checkpoints to
settled history in the background, as
[Tree-state densification](../architecture/chain-ingestion.md#tree-state-densification)
describes. These figures are for the Testnet store above, at tip height
4,638,000, with ingest on 4 CPUs.

### The default selection: one checkpoint per 1,024 note commitments

Zinder 0.19.0, in 3 runs, each on a store already following the tip:

| | Each run |
|---|---:|
| Elapsed | 304-307 s |
| zinder-ingest CPU above following | about 72 s |
| Strides densified | 136 |
| Checkpoints written | 362 |
| Note commitments replayed | 494,924 |

Live commits, 3 runs together:

| | Pass disabled | Pass running |
|---|---:|---:|
| Live commits | 26 | 25 |
| Commit latency p50 | 0.62 ms | 0.55 ms |
| Commit latency p95 | 1.05 ms | 1.25 ms |
| Commit latency max | 1.09 ms | 2.10 ms |
| Delay from the node's new block to Zinder serving it, p50 / max | 0 / 1 s | 0 / 1 s |

The delay is sampled once a second. Commit latency is
`zinder_ingest_canonical_live_commit_seconds`.

The densified checkpoints sit in Testnet's densest strides. A replay starts
from the nearest checkpoint at or below its height, so the worst rebuild in
heights 4,500,000 to 4,600,000 drops from 0.57 s to 0.061 s. The wallet-load
ranges above contain none of these strides, so their figures are the same
with or without the pass.

### One checkpoint per block

`ingest.tree_state_densification.selection = "every-block"`, measured on a
pre-release build of Zinder 0.19.0 on the same Testnet store:

| | Value |
|---|---:|
| Elapsed | 4 h 15 min |
| zinder-ingest CPU | about 6,840 s above following, 0.45 of a CPU on average |
| Pace | 270-334 blocks per second, under the 500 blocks per second limit |
| Checkpoints written | 4,554,000 |
| Note commitments replayed | 1,520,923 |
| Store growth | 828 MB on a 12.0 GB store, about 182 bytes per checkpoint, measured 30 s after the pass |

Over 15 minutes of following during that pass, the median live commit took
0.39 ms, and 7 of 57 commits took 53 to 211 ms. Ingest never fell behind the
node by a block.

On Zinder 0.19.0, two runs of the first 20 minutes of the pass, each on a
store already following the tip, wrote 431,343 to 437,877 checkpoints, about
360 blocks per second. zinder-ingest used about 0.27 of a CPU. Both runs
together:

| | Pass disabled, 2 × 300 s | Pass running, 2 × 20 min |
|---|---:|---:|
| Live commits | 27 | 85 |
| Commit latency p50 | 0.62 ms | 0.23 ms |
| Commit latency p95 | 0.95 ms | 2.51 ms |
| Commit latency max | 1.11 ms | 133.86 ms |
| Commits over 10 ms | 0 | 3 (49, 55, and 134 ms) |
| Delay from the node's new block to Zinder serving it, p50 / max | 1 / 1 s | 0 / 2 s |

On the every-block store, every tree state is read from a stored checkpoint
in about 0.1 ms. Under the Compact Block Bench on Testnet, it refuses no
request at 50 to 200 wallets, and its overall p99 at 200 wallets is
16.8-18.4 ms. At 32 wallets it streams 1,750,451 blocks per second for
Classic SDK, 1,903,234 for Slipstream, and 1,418,038 for Vizor.

## Not yet measured

- **Mainnet:** no figure on this page comes from Mainnet. Mainnet block
  density, store size, and disk class differ from Testnet's.
- **The checkpoint pass's settled disk cost:** the store growth above was
  measured 30 s after the pass, before compaction was confirmed settled.
- **Transparent reads on Testnet.**
