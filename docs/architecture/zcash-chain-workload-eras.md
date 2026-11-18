# Zcash chain workload eras

Zcash mainnet blocks are not uniform. Some height ranges hold far more
transactions, inputs, or shielded actions than others, so a first sync slows
down and speeds up as it moves through history. This page lists the ranges that
matter when you read a sync benchmark, compare two runs, or estimate how long a
sync will take. It is background for operators and contributors, not a
consensus table.

Two kinds of ranges matter:

- A **network upgrade** changes consensus rules at an activation height, for
  example by adding a transaction version or a shielded pool. Zinder reads
  activation heights from the node at runtime, because Testnet, Regtest, and
  custom networks use different heights.
- A **workload band** is a period of unusually heavy or unusual usage, such as
  a spam attack. It is measurement context only.

Zinder's code never branches on a workload band. Ingest sizes its node requests
and in-flight work by bytes rather than by height, so a dense range takes
longer without needing a special case.

## Network upgrades

| Upgrade | Mainnet heights | What changes for an indexer |
| --- | ---: | --- |
| Sprout | `0..347499` | Launch rules, transparent and Sprout transactions, 150-second target spacing |
| Overwinter | `347500..419199` | Transaction expiry, replay protection, and faster transparent transaction validation |
| Sapling | `419200..653599` | Sapling shielded pool, the first compact-block scanning shape for light wallets |
| Blossom | `653600..902999` | Target spacing drops to 75 seconds, so blocks arrive twice as often |
| Heartwood | `903000..1046399` | Shielded coinbase and FlyClient header commitments |
| Canopy | `1046400..1687103` | First halving and development fund |
| NU5 | `1687104..2726399` | Transaction v5, Orchard, Unified Addresses, and non-malleable transaction IDs: the largest parser and compact-block change |
| NU6 | `2726400..3146399` | Funding and lockbox rules; no new transaction version |
| NU6.1 | from `3146400` | Further funding and lockbox rules; no new transaction version |

Later upgrades, including NU6.3 (Ironwood, with version 6 transactions and the
Ironwood shielded pool) and NU7, take their activation heights from the node.
Run `getblockchaininfo` against your node to see them.
[ZIP 204](https://zips.z.cash/zip-0204) lists the published heights through
NU6.1.

## Workload bands

| Band | Approximate heights | Why it matters |
| --- | ---: | --- |
| May 2017 transparent-input stress | `~106178..~108888` | Transactions with many transparent inputs; stresses spend lookups |
| NU5 activation | around `1687104` | Parser and compact-block shape change |
| Post-NU5 shielded spam ("sandblasting") | `~1702296..~2175692` | Unusually large shielded transactions and high transaction load after NU5 |
| Heavy blocks named in zcashd 5.1.0 release notes | `1708048`, `1723244` | Single worst-case blocks for shielded parsing |

The May 2017 and post-NU5 boundaries come from dates converted to heights.
They select useful measurement ranges, not exact start and end points.

## Benchmark anchors

When you measure a change to parsing, compact blocks, or sync, include a window
around each of these heights so one workload shape cannot hide a regression in
another:

| Height | Reason |
| ---: | --- |
| `106178`, `108099`, `108888` | Start, middle, and end of the May 2017 stress |
| `347500`, `419200`, `653600`, `903000`, `1046400` | Overwinter, Sapling, Blossom, Heartwood, and Canopy activations |
| `1687104` | NU5 activation |
| `1702296`, `1708048`, `1723244` | Start of post-NU5 load and two heavy blocks |
| `2175692` | Load back to pre-NU5 levels |
| `2726400`, `3146400` | NU6 and NU6.1 activations |

`zinder-bench rocksdb-canonical-fixture-replay` replays a captured range
through the production construction pipeline into a fresh store and reports
`blocks_per_second`, `stage_durations`, `source_fetch_attribution`, byte
counts, and peak memory. Compare two runs only when they use the same fixture
digest, raw-blob retention, resource limits, and preparation concurrency.

## Reading a slow range

- Slow progress around `1687104` usually reflects the NU5 parser and
  compact-block change.
- Slow progress in `1702296..2175692` usually reflects post-NU5 shielded load.
- Slow progress in `106178..108888` usually reflects transparent spend lookups.

For what a first sync does phase by phase, see
[initial sync](../runbooks/initial-sync.md). For resource settings that change
sync speed, see
[bulk catch-up resource tuning](../runbooks/bulk-catchup-resource-tuning.md).

## Sources

- [Zcash network upgrade pages](https://z.cash/upgrade/) and
  [ZIP 204](https://zips.z.cash/zip-0204) for activation heights.
- [A look back: NU5 and network sandblasting](https://electriccoin.co/blog/a-look-back-nu5-and-network-sandblasting/)
  and
  [Heavily increased transaction load since June 14](https://forum.zcashcommunity.com/t/heavily-increased-transaction-load-since-june-14/42349)
  for the post-NU5 band.
- [Performance issues on the network](https://forum.zcashcommunity.com/t/performance-issues-on-the-network/15664)
  for the May 2017 band.
- [zcashd 5.1.0 release notes](https://github.com/zcash/zcash/blob/3c2b1622f0a6c047e7f91fafb01965279c252d2a/doc/release-notes/release-notes-5.1.0.md)
  for the heavy blocks.
