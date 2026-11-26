# Zinder

> [!WARNING]
> **Zinder is alpha.** APIs, storage layouts, and deployment topology will change, and a tagged release is not a stability promise. Pin exact versions and expect breaking upgrades.

Zinder is a self-hosted Zcash indexer. It reads an archive [Zebra](https://github.com/ZcashFoundation/zebra) or [Zakura](https://github.com/zakura-core/zakura) node, stores one canonical index, and serves wallets, backends, and optional explorers from that consistent view. Keys, note scanning, and shielded balances stay in the wallet.

## Why choose Zinder

- **One index, multiple protocols.** Serve [lightwalletd](https://github.com/zcash/lightwalletd) `CompactTxStreamer` (with native gRPC-Web for browsers), native `WalletQuery` / `zinder-client`, and optional `ExplorerQuery` from one store.
- **Reorg-safe snapshots and resumable events.** Pin multiple native calls to one chain epoch so syncs never mix competing tips, and follow blocks and reorgs through durable `ChainEvents` cursors.
- **Serving through node outages.** Keep answering stored block, tree-state, and transparent queries while the node is down, with ingest and broadcast failover across certified Zebra 7.1.0 and Zakura 1.8.6 nodes.
- **Verified data path.** Serve Ed25519-signed `fs-verity` shielded segments, SHA-256 verified transaction bodies, and verified edge-snapshot bootstraps.

See [Features and roadmap](docs/features.md) for the full breakdown and [Indexer comparison](docs/reference/indexer-comparison.md) for a side-by-side matrix against Zebra, Zakura, ztreamer, Zingod, and Go lightwalletd.

## Try it without deploying

Connect a wallet, `grpcurl`, or `zinder-client` to the Zcash Foundation's public gRPC-over-TLS endpoints (not websites; see [Endpoints](docs/reference/integration-surfaces.md#endpoints) for limits and retries):

| Protocol | Mainnet | Testnet |
| --- | --- | --- |
| lightwalletd `CompactTxStreamer` | `https://lwd.zfnd.org:443` | `https://testnet.lwd.zfnd.org:443` |
| Native `WalletQuery` (`zinder-client`) | `https://zinder.zfnd.org:443` | `https://testnet.zinder.zfnd.org:443` |

## Mainnet footprint (`v0.26.0`)

Measured on Mainnet against Zebra 7.1.0 on 8 writer cores:

- **`light-wallet`:** ~72 GB disk, ~49–60 min first sync (`CompactTxStreamer`, node-forwarded transparent reads).
- **`wallet` (recommended default):** ~123 GB disk, ~76 min first sync (local transparent indexes, verified node-forwarded transaction bodies).

See [Deploying Zinder](docs/runbooks/deploying.md) for host sizing, optional `wallet-archive` blob retention, and the Docker Compose quickstart.

## Start here

| You want to | Read |
| --- | --- |
| Explore features, profiles, and roadmap | [Features and roadmap](docs/features.md) |
| Match Zinder to your product | [Use cases](docs/use-cases.md) |
| Compare Zcash indexers | [Indexer comparison](docs/reference/indexer-comparison.md) |
| Deploy Zinder | [Deploying Zinder](docs/runbooks/deploying.md) |
| Connect a wallet or backend | [Integration surfaces](docs/reference/integration-surfaces.md) |
| Understand the architecture | [How Zinder works](docs/architecture/overview.md) |
| Contribute | [Testing](docs/runbooks/testing.md) and [CLAUDE.md](CLAUDE.md) |

Browse the [documentation index](docs/README.md) or the [navigation tree](docs/SUMMARY.md).

## Contributing

Zinder is a Rust workspace with domain crates under `crates/` and services under `services/`. Run the [default validation gate](docs/runbooks/testing.md#default-validation-gate) before opening a pull request, and include a release-note fragment or waiver per [.changes/README.md](.changes/README.md).
