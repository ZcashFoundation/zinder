# Zinder

> [!WARNING]
> **Zinder is alpha.** APIs, storage layouts, and deployment topology will
> change, and a tagged release is not a stability promise. Pin exact versions
> and expect breaking upgrades.

Zinder is a self-hosted Zcash indexer. It reads the chain from a
[Zebra](https://github.com/ZcashFoundation/zebra) or
[Zakura](https://github.com/zakura-core/zakura) node, stores an index once, and
serves that one consistent view to every wallet, backend, and explorer you run.

## Why Zinder

- **One index, several protocols.** Existing wallets connect through the
  [lightwalletd](https://github.com/zcash/lightwalletd) `CompactTxStreamer`
  protocol. New clients use the native `WalletQuery` gRPC API or the
  `zinder-client` Rust crate. An optional Explorer serves block, transaction,
  address, and aggregate views. All of them read the same stored data.
- **Reads that survive reorgs.** Every response comes from one snapshot of the
  chain, and a native client can pin several calls to the same snapshot. A
  sync never mixes data from two competing tips.
- **Events you can resume.** A backend follows the chain through a cursor that
  survives disconnects and reports reorgs explicitly.
- **Serving through node outages.** Stored blocks, tree states, and transparent
  address data stay available while the node is down. Ingest and broadcast can
  fail over to a second node.

Zinder serves public chain data. Keys, note scanning, and shielded balances
stay in the wallet.

Zinder runs beside an archive Zebra or Zakura node on one host.
[Deploying Zinder](docs/runbooks/deploying.md) lists what it needs. The
[indexer comparison](docs/reference/indexer-comparison.md) puts Zinder beside
Zebra, Zakura, ztreamer, Zingod, and Go lightwalletd, feature by feature.

## Start here

| You want to | Read |
| --- | --- |
| See what Zinder gives your product | [Use cases](docs/use-cases.md) |
| Run Zinder | [Deploying Zinder](docs/runbooks/deploying.md), which starts with a Docker Compose quickstart |
| Connect a wallet or backend | [Integration surfaces](docs/reference/integration-surfaces.md) |
| Understand how it works | [How Zinder works](docs/architecture/overview.md) |
| Contribute | [Testing runbook](docs/runbooks/testing.md) and [CLAUDE.md](CLAUDE.md) |

The [documentation index](docs/README.md) lists everything else.

## Contributing

Zinder is a Rust workspace: domain crates under `crates/` and deployable
services under `services/`. Run the validation gate in the
[testing runbook](docs/runbooks/testing.md#default-validation-gate) before you
open a pull request. Every pull request needs a release note or the waiver
described in [.changes/README.md](.changes/README.md).
