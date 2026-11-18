# Use cases

This page shows what Zinder gives each kind of consumer and what you enable for
it. For a feature-by-feature table beside Zebra, Zakura, ztreamer, Zingod, and
Go lightwalletd, see the [indexer comparison](reference/indexer-comparison.md).

In every case, Zinder serves public chain data from the node you configure. The
node validates consensus, and Zinder trusts what it reports. Keys, note
scanning, transaction building, and signing stay in your application.

## At a glance

| Consumer | Zinder surface | Enabled by default | Zinder adds |
| --- | --- | --- | --- |
| Light wallet that speaks lightwalletd | `CompactTxStreamer` on `zinder-compat-lightwalletd` | Yes | Serving from stored data during a node outage |
| Wallet or wallet backend built for Zinder | `WalletQuery` on `zinder-query`, or the `zinder-client` crate | Yes | Reads pinned to one chain snapshot across calls |
| Payment or deposit tracking | `ChainEvents` and transparent address queries on `zinder-query` | Yes | A resumable event cursor that reports reorgs |
| Block explorer | `ExplorerQuery` on `zinder-explorer` | No | Search and aggregate views over the same index |
| Faucet or other transaction sender | `BroadcastTransaction` and `Transaction` on `zinder-query` | Yes | Typed broadcast outcomes and failover between nodes |
| Several of these at once | All of the above from one deployment | Explorer is optional | One index instead of one per product |

## Light wallets that speak lightwalletd

Point the wallet's existing `CompactTxStreamer` client at
`zinder-compat-lightwalletd`. No client change is needed. Zinder serves compact
blocks, tree states, and transparent address data from its own store, so
wallets keep syncing while the node is down. `Ping` is off by default, and
`GetAddressUtxos` and address history apply Zinder-specific limits. The
[lightwalletd compatibility reference](reference/lightwalletd-compatibility.md)
lists each method.

Zinder supports a named wallet release once that release passes end-to-end
certification.

## Wallets and backends built for Zinder

Use the native `WalletQuery` gRPC API, or the `zinder-client` crate in Rust.
The native API adds what the lightwalletd protocol cannot express:

- **Pinned reads.** A client pins several calls to the same chain snapshot. A
  sync that reads blocks, a tree state, and transparent outputs never combines
  two competing tips.
- **Typed errors.** Most failures carry a stable reason, and each reason maps
  to a documented retry rule. A client knows whether to retry, restart at a
  newer snapshot, or stop. A few transport-level rejections, such as listener
  overload, carry a gRPC status code without a reason.
- **Capability discovery.** `ServerInfo` lists exactly what a deployment can
  serve, so a client checks before it relies on a method.

A server-side wallet pairs `zinder-client` with a wallet library such as
librustzcash. [Integration surfaces](reference/integration-surfaces.md) shows
the smallest working call for each path.

## Payment and deposit tracking

A payment backend needs to learn about new blocks and reorgs without missing
one, and to check transparent outputs and their spend status. Zinder provides:

- **`ChainEvents`.** A stream of committed blocks and reorgs. The client stores
  the cursor from each event and resumes from it after a disconnect. Events are
  kept for seven days by default; a client that stays away longer rebuilds its
  state from block and address queries.
- **Transparent address queries.** Unspent outputs, transaction ids in a height
  range, and the spend status of up to 1,024 outpoints per call.
- **Mempool snapshots and events** for unconfirmed payments.

Your application detects shielded payments by scanning compact blocks with its
own viewing keys, which Zinder never sees.
[Chain events](architecture/chain-events.md) and the
[wallet data plane](architecture/wallet-data-plane.md) describe these contracts.

## Block explorers

The optional `zinder-explorer` runtime serves block and transaction detail,
typed search, mempool views, transparent address activity and ranking, fee
summaries, and value-pool flows. It answers a shielded address with a typed
refusal and never scans shielded data.

The Explorer is off by default. Turning it on makes ingest build a second,
rebuildable store of explorer views, which needs its own disk and build time.
The [Explorer plane](architecture/explorer-plane.md) explains how to enable it,
what it needs, and what each method returns.

## Faucets and transaction senders

A faucet builds and signs transactions itself, then needs to send them and
watch them confirm. `BroadcastTransaction` returns a typed outcome, such as
accepted, duplicate, or rejected with a reason, and moves to the next
configured node when the first is unreachable. `Transaction` reports whether a
transaction is mined, in the mempool, or proven absent at one snapshot.

Block templates and mined-block submission stay with the node. A miner or a
mining faucet uses the node's RPC for them.

## Several consumers from one deployment

The case Zinder is built for is one operator serving several products: a
lightwalletd wallet, a native backend, and an explorer read the same index
through separate processes. Each product sees the same chain at the same
snapshot, and you operate one index instead of one per product.

To run it, start with [Deploying Zinder](runbooks/deploying.md). It lists what
a deployment needs and what to put in front of the listeners before you expose
them.
