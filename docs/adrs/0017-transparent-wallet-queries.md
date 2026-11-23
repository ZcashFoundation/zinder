# ADR-0017: Serve only consumer-owned transparent wallet queries

Status: Accepted

## Context

The native `WalletQuery` protocol once carried transparent methods that no
packaged consumer called, some served only by stubs. Each still cost a
capability, a client method, tests, and documentation, and clients could not
tell a supported method from a placeholder.

Wallets also need to know whether a transparent output is spent and, if so,
by which transaction. Canonical storage keeps block-local facts only and has
no index keyed by spent output. The Explorer has a reverse-spend view, but
that view lives in the optional materialized-view store, which wallet
deployments do not run.

Finally, the wallet projection needs evidence that its unspent set is exactly
the set it claims. A count and a total value do not bind membership: two
different sets can share both.

## Decision

1. **A transparent method exists only when a named consumer calls it.**
   Ownership comes from the consumer's code, not from the protocol or a
   planned feature. The native surface keeps
   `TransparentAddressUnspentOutputs`, `TransparentAddressTxIdsInRange`
   (oldest first only), and `TransparentOutpointSpendStatus`. Adding a method
   requires a consumer call site, a serving reader, and contract tests.
2. **No native balance, mempool-by-address, or UTXO-set summary reads.**
   Wallets sum the unspent outputs they already read to spend. Mempool
   watchers filter `MempoolSnapshot` and `MempoolEvents`, whose entries carry
   transparent outputs and spends. In wallet serving, the lightwalletd
   compatibility runtime still answers `GetTaddressBalance` from the same
   wallet projection rows. Light-wallet serving sums the unspent outputs the
   node reports at the served tip
   ([ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) decision 7).
   Newest-first history and balance totals belong to the Explorer.
3. **Wallet spend status comes from the wallet projection.** Its unspent rows
   decide whether an output is spent, and its spent rows name the spender.
   Both are read from one serving pair at one chain epoch, so a response never
   mixes epochs and never reports a spent output without its spender. One
   request carries 1 to 1,024 outpoints; the Rust client splits longer
   batches at one pinned epoch. `not_found` is returned only when the store
   holds history from height 1. Otherwise a miss fails with
   `CANONICAL_HISTORY_UNAVAILABLE`, the same rule as a transaction lookup
   ([ADR-0018](0018-transaction-facts-status-and-broadcast.md)).
4. **The Explorer keeps its own reverse-spend view.** The
   `transparent_outpoint_spend` view records spender identity for every
   non-coinbase transparent input and serves `TransactionDetail` only. A
   missing row means "no spender recorded", never "unspent". When the view's
   verified coverage does not span from the transaction's height to the
   response tip, the Explorer omits `spent_by` and marks it unavailable
   ([ADR-0019](0019-explorer-plane.md)).
5. **Canonical storage stays unaware of spend views.** It has no
   spend-keyed family, and it never deletes data on the strength of
   materialized-view rows. The spend view is rebuilt from canonical history
   that is complete from height 1.
6. **The wallet projection commits to its unspent set with `LtHash16`.** Each
   unspent output is hashed through BLAKE2X into 1,024 16-bit lanes that are
   summed. The sum does not depend on order, and an output is removed by
   subtraction, so each wallet transition updates the commitment in the same
   write as its rows. Cold validation recomputes it and refuses admission on
   a mismatch. The commitment covers every unspent output, including
   unspendable scripts. A different encoding or membership rule is a new
   scheme value, never a reinterpretation. The commitment is admission
   evidence and is not served on the wire.

Method responses and pinning rules are in
[Wallet data plane](../architecture/wallet-data-plane.md).

## Evidence

- Decision 2, the light-wallet balance: external constraint. The node's
  `getaddressbalance` does not report the tip it answered at, so
  light-wallet serving sums the unspent outputs the node attests at the
  served tip ([#481](https://github.com/ZcashFoundation/zinder/pull/481)).

## Consequences

- A removed read returns only when a consumer needs a shape the kept surface
  cannot serve, such as a multi-address balance or an address filter on the
  mempool feed.
- Wallet deployments need no Explorer view store for spend status.
- A spend-status batch reveals the addresses and exact outputs a wallet
  tracks. It saves round trips over an address scan but does not improve
  privacy.
- A checkpoint-bootstrapped store cannot prove an outpoint absent and cannot
  host the Explorer spend view.
- Every wallet transition pays one BLAKE2X expansion per changed output, and
  cold validation pays one per unspent output.
- The commitment is not comparable with zcashd's `gettxoutsetinfo`, which
  excludes unspendable outputs and hashes the set in a fixed order.

## Alternatives rejected

- **Keep unserved methods as protocol vocabulary.** They still cost a
  registry row, a client method, and documentation, and clients cannot tell
  them from supported methods.
- **Serve wallet spend status from the Explorer view.** Wallet serving would
  then depend on the optional materialized-view store and its replay lag.
- **Add a spend-keyed family to canonical storage.** It would grow canonical
  storage for a reverse lookup that rebuildable views already answer.
- **Expose the UTXO-set summary.** The projection could serve it correctly,
  but no packaged consumer reads it.
- **An XOR accumulator.** XOR is its own inverse, so an element folded twice
  vanishes and crafted collisions are easy.
- **An ordered digest like `hash_serialized`.** It requires a defined
  iteration order that Zinder's storage does not provide.

## Revision history

- 2026-11-23. Decision 2 names the source of `GetTaddressBalance` in
  light-wallet serving.
- 2026-11-23. Evidence records what decision 2 rests on for light-wallet
  serving.
