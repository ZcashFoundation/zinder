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
   unspent output's commitment element is expanded into 1,024 16-bit lanes
   that are summed modulo 2^16. The sum does not depend on order, and an
   output is removed by subtraction, so each wallet transition updates the
   commitment in the same write as its rows. Cold validation recomputes it
   and refuses admission on a mismatch. The commitment covers every unspent
   output, including unspendable scripts. It is admission evidence and is not
   served on the wire.

   Schema v1 uses `LtHash16ChaCha12` as commitment scheme 1. The root is an unkeyed 44-byte BLAKE2b hash of
   the element with the personalization `ZinderUtxoRoot_1`. Its first 32
   bytes are the key and its last 12 bytes the nonce of ChaCha12 in the
   RFC 8439 state layout. The 32 keystream blocks at counters 0 through 31
   are the lanes. The wallet row digest uses the same expansion with its own
   personalization and row encoding, as
   [Storage backend](../architecture/storage-backend.md#wallet-store)
   describes. The two accumulators stay independent: each hashes its own
   encoding, and cold validation recomputes each from the stored rows. A
   different encoding, expansion, or membership rule is a new scheme value,
   never a reinterpretation.

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
- Every wallet transition pays one expansion per changed row and one per
  changed output, and cold validation pays one per stored row and one per
  unspent output.
- The security of both accumulators rests on a heuristic. Zinder treats
  ChaCha12 keyed by a public BLAKE2b root as a random function of the
  element. The project's security review argues that if both the BLAKE2b
  root and the ChaCha permutation were ideal, this hash-then-expand
  construction would behave like a random oracle up to the root's birthday
  bound. That is an informal argument, not a published result, and nothing
  proves a 128-bit guarantee for the concrete BLAKE2b and 12-round ChaCha.
- LtHash16 is weaker than first estimated. Lewi, Kim, Maykov, and Weis
  estimated at least 200 bits of collision security for 1,024 16-bit lanes
  ([ePrint 2019/227](https://eprint.iacr.org/2019/227)). Ding, Gong, Jiang,
  and Tang give a collision attack on unrestricted sets with these
  parameters that needs about 2^81 element hashes
  ([ePrint 2026/2083](https://eprint.iacr.org/2026/2083)). Its colliding sets
  hold at least 550^8, about 2^72.8, distinct elements.
- Zinder's stores rule out that attack's witness. Row and UTXO counts are
  checked 64-bit values, so two READY states hold fewer than 2^68 rows
  together, and keys are unique, so no element repeats. Smaller witnesses
  are not ruled out, and no published analysis gives a work factor for sets
  of this size.
- The hashes Zinder compares have idealized birthday ceilings, not proven
  security levels: 2^128 for the 32-byte display digests and manifest
  hashes, and 2^176 for two elements with equal 44-byte roots.
- Changing the lane count or width changes the problem and needs its own
  analysis.
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
- **BLAKE2X expansion, scheme 1.** A short element costs 33 BLAKE2b
  compressions, one for the root and 32 for the output blocks. It costs
  several times as much as the ChaCha12 expansion on every CPU family
  measured, including CPUs without AVX-512 and Arm.
- **BLAKE3 output expansion.** Its output is fast only with AVX-512. On a
  Neoverse V2 Arm CPU it took 2,765 ns per element against 5,229 ns for
  BLAKE2X and 1,190 ns for ChaCha12, and on AVX2-only CPUs it gained little
  or lost.
- **ChaCha8 or ChaCha20.** ChaCha8 leaves half a round of margin over the
  published 7.5-round attacks on the cipher, in a setting where the key is
  public. Eight rounds are already reached by inside-out distinguishers on
  the ChaCha permutation (Neves and Araújo, Information Processing Letters
  149, 2019) and by a related-key attack on ChaCha8
  ([ePrint 2025/2334](https://eprint.iacr.org/2025/2334)). ChaCha20 costs
  about 45% more per element than ChaCha12, and more rounds do not raise the
  LtHash16 bound above.
- **One expansion per unspent output for both accumulators, or a commitment
  taken from the row digest.** The durable row and the commitment element
  encode different fields. Sharing one expansion either hides row-only
  fields from the row digest or makes the commitment depend on the row
  layout, and a bad shared contribution would pass both checks.

## Revision history

- 2026-11-24. Decision 6 assigns scheme 1 to BLAKE2b and ChaCha12 for the schema v1 cutover.

- 2026-11-23. Decision 2 names the source of `GetTaddressBalance` in
  light-wallet serving.
- 2026-11-23. Evidence records what decision 2 rests on for light-wallet
  serving.
- 2026-11-23. Decision 6 expands elements with a BLAKE2b root and ChaCha12 as scheme 2, replacing BLAKE2X, and records the LtHash16 security limits.
