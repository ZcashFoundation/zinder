# ADR-0026: Transparent UTXO-Set Commitment

| Field | Value |
| ----- | ----- |
| Status | Accepted encoding for wallet-projection evidence; wire surface removed by ADR-0043 |
| Product | Zinder |
| Domain | Wire format, UTXO-set accounting, public proto contract, capability discovery |
| Related | [ADR-0003](0003-canonical-storage-access-boundary.md), [ADR-0011](0011-explorer-freshness-envelope.md), [ADR-0018](0018-capability-gated-optional-payload-fields.md), [ADR-0024](0024-wire-format-rpc-byte-order.md), [ADR-0029](0029-durable-transparent-outpoint-spend-projection.md), [ADR-0038](0038-wallet-runtime-composition-and-capability-discovery.md), [ADR-0043](0043-owned-transparent-wallet-query-surface.md), [Public interfaces §Wire Conventions](../architecture/public-interfaces.md#wire-conventions) |

## Context

The wallet projection keeps a chain-wide transparent unspent-set summary, a count and a total value, as construction and admission evidence. Two deployments at the same settled tip should be able to prove they hold the same set, and a third party should be able to recompute the same value from a plain UTXO dump. A plain count and sum do not bind set membership: two different sets can share a count and a total.

`gettxoutsetinfo` answers this with `hash_serialized`, a digest over the serialized set in a defined iteration order. Zinder does not define a UTXO-set serialization ordering, so that digest is not reproducible here. A commitment that does not depend on iteration order removes the ordering requirement.

A naive XOR accumulator is order-independent but cryptographically broken for snapshot verification: XOR is its own inverse, so any element folded an even number of times vanishes, and an adversary can craft set differences that collide. A homomorphic lattice hash (LtHash) keeps order-independence and invertibility while resisting those attacks.

## Decision

### Scheme: LtHash16 over canonical BLAKE2X

Each transparent output is encoded into a fixed preimage, expanded through a canonical BLAKE2X XOF to 2048 bytes, and read as 1024 little-endian `u16` lanes. The accumulator sums lanes componentwise modulo `2^16`. The 2048-byte accumulator is the commitment; a 32-byte display digest is `BLAKE2b-256` of the accumulator.

Summation is commutative and invertible, so the accumulator is independent of fold order and an element is removed by lane-wise subtraction modulo `2^16`. Two deployments at the same settled tip produce byte-identical accumulators.

The XOF is the official BLAKE2X construction (<https://www.blake2.net/blake2x.pdf>), not an ad-hoc counter feeding repeated `BLAKE2b`. A root `BLAKE2b-512` hash over the preimage carries the total output length in the high 32 bits of the parameter block's `node_offset` word; each 64-byte output block is a `BLAKE2b` hash over the root whose parameter block sets `node_offset` to the block index, the XOF length in the high 32 bits, `leaf_length` and `inner_length` to 64, and `fanout`/`depth` to 0. The construction is pinned by a known-answer test against the official BLAKE2Xb test vectors and cross-checked against the Go `golang.org/x/crypto/blake2b` XOF.

### Element encoding (snapshot-immutable)

The per-UTXO preimage is fixed-width little-endian:

```text
network_id(u32 LE) ‖ encoding_version(u8) ‖ txid(32, internal TransactionId byte order)
  ‖ output_index(u32 LE) ‖ value_zat(u64 LE)
  ‖ script_len(u32 LE) ‖ raw_scriptPubKey ‖ block_height(u32 LE)
```

`txid` is the internal `TransactionId` byte order (the bytes the type stores), not the RPC byte-reversed form of [ADR-0024]; the commitment is an internal accounting value, never a human-facing hash.

`network_id` and `encoding_version` live in the preimage, not in the BLAKE2 personalization. `BLAKE2b`'s personal field is 16 bytes and the fixed domain tag `b"ZinderUtxoSet___"` fills all 16. Keeping network and version in the preimage lets a third party reproduce the bytes from a plain UTXO dump without reconstructing BLAKE2 salt or personalization plumbing. The 16-byte tag is the BLAKE2X personalization and domain-separates this XOF from every other BLAKE2 use in the codebase.

The commitment binds the raw `scriptPubKey` length-prefixed, not the SHA-256 projection key the address index stores. A new `encoding_version` is a new snapshot scheme, never a reinterpretation of this one.

### Full-set membership

The commitment folds every output the wallet projection's unspent-output rows hold at its source position, including `OP_RETURN` and non-standard scripts, matching `utxo_count` and `total_value_zat` exactly. It applies no `IsUnspendable` filter. A zcashd-comparable membership rule would be a future `UtxoSetCommitmentScheme` value, never a reinterpretation of `LtHash16`.

### Self-describing scheme and comparison rules

The persisted evidence records the scheme as the `UtxoSetCommitmentScheme` enum, not a string. Comparison is governed by the scheme:

- Schemes differ, or either side is absent: not comparable. This is not divergence.
- Same scheme, same chain epoch, different accumulator bytes: genuine divergence.

### Maintenance: wallet-projection accumulator

Canonical storage holds no UTXO rows and computes no aggregate. The wallet RocksDB projection maintains the summary, `utxo_count`, `total_value_zat`, and the `LtHash16` accumulator, as part of its ready evidence. Each wallet transition inserts every created unspent output and subtracts every spent or reverted one in the same atomic write that changes the unspent-output rows, so the delta sites are the projection's own row writes. Only the wallet primary writes the accumulator; read-only secondaries never do ([ADR-0003]). Cold validation recomputes the summary by scanning every unspent-output row and refuses admission when the count, total, or accumulator differs from the persisted evidence.

### Capability gating

The fold has real per-output CPU cost. The original implementation exposed an
operator boolean that independently enabled a wire commitment field; no
production composition or named consumer owned that switch, and ADR-0038
removed the boolean. No packaged consumer reads the summary, so
`WalletQuery.TransparentUtxoSetSummary`, `ExplorerQuery.UtxoSetSummary`, their
capabilities, and the commitment wire messages are not part of the protocol
([ADR-0043](0043-owned-transparent-wallet-query-surface.md)). Exposing the
summary again requires a concrete consumer, authenticated admission evidence
from the wallet projection, and an enforced resource budget.

## Consequences

Cold validation can prove that a wallet projection holds exactly the transparent unspent set its evidence claims, and the commitment can be recomputed from a UTXO dump without Zinder-specific plumbing. The cost is one BLAKE2X expansion per output a wallet transition creates, spends, or reverts, plus one per unspent output during cold validation.

The 2048-byte accumulator (not the 32-byte digest) is the canonical commitment, so two accumulators can be summed or differenced to reason about set deltas. A future zcashd-comparable membership rule or a wider lane width lands as a new scheme value, leaving `LtHash16` stable.

The committed unspent set is also the spentness authority in the wallet plane's authority split ([ADR-0029]): the wallet projection's unspent rows decide spent-versus-unspent, while the durable spend projection resolves only spender identity.

## Revision history

- 2026-11-12: The summary and commitment are wallet-projection evidence only.
  `WalletQuery.TransparentUtxoSetSummary`, `ExplorerQuery.UtxoSetSummary`,
  their capabilities, the commitment wire messages, and
  `TransparentUtxoSetSummaryView::comparable_with` were removed because no
  packaged consumer read them
  ([ADR-0043](0043-owned-transparent-wallet-query-surface.md)).
