# ADR-0026: Transparent UTXO-Set Commitment

| Field | Value |
| ----- | ----- |
| Status | Accepted encoding; runtime admission withdrawn by ADR-0038 |
| Product | Zinder |
| Domain | Wire format, UTXO-set accounting, public proto contract, capability discovery |
| Related | [ADR-0003](0003-canonical-storage-access-boundary.md), [ADR-0011](0011-explorer-freshness-envelope.md), [ADR-0018](0018-capability-gated-optional-payload-fields.md), [ADR-0024](0024-wire-format-rpc-byte-order.md), [ADR-0029](0029-durable-transparent-outpoint-spend-projection.md), [ADR-0038](0038-wallet-runtime-composition-and-capability-discovery.md), [Public interfaces §Wire Conventions](../architecture/public-interfaces.md#wire-conventions) |

## Context

`WalletQuery.TransparentUtxoSetSummary` reports the chain-wide transparent unspent set as a count and a total value. Two deployments at the same settled tip should be able to prove they hold the same set, and a third party should be able to recompute the same value from a plain UTXO dump. A plain count and sum do not bind set membership: two different sets can share a count and a total.

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

The commitment folds every output the current-UTXO projection holds at the settled tip, including `OP_RETURN` and non-standard scripts, matching `utxo_count` and `total_value_zat` exactly. It applies no `IsUnspendable` filter. A zcashd-comparable membership rule would be a future `UtxoSetCommitmentScheme` value, never a reinterpretation of `LtHash16`.

### Self-describing scheme and comparison rules

The wire message carries the scheme as an enum (`UtxoSetCommitmentScheme { UNSPECIFIED = 0; LTHASH16 = 1; }`), not a string, so it avoids the duplicate-wire-string anti-pattern. Comparison is governed by the scheme:

- Schemes differ, or either side is absent: not comparable. This is not divergence.
- Same scheme, same chain epoch, different accumulator bytes: genuine divergence.

The comparison rule lives in the client library (`TransparentUtxoSetSummaryView::comparable_with`), so consumers cannot accidentally compare across schemes.

### Maintenance: request-time fold, no schema change

The accumulator is folded inside the existing `read_transparent_utxo_set_aggregate` scan, beside the two `u64` totals, one element per surviving settled-tip row. There is no persistent accumulator, no new column family, and no `STORE_SCHEMA_VERSION`/`MATERIALIZED_VIEW_STORE_FORMAT_VERSION` bump. A persistent incremental accumulator was rejected: it would require writes from the read-only secondary (forbidden by [ADR-0003]) and would add trust-sensitive delta sites at every spend and reorg.

### Capability gating

The fold has real per-output CPU cost. The original implementation exposed an
operator boolean that independently enabled
`wallet.read.transparent_utxo_set_commitment_v1`; no current production
composition or named consumer owned that switch. ADR-0038 removes the boolean.
The serving-pair query does not implement the summary. Removal of the unowned
protocol surface is tracked in
[issue #63](https://github.com/gustavovalverde/zinder/issues/63). Retaining or
reintroducing the commitment requires a concrete production consumer,
authenticated admission evidence, and an enforced resource budget; a manual
support flag is not valid evidence.

## Consequences

A consumer can verify two Zinder deployments hold the same transparent unspent set at one settled tip by comparing 32-byte display digests, and can recompute the commitment from a UTXO dump without Zinder-specific plumbing. The cost is one BLAKE2X expansion per unspent output per summary call on deployments that opt in; deployments that do not pay nothing and omit the field.

The 2048-byte accumulator (not the 32-byte digest) is the canonical commitment carried on the wire, so two accumulators can be summed or differenced off-Zinder to reason about set deltas. A future zcashd-comparable membership rule or a wider lane width lands as a new scheme value, leaving `LtHash16` stable.

The committed unspent set is also the spentness authority in the wallet plane's authority split ([ADR-0029]): absence from `TransparentUnspentOutputsByOutpoint` decides spent-versus-unspent, while the durable spend projection resolves only spender identity.
