# ADR-0023: Typed Broadcast Rejection Reasons And A Queued Outcome

| Field | Value |
| ----- | ----- |
| Status | Accepted |
| Product | Zinder |
| Domain | Transaction-broadcast outcome wire contract and submitter classification |
| Related | [ADR-0004](0004-node-source-and-protocol-boundaries.md), [ADR-0007](0007-mempool-topology-and-retention.md), [Public interfaces](../architecture/public-interfaces.md), [Service boundaries](../architecture/service-boundaries.md) |

## Context

`WalletQuery.BroadcastTransaction` collapsed every Zebra mempool reject into
a single `BroadcastRejected { error_code, message }`. Two upstream conditions
that downstream wallets must treat differently shared that one carrier:

- `MempoolError::AlreadyQueued`: Zebra has the transaction in its download
  or verification queue. Re-broadcasting the same byte-identical transaction
  while the prior submission is still in flight produces this state. It is
  benign: the node will eventually accept or reject the queued attempt, and
  the wallet should hold (not retry, not surface as a hard error).
- Every other mempool reject (`BadExpiryHeight`, consensus-branch mismatch,
  signature verification, mempool-full, plus the long tail of verifier
  failures). These need user-visible classification: an expiry-height
  problem suggests rebuilding with a higher target, a consensus-branch
  mismatch suggests refetching the chain tip, a mempool-full state suggests
  exponential backoff, an invalid signature suggests reauthorizing.

Zebra collapses all of these into JSON-RPC `-25 Verify` with `MempoolError`'s
`Display` impl as the message string. Without a typed signal on Zinder's
wire, every downstream consumer that wanted to distinguish them ran a
substring match against the operator-facing string. Fauzec's auto-shield
loop was the canonical example: a `WalletShieldedFundingPlane` shim called
`is_already_queued_reply(failure)` and compared the message against the
hardcoded literal `"already queued for download"`. Each consumer wrote
its own match. Each rewriting was a forward-compatibility hazard the day
upstream Zebra changed wording, and each was a categorical-error magnet
because the substring tests were the only way to tell a benign in-flight
state apart from a hard rejection.

## Decision

The native wallet protocol gains a typed broadcast-rejection vocabulary
and a separate broadcast-queued outcome. The submitter (`zinder-source`)
is the single place that maps Zebra's free-form message strings into
that vocabulary; every other Zinder crate, every Zinder gRPC consumer,
and every typed Rust client match on the typed value instead of the
message.

### Wire contract

`BroadcastTransactionResponse.outcome` carries six variants:

- `accepted = 1`: `BroadcastAccepted { transaction_id }`.
- `duplicate = 2`: `BroadcastDuplicate { error_code, message }`. Zebra
  returns this through JSON-RPC code `-27` (transaction already in
  mempool or main chain).
- `invalid_encoding = 3`: `BroadcastInvalidEncoding { error_code, message }`.
  Zebra returns code `-22` when the submitted bytes do not parse.
- `rejected = 4`: `BroadcastRejected { error_code, message, kind }`.
  `kind` is a `BroadcastRejectionReason` enum: `INVALID_SIGNATURE`,
  `BAD_EXPIRY_HEIGHT`, `BAD_CONSENSUS_BRANCH`, `MEMPOOL_FULL`, `UNKNOWN`.
  The reserved `UNSPECIFIED = 0` exists because proto3 enums require a
  zero default; the server never writes it. A client that decodes an
  unspecified value (older server or future variant) collapses to
  `UNKNOWN`.
- `unknown = 5`: `BroadcastUnknown { error_code, message }`. The
  upstream returned a non-error response that the submitter could not
  classify, or the call failed with no error code at all.
- `queued = 6`: `BroadcastQueued { message }`. Zebra accepted the
  transaction into its download or verification queue but has not yet
  produced a final verdict.

`Queued` is a distinct top-level variant, not a `kind` inside
`Rejected`, because it is not a rejection. Treating it as a rejection
forces every consumer to introspect the typed reason to recover the
"actually retry-safe" answer. Promoting it to the outcome oneof keeps
the type-level match exhaustive.

### Submitter classification

`zinder-source` is the only crate allowed to inspect Zebra's free-form
error strings (per ADR-0004). `ZebraJsonRpcSource::broadcast_transaction`
delegates classification to two pure functions:

- `is_already_queued_message(message: &str) -> bool` recognises Zebra's
  `MempoolError::AlreadyQueued` by the distinctive lowercase substring
  `queued for download`. The check is case-insensitive so Zebra wording
  changes that re-case the prefix keep matching.
- `classify_rejection_reason(message: &str) -> BroadcastRejectionReason`
  maps the message to one of the typed reasons by lowercase substring
  matches against canonical Zebra wording: `mempool` + `full`,
  `consensus branch` / `branch id`, `expiry` / `expired`, and
  `invalid signature` / `bad signature` / `signature is invalid`.
  Anything that fails to match falls through to
  `BroadcastRejectionReason::Unknown`.

`-22` and `-27` are wire-level codes (not message-string-derived), so
`InvalidEncoding` and `Duplicate` outcomes bypass the message-text
classifier entirely. The classifier only fires inside the `_` arm of
the JSON-RPC error code dispatch, which is where every mempool reject
ends up because Zebra emits them all under `-25 Verify`.

### Type-level surfaces

`zinder-core` exports the value types every downstream consumer matches
on:

- `enum TransactionBroadcastOutcome` gains a `Queued(BroadcastQueued)`
  variant.
- `BroadcastRejected` gains a `kind: BroadcastRejectionReason` field.
- `BroadcastQueued { message: String }` is a new struct.
- `BroadcastRejectionReason` is a new `#[non_exhaustive]` enum with
  `Default = Unknown` plus the four typed reasons.

`zinder-client` re-exports the new types so `RemoteChainIndex` callers use the
same vocabulary as the native protocol.

### Lightwalletd compatibility surface

`compat-lightwalletd::SendResponse` has no queued concept. The compat
shim maps `Queued` to legacy code `-25` (Zebra's underlying code for
this path) and forwards the message verbatim. Legacy wallets see the
same code zcashd would have returned for a transaction the local node
already has queued; nothing about the legacy-wire shape changes.

## Consequences

### What this enables

- Downstream consumers (Zodl, fauzec, third-party wallets) dispatch on
  a typed value instead of substring-checking a string they do not own.
- Future Zebra wording changes (case, prefixed sentences, structured
  causes) do not break Zinder consumers: classification lives in one
  place, and the classifier is the only file that needs an update.
- Auto-shield retry loops can hold on `Queued` and back off on
  `MempoolFull` without conflating the two.
- Metrics labels can use the typed reason directly; histograms by
  `broadcast_rejection_kind` no longer need an external mapping table.

### What this costs

- Adding a new mempool error variant to Zebra requires either an
  explicit substring rule in `classify_rejection_reason` (the variant
  earns a typed reason) or accepting the `Unknown` fallthrough (the
  message is still surfaced verbatim, just without typed dispatch).
- The classifier is case-insensitive substring matching, not a parsed
  Zebra error enum. It cannot distinguish two mempool errors whose
  `Display` impls happen to share a discriminating keyword. The known
  Zebra variants do not currently collide; if a future variant does,
  the fix is to widen the substring or add an exact prefix check.
- Older servers that never write `kind` are indistinguishable from
  servers that explicitly report `Unknown`. Clients collapse both to
  the same `Unknown` value. This is intentional: callers that want
  "definitely unknown" introspect the `message` field anyway.

### Out of scope for this ADR

- Persisting broadcast outcomes in the canonical store. Broadcast
  results are response values, not durable artifacts.
- Returning the txid alongside `Queued`. Zebra's `sendrawtransaction`
  path returns the txid only on accepted submissions; computing it
  client-side at the submitter would require a consensus-aware hash
  and is not needed by the in-flight handling pattern.
- Wider rejection-reason coverage (low-fee, dust thresholds,
  conflicting outputs, ZIP-401 anti-spam). The five reasons above are
  the ones downstream code routinely needs to distinguish today. New
  variants land when a real consumer needs them.

## Vocabulary

- `TransactionBroadcastOutcome::Queued` (the top-level variant).
- `BroadcastQueued { message }` (the carrier struct).
- `BroadcastRejected { kind, error_code, message }` (now typed).
- `BroadcastRejectionReason` (`InvalidSignature`, `BadExpiryHeight`,
  `BadConsensusBranch`, `MempoolFull`, `Unknown`).
- `wallet::BroadcastRejectionReason` (the proto enum, with
  `UNSPECIFIED = 0` reserved by proto3 conventions).
- `classify_broadcast_error` (the submitter entry point).
- `is_already_queued_message` (the queued-state detector).
- `classify_rejection_reason` (the typed-reason classifier).

See [Public interfaces §Core vocabulary](../architecture/public-interfaces.md#core-vocabulary).
