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
  signature verification, mempool-full, NU7 version and action-limit
  rejections, plus the long tail of verifier failures). These need
  user-visible classification: an expiry-height problem suggests rebuilding
  with a higher target, a consensus-branch mismatch suggests refetching the
  chain tip, a mempool-full state suggests exponential backoff, an invalid
  signature suggests reauthorizing, an unsupported transaction version
  suggests rebuilding for the current network upgrade, and a transaction that
  exceeds a per-block action limit suggests splitting the payment.

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
  `kind` is a `BroadcastRejectionReason` enum: `INVALID_SIGNATURE = 1`,
  `BAD_EXPIRY_HEIGHT = 2`, `BAD_CONSENSUS_BRANCH = 3`, `MEMPOOL_FULL = 4`,
  `UNKNOWN = 5`, `UNSUPPORTED_TRANSACTION_VERSION = 6`, and
  `EXCEEDS_BLOCK_ACTION_LIMIT = 7`.
  The reserved `UNSPECIFIED = 0` exists because proto3 enums require a
  zero default; the server never writes it. A client that decodes an
  unspecified or unrecognized value (older server or future variant)
  collapses to `UNKNOWN`. This holds for `zinder-client` from the release
  that adds values 6 and 7; see Consequences for older clients.
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
  The two NU7 reasons are matched first because their messages are the most
  specific: `not supported by the network upgrade` yields
  `UnsupportedTransactionVersion` (for example ZIP 2003 makes v4
  transactions unsupported from NU7), and `zip 218 limit` yields `ExceedsBlockActionLimit`
  (Zebra rejects, at mempool admission, a transaction that alone exceeds a
  ZIP 218 per-block action limit). Zakura words the action-limit rejection as
  `exceeds the per-block limit` and `exceeds the per-block global shielded
  budget`, which are matched too. Anything that fails to match falls through
  to `BroadcastRejectionReason::Unknown`.

`-22` and `-27` are wire-level codes (not message-string-derived), so
`InvalidEncoding` and `Duplicate` outcomes bypass the message-text
classifier entirely. The classifier only fires inside the `_` arm of
the JSON-RPC error code dispatch, which is where every mempool reject
ends up because Zebra emits them all under `-25 Verify`.

### Reason handling

A rejected broadcast is a successful `BroadcastTransaction` response, not a
gRPC failure, so no `ErrorReason` or gRPC status code applies to it and
`error_policy` stays unchanged. The reason is a payload verdict and the
typed `kind` is the only retry signal. Every reason has one wire value, one
`zinder-core` value, and one caller disposition:

| Reason | Wire value | Caller disposition |
| ------ | ---------- | ------------------ |
| `InvalidSignature` | 1 | Do not retry the same bytes. Reauthorize and rebuild. |
| `BadExpiryHeight` | 2 | Do not retry the same bytes. Rebuild with a higher expiry height. |
| `BadConsensusBranch` | 3 | Do not retry the same bytes. Refetch the chain tip, then rebuild. |
| `MempoolFull` | 4 | Retry the same bytes with exponential backoff. |
| `Unknown` | 5 | Surface the node message. Do not treat the absence of a typed reason as authoritative. |
| `UnsupportedTransactionVersion` | 6 | Do not retry the same bytes. Rebuild for the current network upgrade. |
| `ExceedsBlockActionLimit` | 7 | Do not retry the same bytes. Split the payment across transactions. |

Only `MempoolFull` is retryable. The mappings in `zinder-query`
(`broadcast_rejection_reason_to_message`) and `zinder-client`
(`broadcast_rejection_reason_from_message`) map every row symmetrically, and
the client maps unspecified and unrecognized values to `Unknown`.

### Type-level surfaces

`zinder-core` exports the value types every downstream consumer matches
on:

- `enum TransactionBroadcastOutcome` gains a `Queued(BroadcastQueued)`
  variant.
- `BroadcastRejected` gains a `kind: BroadcastRejectionReason` field.
- `BroadcastQueued { message: String }` is a new struct.
- `BroadcastRejectionReason` is a new `#[non_exhaustive]` enum with
  `Default = Unknown` plus the six typed reasons.

`zinder-client` re-exports the new types so `RemoteChainIndex` callers use the
same vocabulary as the native protocol.

### Lightwalletd compatibility surface

`compat-lightwalletd::SendResponse` has no queued concept. The compat
shim maps `Queued` to legacy code `-25` (Zebra's underlying code for
this path) and forwards the message verbatim. Legacy wallets see the
same code zcashd would have returned for a transaction the local node
already has queued; nothing about the legacy-wire shape changes.

`SendResponse` carries only `error_code` and `error_message`, so the typed
reason has no legacy field. Every `Rejected` outcome, whatever its `kind`,
maps to the node-reported code (default `-26`) with the node message
forwarded verbatim. Legacy wallets therefore see Zebra's own wording for the
NU7 version and action-limit rejections. The shim does not invent a
Zinder-specific code table.

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
  "definitely unknown" introspect the `message` field anyway. An older
  server therefore reports an NU7 v4 or action-limit rejection as
  `Unknown`, which a caller must never read as authoritative.
- New reasons are additive enum values on `wallet.broadcast.transaction_v1`
  and do not add a capability string or raise `contract_revision`. Servers
  that predate a reason report it as `Unknown`, which callers must not read
  as authoritative.
- Clients built before the release that adds values 6 and 7 do not degrade.
  Every earlier `zinder-client` release (v0.5.0-rc.1 through v0.9.0)
  rejects discriminants outside 1 to 5 with
  `IndexerError::malformed("rejected.kind", ...)`, so a rejected broadcast
  surfaces as `MalformedResponse` instead of a `Rejected` outcome. Release
  the client and update consumer pins before or together with the server.
  A request-side opt-in field would avoid the break, because the server
  could keep writing `Unknown` to clients that do not send it. A capability
  string or `contract_revision` cannot, because both flow from server to
  client only. This ADR accepts the break instead of adding negotiation for
  two additive values.
  The new client decodes unspecified and unrecognized values as `Unknown`, so
  later additions do not repeat this break.
- The Cipherscan adapter emits `rejected.kind` as a raw integer
  (`"reason"`), so Cipherscan REST clients now see `6` and `7`.

### Out of scope for this ADR

- Persisting broadcast outcomes in the canonical store. Broadcast
  results are response values, not durable artifacts.
- Returning the txid alongside `Queued`. Zebra's `sendrawtransaction`
  path returns the txid only on accepted submissions; computing it
  client-side at the submitter would require a consensus-aware hash
  and is not needed by the in-flight handling pattern.
- Wider rejection-reason coverage (low-fee, dust thresholds,
  conflicting outputs, ZIP-401 anti-spam). The seven reasons above are
  the ones downstream code routinely needs to distinguish today. New
  variants land when a real consumer needs them.

## Vocabulary

- `TransactionBroadcastOutcome::Queued` (the top-level variant).
- `BroadcastQueued { message }` (the carrier struct).
- `BroadcastRejected { kind, error_code, message }` (now typed).
- `BroadcastRejectionReason` (`InvalidSignature`, `BadExpiryHeight`,
  `BadConsensusBranch`, `MempoolFull`, `UnsupportedTransactionVersion`,
  `ExceedsBlockActionLimit`, `Unknown`).
- `wallet::BroadcastRejectionReason` (the proto enum, with
  `UNSPECIFIED = 0` reserved by proto3 conventions).
- `classify_broadcast_error` (the submitter entry point).
- `is_already_queued_message` (the queued-state detector).
- `classify_rejection_reason` (the typed-reason classifier).

See [Public interfaces §Core vocabulary](../architecture/public-interfaces.md#core-vocabulary).

## Revision history

- 2026-11-12: Added `UnsupportedTransactionVersion` and
  `ExceedsBlockActionLimit` (wire values 6 and 7) for the NU7 rejections that
  Zebra 7.0.0 and Zakura emit. `UnsupportedTransactionVersion` covers every
  `not supported by the network upgrade` rejection, not only ZIP 2003 v4
  rejections after NU7. Documented that the lightwalletd shim forwards every
  rejected reason through the node code and message, that clients decode
  unrecognized reason values as `Unknown` from this release on, and that
  earlier clients fail on values 6 and 7.
