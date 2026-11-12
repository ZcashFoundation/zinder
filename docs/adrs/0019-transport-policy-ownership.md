# ADR-0019: Transport policy ownership and self-healing

Status: Accepted
Date: 2026-05-19
Related: [ADR-0004](0004-node-source-and-protocol-boundaries.md),
[ADR-0006](0006-ingest-control-transport-security.md),
[ADR-0013](0013-source-failure-recovery-topology.md)

## Context

Long-lived HTTP/gRPC clients in Zinder accumulated a class of bug where
the underlying connection silently died (peer restart, network sleep,
NAT timeout) while the client's type-level API kept reporting healthy.
Subsequent `.await` calls then hung indefinitely, never surfacing as
errors the caller's reconnect loop could observe.

Two production incidents on testnet traced to this shape:

1. **Intra-Zinder gRPC channels wedged after `zinder-query` restarted.**
   The explorer's `chain_events` subscriber's `wallet_client.chain_events(...).await`
   hung for hours after a single h2 protocol error, even though the
   2-second reconnect loop ran. Fixed (commit `9537461`) by adding
   `http2_keep_alive_interval(30 s)` +
   `keep_alive_while_idle(true)` + `keep_alive_timeout(20 s)` +
   `tcp_keepalive(60 s)` on `connect_authenticated_channel`, plus a
   per-subscribe `tokio::time::timeout(15 s)`.

2. **`zinder-ingest`'s jsonrpsee HTTP/1.1 client to Zebra emitted 8,682
   consecutive `node_unavailable` errors over 12 hours** despite Zebra
   being healthy and reachable from inside the container. Restarting
   the container immediately unstuck it. jsonrpsee 0.26 hides hyper's
   pool config; the keep-alive recipe that worked for tonic does not
   transfer.

Research surfaced three constraints that ruled out the naive
"copy-paste the tonic keep-alive config everywhere" answer:

- **tonic issue #258 (`ENHANCE_YOUR_CALM`):** sending HTTP/2 pings while
  idle against a gRPC server that does not permit
  keep-alive-without-calls triggers a `GOAWAY` and terminates the
  connection. The intra-Zinder defaults are safe because both ends are
  ours; they are not safe to propagate blindly to Zebra Indexer.
- **tonic issue #1635:** after a peer restart, hyper's h2 reconnect
  state machine can wedge for 60+ seconds even with keep-alive
  configured, ignoring channel timeouts. Keep-alive is not sufficient
  on its own.
- **jsonrpsee 0.26 has no surface to inject hyper config.** No
  `pool_idle_timeout`, no `tcp_keepalive`, no
  `with_custom_hyper_client`. The structural fix shape for that seam is
  caller-side rebuild after N consecutive transport errors plus an
  aggressive `request_timeout`.

The codebase already has the architectural precedents this decision
builds on. ADR-0004 declares `zinder-source` the only crate allowed to
depend on upstream-node client crates, transport DTOs, or JSON-RPC
libraries. ADR-0006 declares bearer-token auth and its channel
construction live exclusively in `crates/zinder-runtime/src/auth.rs`.
The `wire/` convention in `zinder-core` and `zinder-proto` shows the
established shape for cross-cutting concerns: one named module per
owning crate, doc-framed as policy and convention (not utility),
enforced by a structural invariant test
(`crates/zinder-core/tests/wire_invariants.rs`).

## Decision

### Core transport modules, one per owning crate

Transport policy lives in two named `transport` modules, one in each
crate that owns an upstream surface:

- [`crates/zinder-runtime/src/transport.rs`](../../crates/zinder-runtime/src/transport.rs)
  owns the contract for *intra-Zinder gRPC* (explorer ↔ query, query ↔
  ingest, compat ↔ ingest). Exposes `connect_zinder_grpc(endpoint,
  bearer_token) -> Result<AuthenticatedChannel, _>`, the three
  `ZINDER_GRPC_*` keep-alive constants, and
  `validate_zinder_grpc_endpoint(addr) -> Result<(), InvalidZinderGrpcEndpoint>`
  for the validation-at-load path config code uses to reject malformed
  URLs at process start. The `AuthenticatedChannel` type alias and
  bearer-token primitives stay in `auth.rs` (they describe *what the
  channel carries*, not how it is built).

- [`crates/zinder-source/src/transport.rs`](../../crates/zinder-source/src/transport.rs)
  owns the contract for every long-lived client to *Zebra* (one
  jsonrpsee HTTP/1.1 client, two tonic Indexer gRPC channels). Exposes
  `build_zebra_json_rpc_client(...)`,
  `connect_zebra_indexer_channel(...)`, the `ResilientClient<C>`
  wrapper (see below), and the `ZEBRA_*` policy constants.

Putting Zebra transport in `zinder-source` is mandated by ADR-0004;
putting intra-Zinder transport in `zinder-runtime` is mandated by
ADR-0006 and by `zinder-runtime`'s declared "no domain types" boundary.
Collapsing the two into one crate would violate one of the two ADRs.

Service-owned compatibility adapters may additionally own focused transport
modules for external product dependencies that are neither Zebra nor another
Zinder service. Those modules keep the external product's endpoint, timeout,
response-bound, caching, and failure-shape policy at the service edge. They do
not gain ownership of Zebra clients or intra-Zinder channels. The first such
module is
[`services/zinder-compat-cipherscan/src/market_price.rs`](../../services/zinder-compat-cipherscan/src/market_price.rs),
which owns Cipherscan's external current-price request and bounded stale cache.

### Per-upstream policy, not one-size-fits-all

Each transport module owns its own set of named `const Duration`
policy values. The intra-Zinder values may safely be aggressive
(`keep_alive_while_idle(true)`) because both endpoints are Zinder
processes and the keep-alive ping behavior is coordinated. The
Zebra-facing values deliberately omit `keep_alive_while_idle(true)`
because Zebra Indexer's tonic server config is outside our control and
tonic #258 is a real risk; the always-active stream pattern means
calls-only pings still cover the bug class.

The constants are *not* operator-tunable. Each carries a doc comment
explaining the chosen value and citing the relevant issue
(tonic #1254, #1635, #258; hyper #3640). This matches the existing
pattern of `MAX_SUBTREE_ROOTS_PER_REQUEST` and `PIPELINE_DEPTH`:
internal policy expressed as a discoverable constant, not a runtime
knob nobody will tune correctly.

### `ResilientClient<C>` for Zebra-facing clients

Keep-alive plus aggressive request timeouts cover the common stale-
connection cases but leave residual edge cases (jsonrpsee 0.26 pool
behavior, tonic #1635's reconnect wedge). The structural answer is a
generic wrapper, located in
[`crates/zinder-source/src/transport.rs`](../../crates/zinder-source/src/transport.rs):

```rust
pub struct ResilientClient<C> { /* arc-swap inner + AtomicU32 counter */ }
```

Adapters call `record_outcome(&result)` on every call's outcome.
Errors that classify as `SourceFailureClass::NodeUnreachable` or
`StreamDisconnected` increment a consecutive-failure counter; after
`ZEBRA_REBUILD_THRESHOLD` (3) consecutive transport failures the next
access swaps the inner client for a freshly-built one via the
rebuilder closure captured at construction. Readers pay one atomic
load via `arc-swap`; rebuilds serialize behind a `tokio::sync::Mutex<()>`
so two concurrent failure cascades do not both rebuild.

The wrapper is generic over any `Clone + Send + Sync + 'static` client
type. Both the jsonrpsee `HttpClient` and tonic `Channel` satisfy that
bound. The classifier function lives in the same module and reuses
the existing `SourceFailureClass` mapping at
[`crates/zinder-source/src/source_error.rs:367`](../../crates/zinder-source/src/source_error.rs);
no new error taxonomy.

### Intra-Zinder gRPC uses its own channel policy

Intra-Zinder channels use the coordinated keep-alive and request-timeout policy
owned by `zinder-runtime::transport`. They do not use `ResilientClient`: that
wrapper classifies Zebra source failures and belongs to the upstream-source
boundary. Keeping the two policies separate avoids importing source-specific
failure semantics and `arc-swap` into every internal service channel.

### Observability

A single helper, `record_transport_event(peer, event, reason)`,
emits structured `tracing::warn!` (target `zinder::transport`) for
`transport_reconnecting` and `tracing::info!` for
`transport_reconnected`. The counter
`zinder_transport_reconnect_total{peer, reason}` is incremented on
each rebuild via the existing `metrics` facade. Every adapter — the
three Zebra clients today, plus any future Zebra or intra-Zinder
adapter — calls it. Operators alert on the counter as a flap detector,
grep one log target for the lifecycle events.

### Structural invariant

[`crates/zinder-source/tests/integration/transport_invariants.rs`](../../crates/zinder-source/tests/integration/transport_invariants.rs)
walks the workspace source tree and asserts that `Endpoint::from_shared` and
`HttpClientBuilder` are referenced only inside the two core transport modules.
It separately permits `reqwest::Client::builder` only in explicitly listed
service-owned external-product transport modules. New code that reaches around the boundary fails the
test with a message naming the offending file and pointing at the
module to import from. This is the same enforcement pattern that
`crates/zinder-core/tests/wire_invariants.rs` uses for byte-level wire
translations.

## Consequences

- Every long-lived client to a Zebra upstream survives upstream
  restart, NAT timeout, and laptop sleep without operator
  intervention.
- A new Zebra adapter (e.g. zcashd JSON-RPC fallback) adds one
  factory function in `zinder-source::transport` and a wrap site in
  the adapter struct. Policy constants stay in one place.
- A new intra-Zinder service-to-service channel calls
  `zinder_runtime::transport::connect_zinder_grpc`. No new
  per-service connect helper is allowed; the structural invariant
  rejects direct `Endpoint::from_shared` calls outside the transport
  module.
- A compatibility service may own a narrowly scoped external-product HTTP
  transport without moving product-specific policy into `zinder-source`.
  Adding one requires an explicit invariant allowlist entry; it does not relax
  Zebra or intra-Zinder transport ownership.
- The `arc-swap` crate enters the workspace dependency graph through
  `zinder-source`. It is small (~50 KB), widely deployed in the Rust
  async ecosystem, and the only `unsafe` it contains is in its
  documented lock-free pointer swap.
- Keep-alive durations are not operator-tunable. Deployments that
  need different values edit the constant in source, just as they
  would for `MAX_SUBTREE_ROOTS_PER_REQUEST` or
  `PIPELINE_DEPTH`. The constants carry doc comments naming the
  upstream bugs that motivated each value.
- Migrating away from jsonrpsee to raw reqwest remains a future
  option behind one factory function; the rest of the codebase is
  decoupled from the choice through the transport module's public
  surface.
