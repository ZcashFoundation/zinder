# ADR-0019: Transport policy ownership and self-healing

Status: Accepted
Date: 2026-05-19
Related: [ADR-0004](0004-node-source-and-protocol-boundaries.md),
[ADR-0006](0006-ingest-control-transport-security.md),
[ADR-0013](0013-source-failure-recovery-topology.md),
[ADR-0014](0014-shared-configuration-sections.md)

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

### Indexer channel TLS

The indexer URL scheme decides whether the indexer channel uses TLS. An
`http://` URL opens plaintext HTTP/2. An `https://` URL negotiates TLS through
tonic's rustls integration. The workspace already enables tonic's `tls-ring`
and `tls-native-roots` features, and the client uses ring when the process has
no default crypto provider.

Zakura allows a plaintext indexer listener only on a loopback address. It
refuses to start a non-loopback indexer listener unless its `[rpc.indexer_tls]`
section configures mutual TLS. A Zinder deployment that reaches a Zakura
indexer over a network therefore needs TLS with a client certificate.

The shared `[node.indexer_tls]` section configures an `https://` indexer
channel:

| Field | Meaning |
| --- | --- |
| `ca_cert_path` | PEM CA bundle that verifies the node's certificate. When it is absent, the client uses the system roots. |
| `client_cert_path` | PEM client certificate chain presented to a node that requires mutual TLS. |
| `client_key_path` | PEM private key for that chain. |
| `server_name` | Name verified against the node's certificate when it differs from the URL host. |

An `https://` URL without the section verifies the node against the system
roots and presents no client certificate. Configuration loading rejects these
combinations before any connection:

- the section with an indexer URL that is not `https://`;
- the section without an indexer URL; and
- a client certificate without its key, or a key without its certificate.

Empty and whitespace-only fields count as absent. The environment loader keeps
empty values, so this rule lets an operator clear a field by setting its
variable to the empty string.

`connect_zebra_indexer_channel` reads the PEM files each time it builds a
channel. The streaming mempool and chain-tip sources build a channel for every
subscription. A rotated certificate or key therefore takes effect on the next
reconnect, without a restart. `ZebraIndexerBlockSource` builds its channel once
and keeps the material it read then. `ResilientClient` wraps only the JSON-RPC
client and is not involved.

tonic skips PEM entries it cannot parse. A CA file without a certificate would
otherwise produce an empty trust store without an error. The transport
therefore requires at least one certificate in the CA file, at least one
certificate in the client chain, and one private key in the key file. It
reports `ZebraTransportError::TlsMaterialUnreadable { role, path, reason }` for
a file that fails these checks and `ZebraTransportError::TlsConfigRejected` for
settings tonic or rustls rejects. `zinder-ingest` calls
`validate_zebra_indexer_endpoint` at startup. The function builds the indexer
endpoint, including its TLS configuration, without connecting, so unusable
material ends the process with a configuration error instead of failing every
later subscription.

`zinder-bench` keeps a plaintext indexer channel because it has no `[node]`
section to carry the TLS settings. A benchmark against Zakura uses a loopback
indexer listener.

An egress proxy that presents the client certificate on Zinder's behalf would
also work. It was not chosen because it adds a component to every deployment
that needs the indexer stream, and because `zinder-client` already negotiates
TLS in-process for `https://` endpoints. Keeping TLS in the transport module
gives both clients one scheme rule.

### Credential delivery

Zinder binaries read indexer TLS material from files only. Configuration holds
paths, `--print-config` prints those paths and never file contents, and key
bytes exist only while a channel is being built. One code path serves every
platform that can deliver a secret as a file, including Kubernetes secret
volumes, Docker and Compose secrets under `/run/secrets`, systemd
`LoadCredential`, and Vault Agent.

Some platforms deliver secrets only as environment variables. For them, the
three single-volume images, `zinder-canonical-runtime`, `zinder-wallet-serving`,
and `zinder-wallet-serving-explorer`, start as root and accept three entrypoint-only variables:
`ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM`,
`ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM`, and
`ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM`. The shared fragment
`deploy/materialize-indexer-tls` converts each non-empty value into a file
before any runtime process starts:

- It rejects a value without a `-----BEGIN` line or without a line break,
  because platform editors can collapse multi-line values into text that no PEM
  parser accepts.
- It rejects an item that sets both its `*_PEM` and its `*_PATH` variable.
- It writes to `/dev/shm` only when that directory is a writable tmpfs, and
  falls back to `/run`, which may be on disk. It logs the location it chose, and
  it fails closed when neither is writable.
- It recreates a root-owned directory on every start: `rm -rf`, which does not
  follow a planted symlink, then `mkdir -m 0711` without `-p`, which fails if
  anything reappears in between.
- It writes each file under `umask 077` with mode 0400, gives it to the
  `zinder` user with `chown -h`, and exports the matching `*_PATH` variable.

No runtime process receives a `*_PEM` variable. The canonical image starts
`zinder-ingest` through `exec env -u`. The wallet-serving image keeps a
supervising shell, so after materializing it re-executes itself through
`exec env -u`. The wallet-serving Explorer image materializes and re-executes
the same way before it starts either the wallet-serving entrypoint, which then
finds no `*_PEM` variable, or `zinder-explorer`, which receives the same
`*_PATH` variables. The supervising shell's initial environment, which
`/proc/1/environ` exposes, and every process it starts then hold paths only. A
Zinder binary that does receive a `*_PEM` variable refuses to start with an
error that names the three images and the `*_PATH` variable to use instead. The
per-binary images have no root entrypoint and accept files only.

The platform's own environment views, health checks, and `exec` sessions can
still show the `*_PEM` values. Platforms that can mount files should use
files.

Reading PEM text from the environment inside the Rust process was rejected. The
value would stay readable in `/proc/<pid>/environ` for the life of the process.
Removing it requires `std::env::remove_var`, which is `unsafe` in Rust 2024,
and the workspace denies `unsafe_code`.

`node.auth.cookie` already accepts an inline credential from the environment.
The indexer client key is handled more strictly because the two credentials
differ in lifetime. The node regenerates its cookie on every restart, so a
leaked cookie stops working when the node restarts. A client private key is a
long-lived identity: whoever holds it can authenticate to the indexer as Zinder
until its certificate expires or is revoked.

[ADR-0006](0006-ingest-control-transport-security.md) excludes native TLS,
mTLS, and environment-sourced secrets for the IngestControl plane, where both
ends are Zinder processes and a reverse proxy can terminate TLS. Those
exclusions cover IngestControl only. The indexer channel is a client of a
server whose TLS policy the node operator sets, so Zinder must meet that
policy itself.

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
- Indexer TLS adds no production dependency beyond `rustls-pki-types`, which
  tonic already pulls in. The transport uses it directly to require a
  certificate or key in each PEM file.
- An operator rotates indexer client material by replacing the files. The
  streaming sources pick up the new files on their next reconnect.
- The single-volume entrypoints own a small amount of root-run file handling
  for env-only platforms. `scripts/test-indexer-tls-entrypoints.sh` covers
  materialization, conflicts, malformed values, the fallback location, and the
  absence of `*_PEM` variables in every started process.

## Revision history

- 2026-11-12: Added indexer channel TLS: the URL scheme selects TLS,
  `[node.indexer_tls]` configures the trust roots, an optional client identity,
  and the verified server name, and PEM files are re-read whenever an indexer
  channel is built and validated at ingest startup. Added credential delivery:
  binaries read TLS material from files only, and the single-volume image
  entrypoints convert `*_PEM` variables into files for env-only platforms.
  Recorded why an egress proxy and in-process environment reads were not
  chosen, and that ADR-0006's exclusions cover IngestControl only.
- 2026-11-14: Added `zinder-wallet-serving-explorer` as the third single-volume
  image that accepts the entrypoint-only `*_PEM` variables, and described how
  its entrypoint delivers the materialized paths to both the wallet-serving
  processes and `zinder-explorer`.
