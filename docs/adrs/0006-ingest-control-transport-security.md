# ADR-0006: IngestControl Transport Security

| Field | Value |
| ----- | ----- |
| Status | Accepted |
| Product | Zinder |
| Domain | Service-to-service transport security |
| Related | [ADR-0003](0003-canonical-storage-access-boundary.md), [Service operations](../architecture/service-operations.md), [Service boundaries](../architecture/service-boundaries.md) |

## Context

[ADR-0003](0003-canonical-storage-access-boundary.md) settles that `zinder-ingest`
exposes a private `IngestControl` gRPC endpoint and that
`zinder-query` and `zinder-compat-lightwalletd` reach the writer's live state
through it. The endpoint serves `WriterStatus`, `VisibleChainEvents`,
`MempoolSnapshot`, and `MempoolEvents`. Anyone who can route a TCP
connection to the listen address can subscribe to chain events, read the
mempool, and observe the writer's progress.

The default deployment is single-host: the writer and one or more readers
share a host and bind the IngestControl port to `127.0.0.1`. Operators
running readers on a different host (a separate VM, a different node in a
private VLAN, a Wireguard peer) need a transport security story that does
not assume the network is trusted. ADR-0003 defines the topology; this ADR
fills the gap.

The constraints on the answer are:

- ZFND-internal single-operator deployments must pay no machinery for
  trusted-network operation. The localhost case has to remain the
  zero-config default.
- Adding native TLS to the gRPC server adds cert provisioning, rotation,
  and CA-distribution machinery that operators with existing reverse-proxy
  infrastructure (Caddy, nginx, traefik) already solve. We do not want to
  duplicate that.
- The `zinder-compat-lightwalletd` public surface already requires TLS
  termination via a reverse proxy in production (see
  [Service operations §Validation Tiers and Trust Boundaries](../architecture/service-operations.md)).
  Whatever we choose for IngestControl should compose with that pattern,
  not contradict it.
- Workspace lints ban `unwrap`, `expect`, `panic`, and disallow secrets in
  log targets. Anything that touches an authentication secret has to
  redact it through `secrecy::SecretString` or equivalent.

Five candidates were considered:

1. **No auth, no TLS.** Operators are responsible for binding to
   `127.0.0.1` or a trusted interface. Cross-host deployment requires
   external tooling (SSH tunnel, VPN with strict ACLs).
2. **Native server TLS.** `zinder-ingest` accepts cert and key paths in
   config and binds with `tonic::transport::ServerTlsConfig`. Readers
   pass a CA bundle through `tonic::transport::ClientTlsConfig`.
3. **Mutual TLS (mTLS).** Same as native TLS plus client certs. Each
   reader has its own cert; the writer enforces a CA-pinned client
   identity.
4. **Shared-secret bearer token over plaintext gRPC.** Writer enforces a
   token loaded from a file at startup; readers attach it as
   `authorization: Bearer <token>` via a tonic interceptor.
5. **Compile-time auth feature flag.** Auth code only links when the
   `auth` cargo feature is enabled; the trusted-network build has zero
   bytes of auth machinery in the binary.

Option 1 alone is unacceptable for any non-loopback deployment because the
writer's storage handle leaks to anyone with network access to the port.

Option 2 (server TLS) addresses encryption and server identity but not
*client* identity: any caller that completes the TLS handshake can issue
RPCs. To get caller identity from server-only TLS, we would still need a
bearer token or similar. Server TLS alone duplicates what the operator's
reverse proxy already does.

Option 3 (mTLS) is the strongest answer technically. It costs cert
issuance per reader, rotation via cert reload (an open hot-reload story
in tonic), and a CA distribution channel. For a single-operator
deployment with a small fixed reader count, this is more machinery than
the threat justifies. For larger fleets, the operator's existing PKI is
the right place for it, fronted by a reverse proxy.

Option 4 (bearer token + reverse proxy for TLS) splits responsibility
cleanly: Zinder enforces caller identity through a shared secret;
encryption and server identity are the proxy's job. The operator's
reverse proxy is the same proxy they already run for the public
lightwalletd-compat surface, so we do not introduce a new operational
component.

Option 5 (compile-time feature flag) trades flexibility for code-size
purity. The runtime cost of an opt-in bearer token is one branch per
intercepted request when no token is configured (`if let
Some(expected) = ...` returns `Ok(request)` immediately); compile-time
gating saves nothing meaningful at the cost of two binary variants and
an extra CI matrix dimension.

## Decision

### Default: no auth, no TLS, loopback or trusted network

The IngestControl endpoint runs plaintext h2c with no authentication when
`[ingest_control] bearer_token_path` is unset on the writer. Operators
who run all processes on one host and bind the port to `127.0.0.1` (or
who put the port on a private network they trust) configure nothing.
This is the **localhost-default** story referenced throughout ADR-0003
and the service-operations doc.

This default exists because the threat model collapses for single-host
deployments: an attacker with local code execution on the writer's host
already has the storage directory; an additional auth check on a
loopback gRPC port is theatre.

### Optional: shared-secret bearer token loaded from a file

When operators run a reader on a different host, or when they want
defense in depth on top of a private-network ACL, they configure a
shared-secret bearer token through the shared `[ingest_control]`
section:

- `[ingest_control] bearer_token_path` on every process — the writer
  (`zinder-ingest`) reads the file to verify, and every reader
  (`zinder-query` and `zinder-compat-lightwalletd`) reads the same file to
  present.
- The same value lives in the file referenced by every process.

The writer's `BearerTokenServerInterceptor` validates the
`authorization: Bearer <token>` metadata header on every request using
constant-time comparison. Readers' `BearerTokenClientInterceptor`
attaches the header to every outbound call. When the token is unset on
either side, the interceptors pass requests through unchanged, which is
how the localhost-default story is preserved.

The token is loaded from a file because environment variables leak into
process listings (`ps`, `/proc/<pid>/environ`) and debugger snapshots.
The token is held in `secrecy::SecretString` so it never appears in
`Debug` output, log targets, or panic messages, and it is redacted from
`--print-config` (which emits the path, never the contents). Token
rotation is replace-the-file-and-restart; there is no hot-reload path,
because the failure modes of a partial-rotation half-state outweigh the
operational cost of a planned restart cycle.

### Encryption and server identity: reverse proxy

Zinder does not ship native TLS for the IngestControl plane. Operators
who need encryption (cross-host deployments outside a trusted private
network) terminate TLS in a reverse proxy (Caddy, nginx, traefik) in
front of the writer and forward h2c to the local port. Readers connect
to the proxy's HTTPS endpoint with the bearer token still attached;
the proxy passes the `authorization` header through, and the writer
performs the bearer check on the inner h2c request.

The reverse proxy is the same component operators already run for the
public lightwalletd-compat surface, so this composes with the existing
operations story rather than introducing a new one.

### Three documented deployment patterns

[Service operations](../architecture/service-operations.md) documents
exactly three IngestControl deployment patterns, in order:

1. **Localhost only.** Bind to `127.0.0.1`. No token, no TLS, no proxy.
2. **VPN or private network.** Configure the bearer token on every
   process. No TLS unless the operator wants defense in depth on top of
   the VPN.
3. **Reverse proxy with TLS.** Proxy terminates HTTPS; writer enforces
   the bearer token on the proxied request.

Operators choose; Zinder enforces no policy beyond "if you set the
token, both sides have to agree on it."

### What we explicitly do not ship

- Native `tonic::transport::ServerTlsConfig` integration.
- mTLS or any cert-based client identity.
- Environment-variable token sourcing.
- Hot-reload of the token file.
- A compile-time feature flag for the auth code path.

These are out of scope for the accepted design. Adding one requires a
separate decision that names the deployment shape it serves.

These exclusions cover the IngestControl plane only. The upstream node's
indexer channel follows
[ADR-0019](0019-transport-policy-ownership.md#indexer-channel-tls), because the
node operator sets that server's TLS policy.

## Consequences

### Operational

- Trusted-network operators (single-host, ZFND-internal default) pay
  zero machinery: one TOML field they leave unset, one CLI flag they
  do not pass, one branch per request that takes the no-op path.
- Cross-host operators without a VPN must run a reverse proxy. This is
  not new machinery; they almost certainly already run one for the
  lightwalletd-compat public surface.
- Token rotation requires coordinated restarts of every process that
  shares the secret. Operators who cannot tolerate this should front
  IngestControl with a proxy that handles its own rotation (mTLS at the
  proxy edge, plaintext bearer token from proxy to writer).

### Security boundary

- The canonical store directory remains the strongest security boundary.
  An actor with read access to the writer's storage path can forge
  cursors and read all canonical state regardless of any transport-level
  auth.
- The bearer token protects against passive observation and casual
  network access. It does not defend against an attacker who can read
  the token file or observe plaintext traffic on the wire. Operators
  who need wire-level secrecy use the reverse-proxy pattern.
- `secrecy::SecretString` and constant-time comparison are
  defense-in-depth against accidental log leaks and timing-extraction,
  not the primary defense.

### Code surface

- `crates/zinder-runtime/src/auth.rs` owns the auth primitives:
  `BearerToken`, `BearerTokenServerInterceptor`,
  `BearerTokenClientInterceptor`, `constant_time_eq`, and
  `BearerTokenError`.
- The interceptors plug into `tonic::service::interceptor::InterceptedService`
  on the server (`CanonicalIngestControlGrpcAdapter::into_server`) and into
  `IngestControlClient::with_interceptor` on every consumer.
- Consumers thread an `Option<BearerToken>` through the exact connection they
  own: `AdmittedIngestControl::connect`,
  `WalletServingPairPublisher::bootstrap_from_writer_status_endpoint`,
  `IngestControlMempoolSurface::with_bearer_token`, and
  `spawn_ingest_control_tip_change_publisher(_, bearer_token, _)`.

## Out of Scope

- Native gRPC TLS in `zinder-ingest`, `zinder-compat-lightwalletd`, or a
  custom native adapter that embeds `zinder-query`. Reverse-proxy termination
  is the documented production answer.
- mTLS or per-reader client identity.
- Hot-reload of the token file.
- Audit logging of authentication failures beyond the existing
  tonic `Status::unauthenticated` response. Operators who need
  authentication audit logs add them at the reverse-proxy layer.
