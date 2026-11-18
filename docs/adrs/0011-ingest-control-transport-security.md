# ADR-0011: Ingest control uses a bearer token, not built-in TLS

Status: Accepted

## Context

`zinder-ingest` serves a private gRPC endpoint, `IngestControl`, that the other runtimes use to reach the writer's live state: writer status, chain events, and the mempool. It also serves canonical owner checkpoint creation, which writes to disk. Anyone who can open a TCP connection to it can read the mempool, follow chain events, and watch the writer's progress.

The supported deployment runs every runtime on one host ([ADR-0004](0004-storage-and-deployment-topologies.md)), so the endpoint normally listens on loopback. An attacker with code execution on that host can already read the store directory, which is the stronger boundary. Some deployments still put the endpoint on a container or private network, where a network check adds real protection.

Native TLS would bring certificate provisioning, rotation, and CA distribution into Zinder. Operators who need encryption already run a reverse proxy for the public wallet listener, and Zinder ships no built-in TLS on any listener ([Service operations](../architecture/service-operations.md#security-boundary)).

## Decision

1. **Loopback needs no configuration.** With no token configured, `IngestControl` serves plaintext h2c without authentication. This is the default for a single-host deployment.
2. **Any non-loopback bind requires a shared bearer token.** Ingest refuses to start when `ingest_control.listen_addr` is outside loopback and `ingest_control.bearer_token_path` is unset. A public or unspecified address additionally requires `security.allow_public_bind`.
3. **The token comes from a file.** Every process that dials `IngestControl` reads the same file and sends the token on every call. The writer checks it in constant time. Environment variables are not supported, because they leak into process listings and debugger snapshots. The token is redacted from logs and `--print-config`. Rotation means replacing the file and restarting every process that shares it.
4. **Checkpoint creation has its own capability token.** The writer refuses owner checkpoint creation unless `ingest_control.checkpoint_bearer_token_path` is configured, and then requires that second token on the call. Reader processes never receive it.
5. **Encryption belongs to the operator.** Zinder ships no TLS on `IngestControl`. When its traffic must cross an untrusted network, the operator wraps it in a VPN or a TLS-terminating proxy that forwards h2c and passes the `authorization` header through.

[Configuration](../reference/configuration.md) lists the `[ingest_control]` keys.

## Consequences

- Single-host operators configure nothing and pay one no-op check per request.
- An `IngestControl` port on a shared network cannot be opened without a token.
- A leaked reader configuration cannot create owner checkpoints.
- Token rotation needs a coordinated restart of every process that shares the token.
- The token protects against casual network access. It does not protect against an attacker who can read the token file or watch plaintext traffic on the wire.
- Authentication failures return `UNAUTHENTICATED` and are not audit-logged. Operators who need an audit trail add it at a proxy.

## Alternatives rejected

- **No authentication anywhere.** It exposes the writer's live state to anyone who can reach a non-loopback port.
- **Native server TLS.** It encrypts but does not identify the caller, and it duplicates what the operator's proxy already does.
- **Mutual TLS.** It is the strongest option, but per-reader certificates, rotation, and CA distribution cost more than a single-operator deployment with a few fixed readers needs. A fleet that wants it can add it at a proxy.
- **A compile-time feature flag for authentication.** It saves one branch per request at the cost of two binary variants and a larger CI matrix.
- **Hot reload of the token file.** A partially rotated set of processes is harder to diagnose than a planned restart.
