# ADR-0007: Recover from node failures in ingest and own transport policy per upstream

Status: Accepted

## Context

Nodes restart, stall, and fall behind. Ingest must survive those failures. Storage and integrity failures must still stop the process, and a reorg deeper than the window must stop following, because retrying them could hide an unsafe state.

Long-lived connections also fail silently. In two production incidents, an internal gRPC channel hung for hours after one HTTP/2 protocol error, and ingest's JSON-RPC client reported the node unavailable for 12 hours while the node was reachable; a restart fixed both. Keep-alive alone does not cure this: the JSON-RPC library exposes no connection-pool settings, and HTTP/2 pings sent while idle make some gRPC servers close the connection.

Public listeners accept clients Zinder does not control, and an advisory against lightwalletd showed how slow or oversized requests exhaust such a server.

## Decision

1. **Ingest owns recovery.** Source adapters describe a failure with a failure class and reason. Ingest decides. While following, every source failure drains ingest readiness and retries from durable state with a backoff chosen by class. During fresh construction, ingest exits once a source failure outlasts its bounded retries, and a restart begins construction again. A storage or integrity failure exits the process, except damage that the background tree-state verification finds in stored history, which ingest reports to its readers and parks readiness on instead ([ADR-0014](0014-consumer-neutral-wallet-data-plane.md)). A replacement deeper than the reorg window parks the writer, which stays up and refuses wallet traffic ([ADR-0010](0010-reorg-window-and-displaced-blocks.md)).
2. **Readiness carries the detail.** Ingest readiness reports the failure class, the latest reason, the consecutive failure count, and the outage duration.
3. **Retries happen at one level.** During fresh construction, a call retries only transport failures. While following, each call makes one attempt, and the loop re-observes the node before it issues dependent requests.
4. **Only the writer drains.** Wallet-serving readers stay ready during a node outage, report the node as a degraded dependency, and fail only the requests that need the node ([ADR-0016](0016-wallet-serving-readiness.md)).
5. **Ingest fails over across an ordered node list.** With `[node_fallbacks.N]` configured, ingest switches endpoints only on transport-class failures or a node that stays not ready. Malformed data, protocol mismatches, and other failure classes never trigger a switch, because switching would pick whichever node's answer parses and could hide a Zinder defect. A candidate must agree with the store's network and admitted activation table ([ADR-0008](0008-consensus-parameters-from-the-node.md)). There is no timed failback, and a first sync does not fail over.
6. **Each upstream has one transport owner.** `zinder-runtime` builds every internal Zinder gRPC channel, and `zinder-source` builds every node client. A compatibility service may own a narrow transport module for an external product it calls, and so may the benchmark load client, which calls lightwalletd servers outside Zinder and sends them no keep-alive pings. A structural test refuses client construction anywhere else.
7. **Transport policy is constants, not configuration.** Internal channels ping while idle because both ends are Zinder. Node-facing channels do not, because the node's server policy is outside Zinder's control. An internal dial gives up after 5 seconds by default. The bound covers the name lookup, the TCP connect, the TLS handshake, and the peer's whole initial HTTP/2 `SETTINGS` frame, because the HTTP/2 client handshake completes before the peer sends a frame. Every HTTP/2 peer, a proxy included, opens its side of the connection with that frame, so the dial sends no request and needs no route or credential at a proxy. A peer that drops packets, or accepts the connection and then stays silent or stops partway through that frame, therefore cannot hold a caller for the operating system's connect timeout. A peer that opens with any other frame does not speak HTTP/2, and the dial fails at once. Every reconnect the channel makes on its own keeps the same bound, from the name lookup until the peer's `SETTINGS` frame is complete, so a call that waits on a reconnect to a stalled peer fails within it rather than waiting for the keep-alive ping to time out. Zinder runs the client TLS handshake itself rather than through tonic, because tonic does not expose the decrypted stream that carries the peer's frames. Callers that dial on first use share one dial in flight, so concurrent callers wait for one bound rather than one each. A runtime that dials at startup, such as `zinder-projector` and `zinder-compat-cipherscan`, dials again after a dial that times out until one connects or the runtime stops, so a peer that is slow to come up delays startup instead of ending it. A caller that needs another bound passes it to `connect_zinder_grpc_with_timeout`, and a caller that also wraps the dial in a deadline of its own lets the shorter bound end it.
8. **Node clients rebuild themselves.** After three consecutive transport failures, a node client is replaced with a freshly built one.
9. **Indexer TLS is in-process and file-based.** The indexer URL scheme selects TLS, and `[node.indexer_tls]` sets the trust roots, an optional client certificate, and the server name. Binaries read TLS material only from files, and streaming sources reread it on each reconnect. For platforms that deliver secrets only as environment variables, the single-volume images convert `*_PEM` variables into files before any runtime starts, and a binary that receives one refuses to start.
10. **Public listeners share one bounded policy.** `zinder-query`, `zinder-compat-lightwalletd`, and `zinder-explorer` bound streams per connection, request message size, requests being received at once, and responses a client stops reading. The values are constants that follow lightwalletd's fix for the same advisory. A response a client stops reading is abandoned at the send-stall deadline, which ends the call's work. The data HTTP/2 already holds for it, and its place among the connection's streams, stay until the client reads, resets the stream, or disconnects, because the HTTP/2 server does not poll a response that is waiting for flow-control window; the proxy's per-client connection limit bounds what stalled streams hold across the listener. The receive limit keeps one client from locking the others out by counting slots per client connection. Behind a proxy, a listener takes that connection from a forwarding header only when the operator sets `trust_proxy_client_address`, because any client that reaches the listener directly can set the header. It reads the header only when a request carries it once, as one address, so a proxy that appends to a client's header instead of replacing it never passes the client's value on. Clients are never grouped by IP address, because wallets behind Tor share exit addresses.

[Node endpoint failover](../architecture/chain-ingestion.md#node-endpoint-failover) describes switching, [Node source boundary](../architecture/node-source-boundary.md) describes failure classes and indexer TLS, and [Configuration](../reference/configuration.md) lists listener limits and the internal dial bound.

## Consequences

- Once following, ingest stays alive through node outages, and operators see the failure class and outage duration without reading logs.
- With fallback nodes, ingest keeps committing through an outage of its active node. Each switch costs one mempool rebuild, during which ingest readiness is withdrawn.
- A new source error must choose a failure class, and a new node adapter or internal channel adds one factory in the owning transport module.
- Operators rotate indexer client material by replacing the files.
- Operators still limit connections per client address at their proxy, because a client spread over many connections can keep the receive slots full ([Service operations](../architecture/service-operations.md#security-boundary)).

## Alternatives rejected

- **A boolean retry classification.** It cannot separate recoverable source failures from unsafe store states, and it carries no operator detail.
- **Switching nodes on any failure.** It would mask Zinder defects behind whichever node answers in a parseable form.
- **Copying the internal keep-alive settings to node clients.** Idle pings can make a node's gRPC server close the connection.
- **Rebuild-on-failure for internal channels.** It would import node failure semantics into every internal service channel.
- **A health-check request on every internal dial.** A proxy that resets unauthenticated streams or drops unknown routes would fail every dial, although it serves the methods Zinder calls.
- **A shorter HTTP/2 keep-alive interval to catch a silent peer.** The first keep-alive ping goes out one interval after the connection opens, so a 5-second bound would set the steady idle cadence to a few seconds.
- **An egress proxy that presents the client certificate.** It adds a component to every deployment that uses the indexer stream, while the client already negotiates TLS in-process.
- **Reading PEM text from the environment inside the binary.** The value would stay readable in `/proc/<pid>/environ`, and removing it needs `unsafe` code, which the workspace denies.

## Revision history

- 2026-11-23. Decision 10 states that abandoning a stalled response ends its work but leaves the data HTTP/2 holds for it until the client reads, resets, or disconnects.
