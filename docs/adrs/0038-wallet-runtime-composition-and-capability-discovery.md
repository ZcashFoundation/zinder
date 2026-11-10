# ADR-0038: Wallet runtime composition and capability discovery

| Field | Value |
| --- | --- |
| Status | Accepted |
| Product | Zinder |
| Domain | Native wallet serving, capability discovery, consumer admission |
| Related | [Wallet data plane](../architecture/wallet-data-plane.md), [Public interfaces](../architecture/public-interfaces.md), [Canonical storage topology](0035-canonical-storage-topologies.md), [Canonical storage access](0003-canonical-storage-access-boundary.md) |

## Context

The native wallet runtime historically had two query implementations. A
primary-store implementation exposed the complete method set, while the
standalone release binary used an exact canonical-and-wallet secondary pair.
Several exact-pair methods were not implemented, so the binary selected
`WalletCapabilityProfile::ExactPair` to suppress their capability strings.

That profile described an implementation accident rather than the running
endpoint. Capability discovery also accepted independently configured booleans
for storage retention and optional providers. The resulting descriptor could
disagree with persisted storage, the admitted serving pair, installed handlers,
or ingest-control wiring. It could advertise structurally unavailable
operations and omit operations whose retained artifacts were present.

Zinder controls its native consumers and can make a coordinated breaking
change. Compatibility aliases would preserve two vocabularies and the invalid
states this decision removes.

## Decision

Zinder has one production native wallet-query composition. It reads through an
admitted `WalletServingReadPair`; every request captures one immutable pair at
an exact canonical fence.

The admitted query privately owns one immutable
`NativeWalletEndpointCapabilities` value. That value is derived once, after
composition and admission, from concrete evidence:

- the serving pair's authenticated persisted `RawBlobRetention` and projection
  evidence;
- the methods implemented by the production query;
- admitted upstream-node capabilities and the concrete tree-state and
  broadcaster dependencies; and
- any authenticated ingest-control dependencies whose methods the native
  adapter exposes.

There are no public setters, capability-string constructors, deserialized
profiles, or caller-populated support booleans. `WalletCapabilityProfile`,
`Complete`, `ExactPair`, and `WalletAdvertiseInputs` are removed without
aliases. Adding a native capability requires updating the protocol registry,
the concrete implementation, evidence derivation, positive and negative
contract tests, and a current production-consumer flow.

`ServerInfoSettings` becomes `WalletEndpointMetadata`. It contains endpoint
identity and descriptive facts such as network, build revision, schema
version, reorg window, and materialized-view identity; it contains no support
decisions. Retention-duration fields with no authenticated writer evidence are
removed rather than populated from reader configuration. The production gRPC
adapter accepts the admitted query and metadata,
obtains the query-owned capability set, and encodes that exact set in
`WalletQuery.ServerInfo`. The operational HTTP endpoint is bound only after
composition and admission and receives the same immutable value. Native and
operational discovery therefore cannot be configured or paired independently.

The static table in `zinder-proto::capabilities` remains the source of protocol
identifiers, owning surface, method association, and semantic version. It does
not decide which subset a process contains.

The descriptor is deliberately native-scoped. `WalletServingQuery` is a domain
read implementation shared by the native and lightwalletd compatibility
adapters, so an in-process domain method may exist for compatibility without
being admitted on the native endpoint. Method existence is not a native support
claim. Each protocol adapter owns its admission guard and must derive its
advertised contract from the concrete providers installed for that adapter.

`WalletServingPairSlot` is an opaque read handle. Only
`WalletServingPairPublisher` can replace its pair, and every request captures
one pair before reading. Test and library callers cannot mutate the slot or
install a replacement that bypasses retention, fence, or source-identity
admission.

The release query admits one probed `NodeSource` and installs clones of that
same source as its tree-state and broadcast providers. A
node-backed capability requires the corresponding probed node feature, the
concrete provider, and `TipId`, which is the liveness prerequisite. The release
query also admits one `AdmittedIngestControl` before opening storage or binding
traffic. Admission opens an authenticated channel once, calls `ServerInfo`,
and requires the exact `zinder-ingest` service identity, network, contract
revision, and seven control methods needed by the composition:
`ServerInfo`, `WriterStatus`, `MempoolSnapshot`, `MempoolTransaction`,
`MempoolEvents`, `TransparentMempoolOutputsByAddress`, and
`TransparentMempoolSpendsByOutpoint`. The query, serving-pair publisher, live
wallet handlers, and readiness probe clone that same accepted channel. They do
not accept independently configured provider identities or perform a second
dial at their use sites.

This evidence admits native transaction lookup, mempool snapshot and events,
and transparent mempool outputs-by-address and spends-by-outpoint.
Transaction-byte support additionally requires authenticated transaction-blob
retention. Transparent live balance remains omitted: its legacy handler
performs multiple canonical and live calls without one authenticated mempool
generation, so the two admitted primitives are not sufficient evidence for the
composite claim. Transparent mempool outputs-by-outpoint also remains omitted
because it is not part of the required admitted provider contract.
Chain value pools also remain omitted: discovering an OpenRPC method name does
not prove that the node returns the required `valuePools` payload, and the
readiness loop does not retain that semantic evidence. Method presence in the
protocol, adapter, or upstream schema is not support.

Every native RPC except `ServerInfo` checks its method capability in the native
adapter before invoking a domain method, touching storage, or contacting a
provider. Optional response fields have their own field capability checks. This
keeps direct native calls honest even when a client ignores discovery without
incorrectly applying native policy to the compatibility adapter. Bounded range
capabilities also carry enforced production limits; an advertised range method
is not permission for unbounded work. `SubtreeRoots` accepts at most 1,024
entries per request across both query implementations and rejects a larger
range before pair capture or storage access with
`SUBTREE_ROOT_RANGE_TOO_LARGE`.

## Support, readiness, and request outcomes

Structural support is immutable for the process lifetime. A structurally
supported operation is advertised even while one of its admitted dependencies
is temporarily unhealthy; readiness becomes false and the request returns
`UNAVAILABLE` with `SERVICE_NOT_READY` plus the stable `readiness_cause`
metadata. A structurally absent operation is omitted and a direct call returns
a precise unsupported or failed-precondition result. An expired chain view
retains its capability and returns the typed epoch-expiry outcome. A
replacement pair that changes the process contract is rejected, leaves the
prior pair authoritative where safe, and makes readiness false.

Capability discovery is not a health check. Pair publication, provider health,
and replica lag may change readiness but never rewrite the endpoint contract.

`WalletServingReadiness` retains pair state, admitted-node state, and
ingest-control health independently and projects their conjunction into the
shared operational and gRPC readiness handle. Shutdown dominates later writes.
The node liveness task uses `tip_id` plus upstream synchronization health
through the exact admitted source; it never repeats capability discovery or
mutates the cached structural feature set. The ingest-control task calls
`WriterStatus` and `MempoolSnapshot(max_entries = 1)` through the admitted
channel, validates coherent network and tip evidence, and publishes
`ingest_control_unavailable` on failure. It does not repeat `ServerInfo`
admission. The task logs only outage and recovery transitions, and shares the
serving-pair refresh cadence because both observe the same control plane. An
unexpected serving-pair publisher, node-readiness task, or
ingest-control-readiness task exit drains readiness, cancels the runtime, and
returns a typed process failure.

## Storage and pair admission

`RawBlobRetention` is part of canonical store identity and is authenticated
through construction, append, replacement, reopen, secondary catch-up, and
serving-pair admission. The reader declares the retention it expects and fails
without mutation when the persisted identity differs.

`RawBlobRetention::All` makes serialized full-block reads structurally
available. `RawBlobRetention::Transactions` retains transaction bytes but omits
full-block capabilities. The query never repairs missing historical blobs from
the upstream node. Changing retention on a non-empty store requires building a
new store and performing a blue-green cutover; it is not a runtime toggle.

Pair rotation may advance the canonical fence but must preserve retention and
every other input to the immutable endpoint contract. A candidate that changes
those inputs is not published.

## Consumer admission and certification

Native consumers such as Zallet own the exact capability requirements for each
packaged feature combination. They fetch `ServerInfo` and report the complete
missing set before opening or migrating wallet storage. A capability profile
named after a consumer is not added to Zinder.

Zallet treats an expired exact view as a typed retry condition and reacquires
the whole workflow boundary. Zinder does not retain an unbounded history of
serving pairs. Consumer tests must prove that a retry does not commit a partial
block batch.

ZODL continues to consume the separate lightwalletd-compatible
`CompactTxStreamer` surface through trusted TLS at the deployment edge. Native
capability strings and full-block requirements do not leak into that protocol.
The compatibility runtime derives its own structural support from retained
storage and concrete providers and fails before binding when a required
provider is absent.

Zinder contract tests prove internal storage, composition, discovery, error,
and protocol invariants. Real current Zallet and ZODL binaries provide separate
consumer certification. Fixture-backed parity tests remain useful contract
evidence but are not downstream certification.

## Consequences

- Operators configure storage retention and dependency endpoints, not an
  implementation profile or a set of overlapping support flags.
- Invalid capability claims become unconstructable at the release composition
  root, and missing structural requirements fail before traffic is accepted.
- Full-block service has explicit disk, ingestion, secondary catch-up, and
  migration cost.
- Tests and alternate compositions must supply the same concrete evidence as
  production or make no support claim.
- Capability additions require a named production flow and end-to-end evidence;
  speculative provider registries, consumer modes, and generic capability
  crates are unnecessary.

## Rejected alternatives

### Configure or rename the profile

Making the profile configurable, or renaming it to a runtime or storage mode,
preserves a shadow model that can disagree with the composed service.

### Configure individual capability booleans

Independent booleans maximize invalid states and make operational intent
ambiguous. Persisted facts and admitted providers are the authority.

### Derive support from the protocol registry alone

The registry proves vocabulary and descriptor coverage. It cannot prove that
storage retained an artifact or that a required provider was composed.

### Add a generic capability-provider framework

There is one production composition root. A provider hierarchy adds
indirection without removing the need to bind each claim to its concrete
handler and evidence.

### Retain historical pairs indefinitely

Unbounded pair retention has unbounded storage and resource semantics. Typed
view expiry and whole-workflow retry satisfy the current consumer requirement.

### Add compatibility aliases

Aliases keep obsolete names and constructors available for new code. The
controlled consumer cutover uses the current contract without shims.
