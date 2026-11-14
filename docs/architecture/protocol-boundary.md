# Protocol Boundary

`zinder-proto` owns Zinder's service protobufs, vendored compatibility
protobufs, generated Rust modules, and wire-level byte conventions. Domain and
storage crates use their own types and convert at service adapters.

The crate is the low-level Rust wire-binding surface and is prepared in
lockstep with `zinder-core` and `zinder-client`. Wallet and application
consumers should prefer the typed `zinder-client` API; direct `zinder-proto`
use is for service implementations, custom transports, reflection, and
protocol tooling.

Generated Rust modules and descriptor sets are checked in under
`crates/zinder-proto/generated/`. Normal builds and downstream package builds
therefore require neither `protoc` nor a build script. The private repository
codegen tool is the only place that depends on `prost-build` and
`tonic-prost-build`; the protocol workflow runs it with the pinned Buf and
protoc versions and rejects generated-file drift.

Upstream node protocols belong to [Node source boundary](node-source-boundary.md).
Storage encodings belong to [Storage backend](storage-backend.md).

## Protocol surfaces

| Surface | Rust module | Service owner | Release status |
| --- | --- | --- | --- |
| Native wallet API | `zinder_proto::v1::wallet` | `WalletQueryGrpcAdapter` in `zinder-query` | Published native query runtime |
| Native explorer API | `zinder_proto::v1::explorer` | `zinder-explorer` | Optional runtime: the `zinder-explorer` release image with the Compose overlay, or the `zinder-wallet-serving-explorer` single-volume image |
| Private control APIs | `zinder_proto::v1::ingest` | `zinder-ingest` and `zinder-projector` | Internal deployed services |
| Shared operations messages | `zinder_proto::v1::ops` | Runtime and service adapters | Embedded in native/control surfaces |
| Lightwalletd compatibility API | `zinder_proto::compat::lightwalletd` | `zinder-compat-lightwalletd` | Published compatibility service |
| Zebra indexer client contract | `zinder_proto::external::zebra` | `zinder-source` | Client-only upstream protocol |

`CanonicalReader` is a Rust storage trait, not a gRPC service. It is the
fence-bound canonical read boundary that `zinder-query` consumes inside an
admitted `WalletServingReadPair`; `RocksDbCanonicalSecondary` implements it.

## Ownership rules

`zinder-proto` owns:

- native `.proto` files and generated `prost`/`tonic` modules;
- vendored lightwalletd protobuf files and their provenance record;
- the external Zebra indexer client schema used by `zinder-source`;
- protocol golden fixtures and wire-compatibility tests; and
- shared enums and messages whose semantics cross a service boundary.

Service crates do not hand-write `prost::Message` types or generate private
copies of shared protocols. Domain crates do not expose generated protocol types
in public APIs. Storage-private control records may live in `zinder-store` when
they never cross a service boundary.

Native protocol names use Zinder's domain vocabulary. Compatibility protocol
names remain identical to the external contract even when their wording differs
from native APIs.

## Native wallet API

`WalletQuery` is the native wallet and application read contract. The
`zinder-query` crate implements it over `WalletQueryApi` and exact-fence
canonical/wallet-projection readers. The release topology publishes a
standalone native query listener and a separate lightwalletd compatibility
listener; both use the shared exact-fence serving-pair implementation.

Wallet responses carry the chain identity needed to prevent mixed-epoch reads.
Streaming and pagination requests use bounded ranges and authenticated cursors.
Capabilities are advertised by exact strings through `ServerInfo`; clients gate
features on those strings rather than parsing a Zinder version.

## Native explorer API

`ExplorerQuery` serves explorer, dashboard, and analytics shapes. The runtime
composes a process-owned materialized-view secondary with selected
`WalletQuery` calls. It does not read the canonical store, so it never
advertises a capability whose policy requires retained canonical facts.
Every response carries `ExplorerFreshness`, and optional fields use typed
unavailability reasons. The checked-in
`deploy/docker-compose.explorer.yml` overlay runs the explorer beside the
wallet-serving topology for Zexplorer and other named explorer consumers. See
[Explorer plane](explorer-plane.md).

## Private control APIs

The `zinder.v1.ingest` package contains:

- `CanonicalControl` for canonical writer position and authenticated checkpoint
  evidence;
- `ProjectorControl` for wallet-projection coordination; and
- `IngestControl` for writer status, retained chain events, mempool events, and
  source-backed control operations.

These endpoints are private operational contracts. They use the shared
transport policy and fail closed when authentication or transport requirements
are not met. See [ADR-0006](../adrs/0006-ingest-control-transport-security.md).

`CanonicalControl.WriterStatus` carries the writer's visible fence as one
`CanonicalWriterFence` message. The writer encodes it with
`zinder_proto::wire::canonical_writer_fence_message`. Secondary readers encode
their own fence with the same function and compare the two field for field, as
described in
[Service operations](service-operations.md#canonical-fence-attestation).

## Lightwalletd compatibility

The vendored `CompactTxStreamer` schema is an external contract. Its source pin
and provenance live in
`crates/zinder-proto/proto/compat/lightwalletd/UPSTREAM.md`. The compatibility
adapter translates native query results, byte order, errors, and capability
claims into that contract. It does not mutate canonical storage or use the
upstream node as a fallback for indexed history or canonical reads. Sparse
tree-state fill and transaction broadcast are allowed only where
`WalletQueryApi` explicitly delegates those operations upstream.

Compatibility claims are bounded by executable certification. The stable method
matrix lives in [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md).

## Byte conventions

Wire fields follow explicit conventions:

- hash strings use lowercase RPC display order;
- raw hash bytes use the order documented by the owning protobuf field;
- opaque cursors are not parsed or synthesized by clients;
- integer fields include units in their names when the type alone is
  insufficient; and
- unknown enum values fail or degrade according to the method contract rather
  than silently mapping to a valid domain value.

Conversion helpers live at protocol adapters. Storage encodings and protobuf
encodings must not share serializers merely because both contain the same
domain value.

## Evolution and verification

- Native schemas evolve compatibly within a versioned package: additive fields
  receive new tags, and removed fields reserve their tags and names.
- Breaking native wire shapes use a new versioned package. Versioned
  capabilities describe optional behavior without weakening protobuf
  compatibility.
- Vendored lightwalletd and external Zebra schemas retain upstream field and
  reservation rules and change only with an explicit update to their pinned
  source commits and provenance records.
- Proto tests cover golden decoding, byte order, enum mappings, pagination, and
  epoch identity.
- `scripts/regenerate-zinder-proto.sh --check` proves that checked-in Rust and
  descriptors match the source schemas with the pinned code generators.
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps`
  keeps generated and adapter documentation references valid.
