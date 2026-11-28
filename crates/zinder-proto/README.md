# zinder-proto

`zinder-proto` is Zinder's low-level generated gRPC wire-binding package. It
contains the native Zinder v1 services, the vendored lightwalletd compatibility
protocol, the vendored Zebra indexer protocol, and the native and compatibility
file descriptor sets. It is prepared and versioned in lockstep with
`zinder-core` and `zinder-client`; it is not the preferred application-facing
API.

Generated Rust is checked into the crate, so compiling a registry package does
not invoke `protoc`. The owned `.proto` source closure is included for
non-Rust generators and protocol review. Repository contributors regenerate
and verify these files with `scripts/regenerate-zinder-proto.sh` from the
Zinder repository root.

To generate bindings without a repository checkout, use the
[versioned protobuf contract archive](https://github.com/ZcashFoundation/zinder/blob/main/docs/reference/integration-surfaces.md#generate-bindings-from-a-pinned-release)
from a Zinder release. The archive includes a standalone Buf workspace and
the vendored schemas with their licenses and upstream pins.

Wallet and application authors should normally use `zinder-client` instead.
Use this crate directly when implementing a gRPC server, a non-SDK transport
adapter, reflection, or protocol compatibility tooling.

Native Zinder schemas evolve compatibly within their versioned package:
additive fields use new tags, removed fields reserve their tags and names, and
breaking wire changes require a new versioned package. The vendored
lightwalletd and Zebra bindings follow the exact upstream source commits
recorded under `proto/compat/lightwalletd/` and `proto/external/zebra/`;
updating either binding requires moving its pin and regenerating the checked-in
artifacts together.
