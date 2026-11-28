# ADR-0024: Verify edge snapshots before publishing canonical state

Status: Accepted

## Context

A regional ingest operator can start from a publisher's canonical checkpoint and
follow the remaining blocks from a configured Zcash validator. The bundle
crosses a distribution boundary and carries a physical copy of a single-writer
store. Admission must bind that copy to the configured chain and give each
recipient independent cursor authentication and process state.

## Decision

### Verified physical checkpoint

Export a quiesced READY canonical store whose visible and settled tips name the
same block at a height divisible by 4,096. A physical checkpoint captures the
entire visible store. Prepare a publisher store at the desired boundary before
exporting it. The store must retain each terminal completed subtree root at
that boundary. The publisher keeps its own store and credentials.

`zinder-edge-snapshot.v1` contains:

- `manifest.v1.json`, a UTF-8 JSON object with `format`, `anchors`, and `files`.
  Anchors carry the canonical schema version, network ID, genesis hash,
  v1 activation-table fingerprint, raw blob retention,
  reorg window, first retained block and snapshot block identities, and three
  tree entries in Sapling, Orchard, and Ironwood order. Each active tree entry
  records its size, final root, and terminal completed subtree, if any. A
  terminal subtree includes its index, root, and completing block identity.
  Inactive trees are null. Hashes and digests are arrays of 32 byte values,
  using the canonical store byte order for block hashes and the typed source
  byte order for tree roots.

- `manifest.v1.sig`, exactly 64 raw Ed25519 signature bytes. The publisher
  signs the ASCII format identifier followed by a NUL byte and the exact JSON
  bytes on disk. The receiver checks the signature against at least one
  operator-configured raw 32-byte public key.
- `canonical-rocksdb/`, the RocksDB checkpoint, canonical construction
  manifest (`canonical-construction-manifest.v2.json`), pinned external
  settled segment manifest (`segments-manifest.json`), and settled segment
  files (`segments/{shielded,supplement,raw_block}/`) required for READY
  admission. Export clears projection build leases and durable mempool events,
  replaces the publisher's cursor key in the copy, and compacts the affected
  families before checkpointing the export.

Before export begins, the writer is quiesced at a boundary divisible by 4,096
blocks (`settled_tip_height == visible_tip_height`). Export refuses any source
workspace that aliases or overlaps `--output-dir` or its temporary staging
directory.

Each file record gives a slash-separated relative path, byte length, and
SHA-256 digest. The receiver refuses unknown fields, duplicate or conflicting
paths, traversal, symlinks, unexpected checkpoint names, and missing or
unlisted local entries. It authenticates the manifest before walking local
entries and accepts at most 65,536 files and directories per bundle, including
the manifest and signature. An HTTP(S) source receives requests for the
manifest and signature first. The receiver checks the signed store
configuration before requesting any payload files and verifies each file's
length and digest while streaming it to disk. An HTTP transfer retrieves
complete files and needs space for both the download and the private staged
copy.

The receiver refuses a snapshot whose first retained block is above the first
block its writer requires, which is block 1 without a checkpoint override and
the block after the override otherwise. Manifest preflight enforces this before
any payload is copied or downloaded, and attestation enforces it again. A
writer started with the same `checkpoint_height` applies the same rule, so
normal writer startup never rejects a published bootstrap.

After cold admission, the receiver compares the signed anchors with evidence
read back from the store. The configured `NodeSource` must also attest:

1. The genesis hash and exact network-upgrade activation table.
2. The best-chain identities at the snapshot height and the first retained
   block height. The node's tip must be at least the configured canonical
   reorg window ahead of the snapshot.
3. The final note-commitment roots and tree sizes at the snapshot block for
   Sapling, Orchard, and Ironwood.
4. The terminal completed subtree root and completing block for each pool
   that has completed a subtree at that height.
5. The block hashes and ordered transaction IDs for 32 uniformly sampled
   distinct retained blocks by default, plus the snapshot boundary. The OS
   CSPRNG selects the sample after staging. For a shorter retained interval,
   every retained block is sampled.

The bootstrap checks that the initially observed validator tip remains on its
best chain after the observations. The signature establishes publisher
provenance and full bundle byte integrity. Validator attestation checks the
consensus anchors and samples history. A trusted publisher remains responsible
for the accuracy of historical rows outside those checks.

### Shared HTTPS settled-segment pool

The shipped `zinder-edge-snapshot.v1` path above retrieves whole files. The
ratified distribution contract uses one public, versioned catalog and
content-addressed object layout on static HTTPS. Published objects are
immutable Schema v2 settled segments (`ZSHRNG02`, `ZSTSUP01`, `ZSBLKB01`).
Within the versioned layout, the catalog and objects have immutable
addresses derived from their SHA-256 digests. Each segment entry binds
its format, height interval, byte length, and whole-segment SHA-256 digest.
For `ZSHRNG02`, this digest covers all bytes preceding the trailer and
is recorded in the trailer itself. The signed catalog also binds each
`ZSHRNG02` entry's `index_sha256` pin. A partial HTTPS `Range` reader
checks the trailer against the whole-segment digest and authenticates the
header, raw row index, and trailer against `index_sha256` before reading
row slices. The authenticated index supplies the row digests, so a reader
can verify those slices without fetching the entire object.

The catalog is a public distribution contract separate from the store's
internal `segments-manifest.json`. Track the RocksDB checkpoint and indexes
separately per `(network, schema_version, stored_set)`. Regional seed hosts
reuse the same pool and run their own single writer under
[ADR-0004](0004-storage-and-deployment-topologies.md).
[#478](https://github.com/ZcashFoundation/zinder/issues/478) and
[#714](https://github.com/ZcashFoundation/zinder/issues/714).

An expiring, domain-separated Ed25519-signed availability record binds the
current catalog digest and revision for each track. Verify it against an
explicit `--public-key` allowlist. For routine rotation, overlap the old and
new public keys, then remove the retired key. Revalidate availability against
the HTTPS origin before selecting a catalog and again before publication.
Reject expired, withdrawn, or superseded records and revisions older than one
already accepted for the track. Withdrawing the current record blocks new
admission at once. Expiration bounds the lifetime of a previously signed
record.

Stream directly into private staging. Resume a partial object with
`Range` and `If-Range` using its strong `ETag`. Append only a matching
`206 Partial Content` response whose `Content-Range` starts at the staged
offset and agrees with the expected length. If the origin does not support
`206` or the representation changes, retain verified complete files and
restart the partial file from byte zero. Verify signed length and digest
before admitting each completed object.
[RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#section-14).

Physical checkpoint admission, readback, and validator RPC attestation still
precede writer `READY` and publication. Each host generates a fresh
`cursor_auth_key`. Attestation excludes P2P-only `NODE_TREE_ROOTS` under
[ADR-0006](0006-node-source-boundary.md). Wallet HTTP range downloads share
the object layout and use the TLS-authenticated Zinder length and digest pins in
[ADR-0014](0014-consumer-neutral-wallet-data-plane.md).
[#697](https://github.com/ZcashFoundation/zinder/issues/697).

## Publication and recovery

Bootstrap prepares the store beneath `<target>.bootstrap.staging`, with the
stage accessible only to the current operator. The canonical destination must
be absent or empty, and its resolved path must be separate from the private
workspaces. Reservation checks device and inode identity as well as paths,
including aliases through bind mounts. Each workspace holds a private marker.
Recursive cleanup starts only after the workspace creates its marker
exclusively. Before that, a reservation failure can remove only an empty
directory with the reserved identity. Cleanup traverses open directory
descriptors and verifies ownership before unlinking the directory. Bootstrap
reserves workspaces before starting an HTTP transfer. Before publication it
clears inherited projection build leases and mempool events, generates a fresh
`cursor_auth_key` using the OS CSPRNG, and runs normal READY admission on the
resulting store. Bootstrap pins the admitted canonical directory before
attestation. It rechecks workspace and staged directory identities before
descriptor-relative publication. The receiver syncs files and directories,
publishes the canonical directory by atomic no-replace rename, and syncs the
destination parent directory. Normal ingest resumes ownership and follows from
the snapshot tip.

An ordinary failure before canonical publication cleans up the stage. An
interrupted process can leave private staging or download directories. After
confirming no snapshot process is active, the operator can remove these
directories and retry. If the parent directory sync fails after canonical
rename, inspect the visible target and rerun normal store admission before
following. An existing canonical primary must be preserved.

A bundle initializes a separate `rocksdb` writer with its own
storage ownership. Capacity, recovery times, and shared storage topologies
require separate qualification.

## Operator commands

With the normal ingest config and the source writer stopped, run
`zinder-ingest export-edge-snapshot --output-dir BUNDLE --signing-key KEY.der`.

Point the receiving configuration at the empty canonical destination and
its validator. Run `zinder-ingest bootstrap-from-snapshot --source BUNDLE
--public-key HEX`, using a local directory or an HTTP(S) base URL for
`BUNDLE`. Repeat `--public-key` during a publisher key transition.
`--spot-check-blocks` sets a positive sample count.

## Evidence

- Writer attestation: Judgment. Publisher signatures authenticate bytes,
  and an admitted validator attests to consensus anchors before publication.
  A signed bundle that passes these checks with an incorrect attested anchor
  would reopen admission.
- Shared catalog and independent checkpoint tracks: Judgment. Immutable
  content can be shared across consumers, while each regional host needs
  its own admitted writer. Regional recovery that cannot meet its objective
  using the shared objects would reopen the split.
- Partial segment verification: Judgment. The existing `ZSHRNG02` decoder
  checks a trusted `index_sha256` over the header, raw index, and trailer.
  A reader cannot recompute the whole-segment digest from those ranges.
  Reopen the catalog contract if the pins cannot authenticate a requested
  slice without a full-object read.
- Availability and rotation: Judgment. Current-origin revalidation,
  expiration, and an explicit trust allowlist keep the catalog revocable
  without a separate certificate revocation service. An expired, withdrawn,
  or superseded catalog accepted by a receiver would reopen the design.
- Transfer semantics: External constraint. RFC 9110 defines conditional
  range requests, and an origin can ignore a range and return the entire
  representation. A complete-file fallback preserves compatibility.
- Direct-to-stage transfer: Judgment. Staging once avoids an extra disk
  copy. Reopen if measured recovery shows the staging layout cannot verify
  and resume safely after interrupted transfers.

## Consequences

- Regional hosts can share settled objects, with per-host checkpoints,
  indexes, credentials, and validator following.
- Range-aware origins save retransmission after an interruption. Simple
  HTTPS origins can still resume at verified file boundaries.
- Operators must keep the active availability record and trust keys current.
  A withdrawn or expired publication cannot seed a new writer.

## Alternatives rejected

- **Publish the internal RocksDB segment manifest.** It would couple
  consumers to an internal store inventory and lifecycle.
- **Add a regional gRPC replication listener.** Regional seeding can
  reuse the same static objects as snapshot bootstrap.
- **Use an indefinite signature or add CRL/OCSP infrastructure.** An
  indefinite signature can outlive withdrawal. An expiring record and
  explicit trust rotation provide withdrawal and rollback protection
  without a separate revocation system.

## Revision history

- 2026-11-28. Added the ratified shared HTTPS segment pool, resumable transfers, and signed availability for regional bootstrap and wallet downloads.
- 2026-11-27. Removed `canonical workload` from `ManifestAnchors` and documented `segments-manifest.json` / `segments/` within `canonical-rocksdb/` for Canonical Schema v2.
- 2026-11-27. Retired the optional `sealed-history/` snapshot sidecar (`--include-sealed-history` / `--sealed-history-output`) after cutting lightwalletd streaming over directly to canonical `ZSHRNG02` segments inside the canonical store.
- 2026-11-24. Renamed topology identifier `rocksdb-single-host` to `rocksdb` to match ADR-0004.
