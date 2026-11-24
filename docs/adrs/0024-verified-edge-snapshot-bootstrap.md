# ADR-0024: Verify edge snapshots before publishing canonical state

Status: Accepted

## Context

A regional ingest operator can start from a publisher's canonical checkpoint and
follow the remaining blocks from a configured Zcash validator. The bundle
crosses a distribution boundary and carries a physical copy of a single-writer
store. Admission must bind that copy to the configured chain and give each
recipient independent cursor authentication and process state.

## Decision

Export a quiesced READY canonical store whose visible and settled tips name the
same block at a height divisible by 4,096. A physical checkpoint captures the
entire visible store. Prepare a publisher store at the desired boundary before
exporting it. The store must retain each terminal completed subtree root at
that boundary. The publisher keeps its own store and credentials.

`zinder-edge-snapshot.v1` contains:

- `manifest.v1.json`, a UTF-8 JSON object with `format`, `anchors`, and `files`.
  Anchors carry the canonical schema version, network ID, genesis hash,
  v1 activation-table fingerprint,
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
- `canonical-rocksdb/`, the RocksDB checkpoint and canonical construction
  manifest required for READY admission. Export clears projection build leases
  and durable mempool events, replaces the publisher's cursor key in the copy,
  and compacts the affected families before checkpointing the export.
- Optional opaque files beneath `sealed-history/`. The signature attests
  their paths, lengths, and digests. The compatibility runtime retains
  responsibility for admitting their format and chain linkage.

Each file record gives a slash-separated relative path, byte length, and
SHA-256 digest. The receiver refuses unknown fields, duplicate or conflicting
paths, traversal, symlinks, unexpected checkpoint names, and missing or
unlisted local files. HTTP(S) retrieves only the signed inventory. The
receiver verifies every listed file while streaming its bytes to disk. An
HTTP transfer needs space for the download and the private staged copy. Each
request retrieves one complete file. HTTP transport can use HTTPS for confidentiality and endpoint authentication.

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

## Publication and recovery

Bootstrap prepares the store beneath `<target>.bootstrap.staging`, with the
stage accessible only to the current operator. It refuses an occupied target.
Before publication it clears inherited projection build leases and mempool
events, generates a fresh `cursor_auth_key` using the OS CSPRNG, and runs
normal READY admission on the resulting store. The receiver syncs files and
directories, publishes the canonical directory by atomic no-replace rename,
and syncs the destination parent directory. Normal ingest resumes ownership
and follows from the snapshot tip.

When a sealed-history output is requested, the receiver verifies it with the
rest of the bundle and publishes it before the canonical rename. The two
destinations have separate publication points. A crash between renames can
leave verified opaque files at their destination. An ordinary failure before
canonical publication cleans up the stage and any sidecar published by that run.
An interrupted process can leave private staging or download directories.
After confirming no snapshot process is active, the operator can remove these
directories and retry. If the parent directory sync fails after canonical
rename, inspect the visible target and rerun normal store admission before
following. An existing canonical primary must be preserved.

A bundle initializes a separate `rocksdb-single-host` writer with its own
storage ownership. Capacity, recovery times, and shared storage topologies
require separate qualification.

## Operator commands

With the normal ingest config and the source writer stopped, run
`zinder-ingest export-edge-snapshot --output-dir BUNDLE --signing-key KEY.der`.
Pass `--include-sealed-history DIR` to package opaque files from that directory.

Point the receiving configuration at the empty canonical destination and
its validator. Run `zinder-ingest bootstrap-from-snapshot --source BUNDLE
--public-key HEX`, using a local directory or an HTTP(S) base URL for
`BUNDLE`. Repeat `--public-key` during a publisher key transition.
`--spot-check-blocks` sets a positive sample count, and
`--sealed-history-output DIR` publishes the verified optional files.
