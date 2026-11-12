# RocksDB Storage Lifecycle

This harness measures fresh canonical-store construction followed by fresh
wallet-store construction against an already-synchronized Zebra. It reports
the time to canonical `READY` and wallet `READY` as separate acceptance
boundaries, then independently cold-opens both stores and verifies that the
wallet source fence equals the canonical fence. It does not certify continuous
following, reorg recovery, query serving, lightwalletd compatibility, or a
PostgreSQL deployment.

The Compose topology joins Zebra's existing external network and mounts only
its authentication cookie read-only. It never declares or mounts Zebra's chain
volume. Canonical and wallet data live in disposable, project-scoped volumes;
the runner deletes those volumes before every measurement while preserving the
Zebra state that makes repeated tests practical.

## Build

Build the exact revision under test with the existing benchmark image target:

```bash
docker build \
  --file deploy/Dockerfile \
  --target zinder-bench \
  --tag zinder-bench:local \
  .
```

The runner resolves `zinder-bench:local` to its immutable Docker image ID and
records that ID in the report. It also records a full Git object ID. The default
revision is accepted only from a clean worktree; set
`ZINDER_STORAGE_LIFECYCLE_SOFTWARE_REVISION` to the exact revision baked into a
prebuilt image when the local checkout is unrelated. Set
`ZINDER_STORAGE_LIFECYCLE_IMAGE` when using a different local tag or a
digest-pinned registry reference.

## Smoke test

Use a small fixed tip to prove the complete deployed code path before spending
hours on a current-tip measurement:

```bash
ZINDER_STORAGE_LIFECYCLE_TIP_HEIGHT=10000 \
ZINDER_STORAGE_LIFECYCLE_PROJECT_NAME=zinder-storage-lifecycle-smoke \
ZINDER_STORAGE_LIFECYCLE_EVIDENCE_PATH="$PWD/.tmp/rocksdb-storage-lifecycle-smoke" \
  scripts/run-rocksdb-storage-lifecycle.sh
```

This is functional evidence only. A small-tip result is not a performance
projection for the current chain.

## Current-tip measurement

Run without `ZINDER_STORAGE_LIFECYCLE_TIP_HEIGHT` to capture the synchronized
testnet Zebra height immediately before the measurement:

```bash
scripts/run-rocksdb-storage-lifecycle.sh
```

The report and independent container-resource evidence are written under
`.tmp/rocksdb-storage-lifecycle-evidence` by default. The report separates
source discovery, canonical source loading, canonical cold validation and
publication, wallet scanning and external sorting, wallet SST ingestion, wallet
cold validation and publication, and final independent cold admission. Wallet
cold validation is a digest and row-count readback of the durable rows against
the construction evidence, and the report records the wall time of each family
scan (the scans overlap, so each is bounded by the cold-validation total). The
resource observer records exact cgroup peak memory and sampled peak disk usage
for the complete process.

The current tip is the Zebra height observed immediately before construction
starts. It is frozen for the entire run, so a node that advances while the test
is running cannot move the acceptance fence. The result proves synchronization
from height 1 through that fixed starting tip; it does not claim that the
completed store also contains blocks Zebra mined during the measurement.

To inspect the two acceptance durations after a successful run:

```bash
jq '{
  fixed_tip: .source.fixed_build_tip,
  canonical_storage_ready: .acceptance.canonical_storage_ready.wall_clock_seconds,
  wallet_storage_ready: .acceptance.wallet_storage_ready.wall_clock_seconds,
  total: .phase_durations.total_seconds
}' .tmp/rocksdb-storage-lifecycle-evidence/rocksdb-storage-lifecycle.json
```

The default local envelope is 10 CPU cores and 10 GiB of memory, leaving Docker
Desktop headroom on the reference development machine. The runner refuses a
limit above Docker's advertised capacity before it creates any containers.
Override `ZINDER_STORAGE_LIFECYCLE_CPU_LIMIT_CORES`,
`ZINDER_STORAGE_LIFECYCLE_MEMORY_LIMIT_BYTES`, and
`ZINDER_STORAGE_LIFECYCLE_STORAGE_CLASS` together when comparing another
machine or deployment class. The lifecycle derives source concurrency,
source/prepare watermarks, and prepare concurrency from that envelope plus
`ZINDER_NODE_MAX_RESPONSE_BYTES`; independent pipeline tuning is intentionally
not part of this certification command. Every report records the resolved
limits, and the validator recomputes them from the requested Compose envelope.
The CPU value is a container quota, not an exclusive reservation; Zebra shares
the Docker engine and remains outside the measured lifecycle cgroup.

## State lifecycle

Every runner invocation first renders and inspects the Compose topology. It
refuses an unsafe project name, a non-testnet network, a writable cookie mount,
or any reference to the configured Zebra chain volume. Only after those checks
does it run `docker compose down --volumes` for the project-scoped harness.

The harness leaves the completed canonical and wallet volumes available for
diagnosis. A subsequent invocation deletes them before starting. After the
runner has removed its containers, delete the default project's two volumes
manually without touching Zebra with:

```bash
docker volume rm \
  zinder-storage-lifecycle-test_canonical_state \
  zinder-storage-lifecycle-test_wallet_state
```

## Acceptance contract

The runner invokes
`scripts/validate-rocksdb-storage-lifecycle-report.sh` after the container
exits. The validator is independent of the Rust report validator and pins the
closed report-format-version-4 JSON shape. It rejects evidence unless all of these conditions
hold:

- canonical physical schema 5, wallet-store and wallet-projection physical
  schema 1, wallet value encoding 2, and replay, block digest, sequence digest,
  and resource-evidence versions 1 are exact; the benchmark report contract is
  version 4;
- canonical `READY` covers the contiguous height-1-through-fixed-tip range,
  authenticates the source checkpoint, and survives a cold reopen;
- wallet `READY` has the same epoch, event, tip, sequence digest, block count,
  and transaction count, and both stores pass the final cold fence admission;
- durable wallet counts, projection digest, UTXO commitment, and retained undo
  depth are internally consistent;
- all three build sorters remain within their declared memory and
  temporary-file ceilings, both reorg suffixes remain within their
  memory ceiling, and historical prevout and cold-validation random reads are
  zero;
- every monotonic phase duration and acceptance boundary is non-negative and
  internally ordered;
- the report carries the expected fixed tip, immutable image ID, full software
  revision, trial ID, runner envelope, and testnet source identity; and
- the separate resource observer covers the whole report window with a private
  cgroup-v2 namespace, exact memory sources, sampled storage, a zero child exit
  status, matching trial identity, and cgroup and process peaks independently
  within the declared memory limit. The validator does not order process
  `VmHWM` against cgroup `memory.peak` because their shared-page accounting is
  not identical. It also does not compare monotonic phase durations with Unix
  timestamp deltas because wall-clock synchronization can move Unix time while
  a run is active; Unix timestamps remain ordered provenance and
  resource-correlation boundaries.

This is a storage-construction acceptance boundary. Query serving, live
following, and consumer-protocol parity require their own tests after these
stores are admitted.

Run the separate [canonical runtime tracer](../docs/runbooks/testing.md#canonical-runtime-tracer)
to certify the real `zinder-ingest` composition, append-only following, source
recovery, and authenticated restart. That gate does not turn this construction
report into service or topology certification.
