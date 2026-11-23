# RocksDB storage lifecycle

This harness measures how long a fresh canonical store and then a fresh wallet
store take to reach `READY` against an already synchronized Testnet Zebra. It
reports the two times separately, then cold-opens both stores and checks that
the wallet store has reached the same canonical chain event. It is a
maintainer procedure.

The harness measures construction. Continuous following, reorg recovery,
query serving, and lightwalletd compatibility are outside its scope. The
canonical runtime tracer in the [testing runbook](../docs/runbooks/testing.md)
covers the real `zinder-ingest` composition, following, and restart.

## Prerequisites

- A synchronized Testnet Zebra from the Z3 stack. The harness joins its Docker
  network (`z3-testnet` by default) and mounts only its cookie volume
  (`z3-testnet-cookie`), read-only. It never mounts the chain volume.
- Docker with at least the CPU and memory limits you choose below.
- A clean checkout of the revision under test.

## Build the image

```bash
docker build \
  --file deploy/Dockerfile \
  --target zinder-bench \
  --tag zinder-bench:local \
  .
```

The runner records the image ID and the full Git revision in the report. It
accepts the default revision only from a clean worktree. If you use a prebuilt
image from another checkout, set `ZINDER_STORAGE_LIFECYCLE_SOFTWARE_REVISION`
to the revision baked into it. Set `ZINDER_STORAGE_LIFECYCLE_IMAGE` to use a
different tag or a digest-pinned reference.

## Run a smoke test

A small fixed tip proves the whole code path in minutes:

```bash
ZINDER_STORAGE_LIFECYCLE_TIP_HEIGHT=10000 \
ZINDER_STORAGE_LIFECYCLE_PROJECT_NAME=zinder-storage-lifecycle-smoke \
ZINDER_STORAGE_LIFECYCLE_EVIDENCE_PATH="$PWD/.tmp/rocksdb-storage-lifecycle-smoke" \
  scripts/run-rocksdb-storage-lifecycle.sh
```

A smoke result shows that the code path works. Measure to the current tip for
timing.

## Measure to the current tip

```bash
scripts/run-rocksdb-storage-lifecycle.sh
```

The runner reads Zebra's height just before construction starts and freezes it
for the whole run. The result covers height 1 through that tip, not blocks
mined during the run.

The report and container resource evidence go to
`.tmp/rocksdb-storage-lifecycle-evidence` by default. Read the two acceptance
times with:

```bash
jq '{
  fixed_tip: .source.fixed_build_tip,
  canonical_storage_ready: .acceptance.canonical_storage_ready.wall_clock_seconds,
  wallet_storage_ready: .acceptance.wallet_storage_ready.wall_clock_seconds,
  total: .phase_durations.total_seconds
}' .tmp/rocksdb-storage-lifecycle-evidence/rocksdb-storage-lifecycle.json
```

The report also breaks the run into phases: canonical loading, canonical
validation and publication, wallet scanning and sorting, wallet SST ingestion,
wallet validation and publication, and the final cold admission. The resource
observer records the cgroup memory peak and sampled peak disk use.

## Choose resource limits

The default envelope is 10 CPU cores and 10 GiB of memory. To measure another
machine or deployment class, set these together:

- `ZINDER_STORAGE_LIFECYCLE_CPU_LIMIT_CORES`
- `ZINDER_STORAGE_LIFECYCLE_MEMORY_LIMIT_BYTES`
- `ZINDER_STORAGE_LIFECYCLE_STORAGE_CLASS`

The runner refuses limits above Docker's advertised capacity before it creates
containers. It derives source and preparation concurrency from the envelope and
`ZINDER_NODE_MAX_RESPONSE_BYTES`. The CPU limit is a quota, not a reservation:
Zebra shares the Docker engine and is not measured.

Canonical construction uses `cadence-and-tip` tree-state checkpoint coverage by
default, matching fresh ingest with `explicit` coverage. Set
`ZINDER_STORAGE_LIFECYCLE_TREE_STATE_CHECKPOINT_COVERAGE=every-block` to
measure dense construction before READY. Report format
version 5 records the chosen coverage in
`resource_limits.tree_state_checkpoint_coverage`, the canonical publication mode
in `resource_limits.canonical_publication_mode` (`trusted-fresh` or
`cold-certified`), and cross-checks coverage against
`canonical_storage_ready.tree_state_checkpoint_count`.

The default run publishes from the fresh writer's authenticated construction
evidence, as ingest does. For a separate run that measures a full prepublication
scan, set `ZINDER_STORAGE_LIFECYCLE_CANONICAL_PUBLICATION_MODE=cold-certified`.
The report records this choice in `resource_limits.canonical_publication_mode`.
For `trusted-fresh`, preparation counts toward `canonical_ready_publication_seconds`
and `canonical_cold_validation_seconds` is zero. Both modes reopen the published
store before the canonical storage-ready timer stops.

## State and cleanup

Before each run, the runner checks the rendered Compose model. It refuses an
unsafe project name, a network other than Testnet, a writable cookie mount, or
any reference to the Zebra chain volume. Only then does it run
`docker compose down --volumes` for the harness project.

The finished canonical and wallet volumes stay available for diagnosis until
the next run deletes them. To delete them yourself after the containers have
stopped:

```bash
docker volume rm \
  zinder-storage-lifecycle-test_canonical_state \
  zinder-storage-lifecycle-test_wallet_state
```

## Acceptance

After the container exits, the runner calls
`scripts/validate-rocksdb-storage-lifecycle-report.sh`. The validator is
independent of the Rust code and pins the exact report shape and contract
versions. It rejects the evidence unless:

- the store schema, replay, digest, and report versions match the values it
  pins (report format version 5 for current evidence, with a separate exact
  version 4 branch for archived reports when no version 5 expectation is
  passed);
- canonical `READY` covers heights 1 through the fixed tip, persists the exact
  tree-state checkpoint count required by
  `resource_limits.tree_state_checkpoint_coverage` (and records zero
  `canonical_cold_validation_seconds` under `trusted-fresh` publication in
  version 5), and survives a cold reopen;
- wallet `READY` has the same epoch, event, tip, sequence digest, block count,
  and transaction count, and both stores pass the final cold admission;
- wallet row counts, projection digest, UTXO commitment, and undo depth are
  consistent;
- the build sorters stay within their memory and temporary-file ceilings, the
  reorg suffixes stay within their memory ceiling, and historical prevout and
  cold-validation random reads are zero;
- phase durations are non-negative and ordered;
- the report names the fixed tip, image ID, full revision, trial ID, resource
  envelope, and Testnet source; and
- the resource observer covers the whole report window from a private cgroup
  v2 namespace, the child exited with status 0, and both the cgroup and process
  memory peaks are within the limit.
