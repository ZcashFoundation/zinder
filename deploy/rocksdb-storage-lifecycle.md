# RocksDB storage lifecycle

> **Maintainer harness.** This procedure measures fresh canonical and wallet store construction against a synchronized Testnet Zebra node. Operators deploying Zinder should follow [Deploying Zinder](../docs/runbooks/deploying.md).

This harness measures how long a fresh canonical store and then a fresh wallet store take to reach `READY`, then cold-opens both stores to verify that the wallet store reached the same canonical chain event. Continuous following, reorg recovery, query serving, and lightwalletd compatibility are outside its scope; the canonical runtime tracer in the [testing runbook](../docs/runbooks/testing.md) covers `zinder-ingest` composition, following, and restart.

## Prerequisites

- Synchronized Testnet Zebra from the Z3 stack (`z3-testnet` network and read-only `z3-testnet-cookie` volume; never mount the chain volume).
- Docker with the chosen CPU and memory limits.
- Clean checkout of the revision under test.

## Build the image

```bash
docker build \
  --file deploy/Dockerfile \
  --target zinder-bench \
  --tag zinder-bench:local \
  .
```

The runner records the image ID and full Git revision and accepts the default revision only from a clean worktree. Set `ZINDER_STORAGE_LIFECYCLE_SOFTWARE_REVISION` for a prebuilt image from another checkout, and `ZINDER_STORAGE_LIFECYCLE_IMAGE` for a custom tag or digest.

## Run a smoke test

A small fixed tip exercises the full path in minutes:

```bash
ZINDER_STORAGE_LIFECYCLE_TIP_HEIGHT=10000 \
ZINDER_STORAGE_LIFECYCLE_PROJECT_NAME=zinder-storage-lifecycle-smoke \
ZINDER_STORAGE_LIFECYCLE_EVIDENCE_PATH="$PWD/.tmp/rocksdb-storage-lifecycle-smoke" \
  scripts/run-rocksdb-storage-lifecycle.sh
```

## Measure to the current tip

```bash
scripts/run-rocksdb-storage-lifecycle.sh
```

The runner freezes Zebra's tip height before construction starts and writes the report and resource evidence to `.tmp/rocksdb-storage-lifecycle-evidence` by default:

```bash
jq '{
  fixed_tip: .source.fixed_build_tip,
  canonical_storage_ready: .acceptance.canonical_storage_ready.wall_clock_seconds,
  wallet_storage_ready: .acceptance.wallet_storage_ready.wall_clock_seconds,
  total: .phase_durations.total_seconds
}' .tmp/rocksdb-storage-lifecycle-evidence/rocksdb-storage-lifecycle.json
```

The report records per-phase timings (canonical loading, validation/publication; wallet scanning/sorting, SST ingestion, validation/publication; final cold admission) plus cgroup memory peak and sampled peak disk use.

## Choose resource limits

The default envelope is 10 CPU cores and 10 GiB of memory. Override together:

- `ZINDER_STORAGE_LIFECYCLE_CPU_LIMIT_CORES`
- `ZINDER_STORAGE_LIFECYCLE_MEMORY_LIMIT_BYTES`
- `ZINDER_STORAGE_LIFECYCLE_STORAGE_CLASS`

The runner rejects limits above Docker's advertised capacity and derives source and preparation concurrency from the envelope and `ZINDER_NODE_MAX_RESPONSE_BYTES`. The CPU limit is a quota, not a reservation (Zebra shares the Docker engine and is not measured).

Canonical construction uses `cadence-and-tip` tree-state checkpoint coverage by default, matching fresh ingest with `explicit` coverage; set `ZINDER_STORAGE_LIFECYCLE_TREE_STATE_CHECKPOINT_COVERAGE=every-block` to measure dense construction before `READY`. Publication uses the fresh writer's authenticated evidence by default (`trusted-fresh`, where `canonical_cold_validation_seconds` is zero); set `ZINDER_STORAGE_LIFECYCLE_CANONICAL_PUBLICATION_MODE=cold-certified` to measure a full prepublication scan. Both modes reopen the published store before stopping the timer, and report format version 5 records both choices in `resource_limits`.

## State and cleanup

Before each run, the runner verifies the rendered Compose model (Testnet only, safe project name, read-only cookie mount, no Zebra chain volume) and runs `docker compose down --volumes` for the harness project. Finished volumes stay available for diagnosis until the next run or manual removal:

```bash
docker volume rm \
  zinder-storage-lifecycle-test_canonical_state \
  zinder-storage-lifecycle-test_wallet_state
```

## Acceptance

The runner validates the report with `scripts/validate-rocksdb-storage-lifecycle-report.sh`, rejecting evidence unless:

- schema, replay, digest, and report versions match (format v5 for current runs, with a v4 branch for archived reports);
- canonical `READY` covers heights `1..=tip`, matches the required tree-state checkpoint count (with zero `canonical_cold_validation_seconds` under `trusted-fresh`), and survives a cold reopen;
- wallet `READY` matches the canonical epoch, event, tip, sequence digest, block count, and transaction count, and both pass final cold admission;
- wallet row counts, projection digest, UTXO commitment, and undo depth agree;
- build sorters and reorg suffixes stay within memory/temp-file ceilings with zero historical prevout or cold-validation random reads;
- phase durations are non-negative and ordered, and provenance fields are set;
- the cgroup v2 resource observer covers the full window with child exit `0` and memory peaks within limit.
