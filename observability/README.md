# Local Observability Smoke

This directory contains the local Prometheus and Grafana stack used to inspect
Zinder metrics while the binaries run on the host. The compose file intentionally
does not containerize `zinder-ingest`, `zinder-projector`, `zinder-query`, or
`zinder-compat-lightwalletd`: the smoke path should exercise the same Cargo-built
binaries that developers use during T3 live testing.

## What It Proves

`scripts/observability-smoke.sh run` performs an end-to-end checkpoint smoke
against the selected local upstream node:

1. Reads the selected Zebra node tip.
2. Bulk-catches-up a fresh store from a checkpoint.
3. Records that restore is blocked because a coherent canonical-plus-wallet
   bundle restore is not implemented.
4. Starts `zinder-ingest`, `zinder-projector`, `zinder-query`, and
   `zinder-compat-lightwalletd` with `/metrics` endpoints.
5. Requires all four runtimes' `/readyz` responses before sending traffic.
6. Optionally mines explicitly requested regtest blocks so the live ingest
   writer path records a commit after startup. Node mutation is disabled by
   default.
7. Calls native `WalletQuery` and lightwalletd-compatible `CompactTxStreamer`
   APIs with `grpcurl`.
8. Fails if Prometheus reports a traffic-blocking readiness cause or any of the
   4 service scrape targets is unavailable.
9. Archives JSON readiness samples, raw metrics, process logs, exact writer
   fences, native `ServerInfo`, `GetLightdInfo`, the Zebra version, and the Git revision under
   `.tmp/observability/reports/<run-id>-evidence`.

This is a local observability smoke, not a benchmark. It proves the metrics are
emitted, scrapeable, and usable for bottleneck investigation. Use the T2 perf
tests and `scripts/observability-smoke.sh calibrate` for latency budgets.

## Prerequisites

The defaults match the Z3 local regtest sidecar:

```bash
ZINDER_OBSERVABILITY_NODE_ADDR=http://127.0.0.1:39232
ZINDER_OBSERVABILITY_NODE_AUTH_USERNAME=zebra
ZINDER_OBSERVABILITY_NODE_AUTH_PASSWORD=zebra
```

Required commands:

- `cargo`
- `curl`
- `docker`
- `grpcurl`
- `jq`
- `python3`
- `ps`

## Run

```bash
scripts/observability-smoke.sh run
```

To exercise the advancing 3-process topology against regtest, enable the
destructive certification phases explicitly:

```bash
ZINDER_OBSERVABILITY_CERTIFY_TOPOLOGY=1 \
scripts/observability-smoke.sh run
```

The opt-in path pauses the projector while Zebra advances beyond the configured
compatibility lag threshold, requires the typed `replica_lagging` readiness
cause, resumes the projector, and waits for wallet-serving admission recovery. It then
restarts compatibility, query, projector, and ingest one at a time, requiring the
complete readiness chain after each restart, before invalidating 1 regtest
block and mining a 2-block replacement branch. The pass condition requires both
the node and compatibility `GetBlock` to expose a different hash at the
invalidated height, and requires the authenticated writer fence to advance onto
the replacement branch.

Before any mutation, the script checks the live node rather than trusting only
the configured network name. Zebra uses the BIP70 chain name `test` for both
testnet and regtest, so the check also requires the default regtest activation
fingerprint, where every advertised upgrade activates at height 1, plus every
RPC used by the certification. The script refuses mutation on a mismatched
node, and refuses complete-topology certification on `calibrate`.

The script leaves the services running so dashboards remain inspectable:

- Prometheus: <http://127.0.0.1:9095>
- Grafana: <http://127.0.0.1:3002>
- Ingest metrics: <http://127.0.0.1:9190/metrics>
- Projector metrics: <http://127.0.0.1:9194/metrics>
- Compat metrics: <http://127.0.0.1:9192/metrics>

Grafana uses `admin/admin` by default. Override with
`ZINDER_GRAFANA_ADMIN_PASSWORD` when needed.

To print the current evidence again:

```bash
scripts/observability-smoke.sh snapshot
```

To stop the local services and compose stack:

```bash
scripts/observability-smoke.sh stop
```

## Public-Network Smokes

Use public-network smokes when a synced local node is available. Set
`ZINDER_OBSERVABILITY_GENERATE_BLOCKS=0` because public networks cannot mine
ad-hoc blocks:

```bash
AUTH="$(docker exec z3_zebra sh -lc 'cat /var/run/auth/.cookie')"
ZINDER_OBSERVABILITY_NETWORK=zcash-testnet \
ZINDER_OBSERVABILITY_NODE_ADDR=http://127.0.0.1:18232 \
ZINDER_OBSERVABILITY_NODE_AUTH_USERNAME="${AUTH%%:*}" \
ZINDER_OBSERVABILITY_NODE_AUTH_PASSWORD="${AUTH#*:}" \
ZINDER_OBSERVABILITY_GENERATE_BLOCKS=0 \
ZINDER_OBSERVABILITY_BULK_CATCHUP_BLOCKS=100 \
scripts/observability-smoke.sh run
```

```bash
AUTH="$(docker exec z3_mainnet_observability_zebra sh -lc 'cat /var/run/auth/.cookie')"
ZINDER_OBSERVABILITY_NETWORK=zcash-mainnet \
ZINDER_OBSERVABILITY_NODE_ADDR=http://127.0.0.1:29232 \
ZINDER_OBSERVABILITY_NODE_AUTH_USERNAME="${AUTH%%:*}" \
ZINDER_OBSERVABILITY_NODE_AUTH_PASSWORD="${AUTH#*:}" \
ZINDER_OBSERVABILITY_GENERATE_BLOCKS=0 \
ZINDER_OBSERVABILITY_BULK_CATCHUP_BLOCKS=1000 \
scripts/observability-smoke.sh run
```

## Calibration

`calibrate` repeats the full smoke and writes an aggregate baseline with P50,
P95, P99, and worst-case values:

```bash
ZINDER_OBSERVABILITY_NETWORK=zcash-mainnet \
ZINDER_OBSERVABILITY_NODE_ADDR=http://127.0.0.1:29232 \
ZINDER_OBSERVABILITY_NODE_AUTH_USERNAME="${AUTH%%:*}" \
ZINDER_OBSERVABILITY_NODE_AUTH_PASSWORD="${AUTH#*:}" \
ZINDER_OBSERVABILITY_GENERATE_BLOCKS=0 \
ZINDER_OBSERVABILITY_BULK_CATCHUP_BLOCKS=1000 \
ZINDER_OBSERVABILITY_RUNS=6 \
scripts/observability-smoke.sh calibrate
```

Reports are written to:

- `.tmp/observability/reports/latest-readiness.json`
- `.tmp/observability/reports/latest-readiness.md`
- `.tmp/observability/reports/latest-calibration.json`
- `.tmp/observability/reports/latest-calibration.md`
- `.tmp/observability/reports/<run-id>-evidence/manifest.json`

The evidence directory also contains every readiness transition sampled during
lag, restart, and reorg phases, the remote mutation preflight, compact blocks
before and after reorg, and writer fences before and after controlled changes. A
failed certification can leave partial evidence in that directory and the live
process logs under `.tmp/observability/logs`.

The live smoke proves process and network lifecycle behavior. The deterministic
`atomic_publication_keeps_a_retired_generation_until_every_request_arc_drains`
test remains the gate for an in-flight request retaining its original storage handles
while a replacement pair is published; the shell smoke does not manufacture a delayed
wallet request to duplicate that proof.

## Tunables

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `ZINDER_OBSERVABILITY_NETWORK` | `zcash-regtest` | Network written to service configs. |
| `ZINDER_OBSERVABILITY_BULK_CATCHUP_BLOCKS` | `50` | Blocks ingested after the checkpoint. |
| `ZINDER_OBSERVABILITY_GENERATE_BLOCKS` | `0` | Explicitly requested regtest blocks to mine after the ingest loop reaches the `TipFollow` phase. |
| `ZINDER_OBSERVABILITY_CERTIFY_TOPOLOGY` | `0` | Set `1` on the `run` command to enable regtest-only lag, restart, and reorg certification. |
| `ZINDER_OBSERVABILITY_COMPAT_REPLICA_LAG_THRESHOLD_CHAIN_EPOCHS` | `4` | Compatibility readiness threshold written to the generated config. |
| `ZINDER_OBSERVABILITY_CERTIFICATION_LAG_BLOCKS` | Threshold plus `1` | Blocks mined while the projector is suspended; this must exceed the compatibility lag threshold. |
| `ZINDER_OBSERVABILITY_WORK_DIR` | `.tmp/observability` | Absolute harness-owned directory for generated state, configs, logs, and evidence. |
| `ZINDER_OBSERVABILITY_RESET` | `1` | Reset `.tmp/observability` before a run. |
| `ZINDER_OBSERVABILITY_RUNS` | `5` | Number of smoke repetitions for `calibrate`. |
| `ZINDER_PROMETHEUS_PORT` | `9095` | Host Prometheus port. |
| `ZINDER_GRAFANA_PORT` | `3002` | Host Grafana port. |
| `ZINDER_OBSERVABILITY_INGEST_OPS_ADDR` | `0.0.0.0:9190` | Ingest `/metrics` bind address. |
| `ZINDER_OBSERVABILITY_PROJECTOR_OPS_ADDR` | `0.0.0.0:9194` | Projector `/metrics` bind address. |
| `ZINDER_OBSERVABILITY_COMPAT_OPS_ADDR` | `0.0.0.0:9192` | Compat `/metrics` bind address. |

The script writes generated configs and logs under `.tmp/observability`, which
is ignored by Git. It strips `ZINDER_OBSERVABILITY_*` variables before launching
Zinder binaries, so harness-control values cannot be mistaken for production
configuration by the shared `ZINDER_*` config loader.

Reset is allowed only for an absent or empty absolute work directory, or one
already marked as owned by this harness. A non-empty unmarked override fails
closed before any child path is removed; do not point the harness at a live
Zinder store or a shared operator directory.

## Expected Signals

The smoke should produce samples for:

- `zinder_node_request_total`
- `zinder_readiness_state`
- `zinder_readiness_sync_lag_blocks`
- `zinder_readiness_replica_lag_chain_epochs`
- `zinder_ingest_commit_duration_seconds_count`
- `zinder_ingest_writer_chain_epoch_id`
- `zinder_ingest_writer_status_request_total`
- `zinder_compat_lightwalletd_wallet_serving_pair_publisher_publications_total`
- `zinder_compat_lightwalletd_wallet_serving_pair_publisher_convergence_total`
- `zinder_compat_lightwalletd_wallet_serving_pair_publisher_teardown_duration_seconds`
- `zinder_compat_lightwalletd_wallet_serving_pair_publisher_replica_lag_chain_epochs`
- `zinder_compat_lightwalletd_writer_status_total`
- `zinder_store_read_duration_seconds_count`
- `zinder_store_visibility_seek_total`
- `zinder_store_rocksdb_property`

When block generation remains at its default of `0`, the ingest process still
runs and node-poll metrics remain visible, but the live
`zinder_ingest_commit_duration_seconds_count` sample may stay absent until a new
block arrives.

Restore is deliberately not exercised: no coherent canonical-plus-wallet
bundle restore exists. The readiness report records that blocked boundary
directly and the smoke does not synthesize a restore result from an incomplete
store checkpoint.

## Alert Rules

Prometheus loads `observability/prometheus/rules/zinder-readiness.yml`. The
local rules cover:

- scrape targets down
- traffic-blocking readiness causes
- traffic-safe readiness warnings
- a wallet-serving pair serving past its last writer attestation
- secondary replica lag
- node RPC errors
- wallet-query p95 above 250ms
- store-read p95 above 50ms
- RocksDB pending compaction bytes above 256MiB
- more than four RocksDB compactions running for 15 minutes

## Grafana Overview

The bundled dashboard highlights multi-process storage-access signals that
should make writer, secondary-reader, and replica-lag failures easy to spot:

- `Traffic-Blocking Services`: red when any service reports a readiness cause
  that should fail load-balancer readiness.
- `Storage Access Availability`: red when writer-status serving, writer-status
  fetching, or wallet-serving-pair readiness is unavailable over the last five minutes.
- `Replica Lag`: chain-epoch lag from readiness and secondary catchup.
- `Canonical Writer Chain Epoch`: authenticated canonical-writer progress.
- `Secondary Catchup P95`: catchup latency for the compatibility wallet-serving pair.
- `Storage Access Error Rate`: writer-status and wallet-serving-pair catchup error rates.
  A flat zero line is the healthy state.
