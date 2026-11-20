# Local observability smoke

This guide runs Zinder from your checkout against a node you already run, with
Prometheus and Grafana scraping it. Use it to check that metrics, alerts, and
dashboards work after you change them. It is not a benchmark.

The same Prometheus rules and Grafana dashboard ship with the reference
Compose deployment. Metric names and alerts are described in
[Metrics](../docs/reference/metrics.md).

## What it starts

`scripts/observability-smoke.sh run` does the following:

1. Builds `zinder-ingest`, `zinder-projector`, `zinder-query`, and
   `zinder-compat-lightwalletd` with `cargo build`.
2. Starts Prometheus and Grafana from `docker-compose.observability.yml`.
   They load `observability/prometheus/` and `observability/grafana/`.
3. Builds a fresh store from a checkpoint 50 blocks below the node's tip.
4. Starts the four runtimes as host processes and waits until every
   `/readyz` reports ready.
5. Sends native `WalletQuery` and lightwalletd `CompactTxStreamer` requests
   with `grpcurl`.
6. Fails if Prometheus sees a runtime refusing traffic or fewer than four
   scrape targets up.
7. Writes a readiness report and an evidence directory, and leaves
   everything running so you can inspect the dashboards.

The readiness report records restore as `blocked`, because Zinder recovers
by rebuilding from the node, as
[Service operations](../docs/architecture/service-operations.md#recovery)
describes.

To test the shipped Prometheus configs, rules, and Scrape Targets panel with
isolated samples and no node, run `python3 scripts/test-observability-rules.py`.
It checks base Compose, the Explorer overlay, and the local smoke stack using
their configured Prometheus images. A down target and a target that disappears
from discovery must each produce their own alert.

## Prerequisites

- A Zebra node with JSON-RPC enabled. The default is Regtest at
  `http://127.0.0.1:39232` with basic auth `zebra`/`zebra`.
- These commands: `cargo`, `curl`, `docker` with Compose, `grpcurl`, `jq`,
  `python3`, and `ps`.
- Free host ports: 9095 and 3002 for Prometheus and Grafana, 9190, 9192,
  9193, and 9194 for the operational endpoints, and 9067, 9100, and 9102 on
  loopback for gRPC and ingest control.

The operational endpoints bind `0.0.0.0` so the Prometheus container can
scrape them, and the generated configs set `security.allow_public_bind`. Run
the smoke only on a machine whose ports untrusted networks cannot reach.

## Run the smoke

From the repository root:

```bash
scripts/observability-smoke.sh run
```

The first run compiles the binaries. When the smoke passes, it ends with:

```text
[zinder-observability] smoke run is still running so dashboards stay inspectable
[zinder-observability] stop it with: scripts/observability-smoke.sh stop
```

Then open:

| What | Where |
| --- | --- |
| Grafana, with the Zinder overview dashboard | <http://127.0.0.1:3002>, user `admin`, password `admin` unless you set `ZINDER_GRAFANA_ADMIN_PASSWORD` |
| Prometheus, with the alert rules | <http://127.0.0.1:9095> |
| Metrics of each runtime | `http://127.0.0.1:<port>/metrics` with port 9190 (ingest), 9194 (projector), 9193 (query), or 9192 (compatibility) |
| Readiness report | `.tmp/observability/reports/latest-readiness.md` and `.json` |
| Evidence: readiness samples, raw metrics, gRPC responses, writer fences, and logs | `.tmp/observability/reports/<run-id>-evidence/`, indexed by `manifest.json` |
| Process logs | `.tmp/observability/logs/` |

To print the endpoints and current Prometheus values again, run
`scripts/observability-smoke.sh snapshot`.

A run with the default settings mines no blocks. Ingest follows the tip, but
`zinder_ingest_canonical_live_appends_total` stays absent until a block
arrives. To mine Regtest blocks after startup, set
`ZINDER_OBSERVABILITY_GENERATE_BLOCKS` to a positive number.

If the run fails, it prints the error and copies the logs into the evidence
directory. The full logs are under `.tmp/observability/logs/`.

## Run against Testnet or Mainnet

Point the smoke at a synced node and disable mining, because public networks
cannot mine on demand. With a Z3 stack, read the node's cookie from its
volume:

```bash
cookie=$(docker run --rm -v z3-testnet-cookie:/auth:ro alpine cat /auth/.cookie)
ZINDER_OBSERVABILITY_NETWORK=zcash-testnet \
ZINDER_OBSERVABILITY_NODE_ADDR=http://127.0.0.1:18232 \
ZINDER_OBSERVABILITY_NODE_AUTH_USERNAME="${cookie%%:*}" \
ZINDER_OBSERVABILITY_NODE_AUTH_PASSWORD="${cookie#*:}" \
ZINDER_OBSERVABILITY_GENERATE_BLOCKS=0 \
ZINDER_OBSERVABILITY_BULK_CATCHUP_BLOCKS=100 \
scripts/observability-smoke.sh run
```

For Mainnet, use `zcash-mainnet`, the `z3-mainnet-cookie` volume, your
node's Mainnet RPC address, and a larger
`ZINDER_OBSERVABILITY_BULK_CATCHUP_BLOCKS`, such as 1000.

## Certify lag, restarts, and a reorg on Regtest

This optional run changes the node's chain, so it works only on Regtest:

```bash
ZINDER_OBSERVABILITY_CERTIFY_TOPOLOGY=1 scripts/observability-smoke.sh run
```

After the normal smoke, it does the following and requires every runtime to
report ready again after each step:

1. Pauses the projector, mines past the compatibility lag threshold, requires
   `replica_lagging`, and resumes the projector.
2. Restarts compatibility, query, the projector, and ingest, one at a time.
3. Invalidates one block and mines a two-block replacement. The run passes
   only if both the node and compatibility return the new block hash at that
   height and the writer moves onto the new branch.

Before it mines anything, the script checks that the node really is Regtest:
every network upgrade must activate at height 1, and every RPC it needs must
exist. It refuses to mine on any other node.

## Calibrate

`calibrate` repeats the smoke and writes P50, P95, P99, and worst-case values
to `.tmp/observability/reports/latest-calibration.md` and `.json`:

```bash
ZINDER_OBSERVABILITY_RUNS=6 scripts/observability-smoke.sh calibrate
```

Set the node variables as in the Testnet example to calibrate against a
public network. Calibration cannot run the Regtest certification.

## Tunables

| Variable | Default | Effect |
| --- | --- | --- |
| `ZINDER_OBSERVABILITY_NETWORK` | `zcash-regtest` | Network written to the generated configs. |
| `ZINDER_OBSERVABILITY_NODE_ADDR` | `http://127.0.0.1:39232` | Node JSON-RPC address. |
| `ZINDER_OBSERVABILITY_NODE_AUTH_USERNAME`, `ZINDER_OBSERVABILITY_NODE_AUTH_PASSWORD` | `zebra`, `zebra` | Node basic-auth credentials. |
| `ZINDER_OBSERVABILITY_BULK_CATCHUP_BLOCKS` | `50` | Blocks built after the checkpoint. |
| `ZINDER_OBSERVABILITY_GENERATE_BLOCKS` | `0` | Regtest blocks to mine after ingest follows the tip. |
| `ZINDER_OBSERVABILITY_CERTIFY_TOPOLOGY` | `0` | `1` runs the Regtest certification. |
| `ZINDER_OBSERVABILITY_COMPAT_REPLICA_LAG_THRESHOLD_CHAIN_EPOCHS` | `4` | Compatibility lag threshold in the generated config. |
| `ZINDER_OBSERVABILITY_CERTIFICATION_LAG_BLOCKS` | threshold plus 1 | Blocks mined while the projector is paused. Must exceed the threshold. |
| `ZINDER_OBSERVABILITY_RUNS` | `5` | Runs for `calibrate`. |
| `ZINDER_OBSERVABILITY_WORK_DIR` | `.tmp/observability` | Absolute directory for stores, configs, logs, and reports. |
| `ZINDER_OBSERVABILITY_RESET` | `1` | `1` deletes the previous run's stores, configs, and logs first. |
| `ZINDER_PROMETHEUS_PORT`, `ZINDER_GRAFANA_PORT` | `9095`, `3002` | Host ports for Prometheus and Grafana. |
| `ZINDER_OBSERVABILITY_INGEST_OPS_ADDR`, `ZINDER_OBSERVABILITY_PROJECTOR_OPS_ADDR`, `ZINDER_OBSERVABILITY_QUERY_OPS_ADDR`, `ZINDER_OBSERVABILITY_COMPAT_OPS_ADDR` | `0.0.0.0:9190`, `:9194`, `:9193`, `:9192` | Operational endpoint addresses. Prometheus scrapes the fixed ports in `observability/prometheus/prometheus.yml`. |

The script refuses a work directory that is not empty and was not created by
the smoke, so it cannot delete a real store. Do not point it at an operator
directory. The runtimes start with a clean environment, so other `ZINDER_*`
variables in your shell do not reach them.

## Stop

```bash
scripts/observability-smoke.sh stop
```

This stops the four runtimes and removes the Prometheus and Grafana
containers. The work directory stays until the next run resets it.
