# Restoring the Mainnet benchmark fixture

Maintainers use this procedure to bring up a frozen Mainnet deployment for a
benchmark or a recovery drill without rebuilding the stores from genesis.
A from-genesis build of the wallet-serving stores takes several hours. A
restore from the fixture's disk snapshots takes minutes, plus the time to
read the store files once.

The fixture is a set of GCP disk snapshots, one per component, described by a
`fixture.json` file. [`scripts/mainnet-fixture/fixture.schema.json`](../../scripts/mainnet-fixture/fixture.schema.json)
defines that file. Each fixture records:

- the network and the chain fence it serves: visible and settled heights and
  block hashes;
- the canonical, wallet, and artifact schema versions, and the release that
  built the stores;
- for each component, the snapshot name, the disk size, the mount path, the
  paths of the stores or node state on it, and the SHA-256 of a sorted
  `sha256sum` listing of its files.

| Component kind | Holds |
| --- | --- |
| `node-state` | Zebra state at the fixture tip, including the persisted non-finalized blocks. |
| `stores` | The canonical and wallet RocksDB stores, closed. |
| `catalog` | A signed history catalog generation on an fs-verity ext4 disk, with its benchmark-only publisher key. |
| `oracle` | Recorded wallet responses that a restored reader must reproduce. |

A fixture is tied to one store schema. A release that changes the canonical
or wallet schema needs a new fixture, built by that release.

## Before you start

- Use a workstation with `gcloud`, `jq`, Docker, `curl`, and `zstd`, and
  access to the fixture project.
- Have the fixture's `fixture.json`.
- Have Zinder release binaries for the release you will serve with. The
  `release-binaries` target of `deploy/Dockerfile` produces them:

  ```bash
  docker buildx build -f deploy/Dockerfile --target release-binaries \
    --build-arg ZINDER_BUILD_GIT_COMMIT="$(git rev-parse HEAD)" \
    --platform linux/amd64 --output type=local,dest=.tmp/release-binaries .
  ```

- Have a private bucket for the stage tarball. Fixture hosts have no
  internet access. They reach Cloud Storage through Private Google Access.

## Build the stage

The stage tarball holds everything the host runs: the Zinder binaries, the
pinned Zebra binary, `jq`, `grpcurl`, the guest scripts, and the protocol
files. Build it once per release and upload it with its SHA-256.

```bash
scripts/mainnet-fixture/build-stage.sh --binaries .tmp/release-binaries/bin \
  --out .tmp/zfx-stage.tar.zst \
  --upload gs://zfnd-dev-zinder-fixture-backups/zinder/tools/zfx-stage-905dac4e.tar.zst
```

## Restore a host

Run the restore script with the fixture, the stage, a host name, and a disk
profile.

```bash
scripts/mainnet-fixture/restore-fixture.sh \
  --fixture fixture.json \
  --stage gs://zfnd-dev-zinder-fixture-backups/zinder/tools/zfx-stage-905dac4e.tar.zst \
  --name zfx-bench-1 --profile fast --verify-hashes stores
```

The script performs these steps:

1. It checks that every component snapshot is `READY` and that the stage has
   a published SHA-256.
2. It creates a `c4-standard-16` host with one thread per core, no service
   account, no external address, and a termination time. The default
   termination time is 12 hours, and `--hours` allows up to 36.
3. It creates one Hyperdisk Balanced disk per component from its snapshot,
   with the selected profile.
4. It installs a short-lived access token, downloads the stage, deletes the
   token, and checks the stage and binary hashes.
5. It mounts each component. With `--verify-hashes stores` or `all`, it reads
   every listed file and compares it with the component's listing. The
   listing itself must match the SHA-256 in `fixture.json`. This read also
   fetches every block of the restored disk, so later timings do not include
   first-read latency.
6. It starts Zebra with no peers, then ingest, projector, query, and
   lightwalletd compatibility, with fresh control tokens and a fresh
   projector owner identity.
7. It waits for the compatibility reader's `/readyz`. It then checks that
   `GetLatestBlock` returns the fixture's visible height and hash, and records
   a fixed set of wallet responses.
8. It writes a JSON receipt with each step's start time, the profile, the
   stage and fixture hashes, the served tip, and the wallet sample digest.

| Profile | Component disk IOPS | Component disk throughput |
| --- | --- | --- |
| `fast` | 12,000 | 375 MiB/s |
| `modest` | 3,000 | 140 MiB/s |

Services run on fixed CPUs of the 8-CPU host: Zebra on 0–1, ingest and the
projector on 2–3, and query and compatibility on 4–5. CPUs 6–7 stay free for
the load client.

## Check the result

The script exits with an error if any check fails. On success, the receipt
shows the served tip and `wallet_sample_matches_fixture`:

```bash
jq '{nothing_to_reader_seconds, served_tip: .guest.served_tip,
  wallet_sample_matches_fixture: .guest.wallet_sample_matches_fixture}' \
  restore-receipt-zfx-bench-1.json
```

`wallet_sample_matches_fixture` is `true` when the 21 recorded wallet
responses match the fixture byte for byte, and `null` when the fixture
records no sample digest.

## Clean up

Delete the host when the benchmark ends. Its disks are deleted with it.

```bash
gcloud compute instances delete zfx-bench-1 --project zfnd-dev-zinder --zone us-east1-c --quiet
```

Keep the component snapshots. They are the fixture.

## Private backup archives

The fixture's snapshots are disk images for benchmarks. A
[private backup](private-backups.md) is the product's recovery path and holds
the same stores in a sealed archive. A private backup archive contains the
deployment's cursor authentication key. Keep it in private storage and do
not publish it.
