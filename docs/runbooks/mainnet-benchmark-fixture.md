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
  `sha256sum` listing of its files;
- for the component that holds a signed history catalog, a `signed_history`
  entry: the publisher directory, the publisher key, the SHA-256 of the
  catalog's `current.json` and of the key, the segment count, and the highest
  height the catalog covers. Such a fixture also pins the exact stage tarball
  in `stage_sha256`, because an older stage would restore without the catalog.

| Component kind | Holds |
| --- | --- |
| `node-state` | Zebra state at the fixture tip, including the persisted non-finalized blocks. |
| `stores` | The canonical and wallet RocksDB stores, closed. |
| `catalog` | A signed history catalog generation on an fs-verity ext4 disk, with its benchmark-only publisher key. A `stores` component can carry the catalog instead. |
| `oracle` | Recorded wallet responses that a restored reader must reproduce. |

A fixture is tied to one store schema. A release that changes the canonical
or wallet schema needs a new fixture, built by that release.

## Before you start

- Use a workstation with `gcloud`, `jq`, Python 3, Docker, `curl`, and
  `zstd`, and access to the fixture project.
- Have the fixture's `fixture.json`.
- Have Zinder release binaries for the release you will serve with. When the
  fixture pins binaries in `verified_with_release.binaries`, use exactly that
  set, for example from the archive its `executables` field names; the guest
  refuses any other. Otherwise the `release-binaries` target of
  `deploy/Dockerfile` produces them:

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
files. Build it once per release and upload it with its SHA-256. For a fixture
with a catalog, set `stage_sha256` in `fixture.json` to that SHA-256.

A stage built before the restore recorded a per-attempt outcome cannot be used
with the current `restore-fixture.sh`, even for a fixture without a catalog.
The older guest writes a single receipt file instead of the attempt outcome
the workstation waits for, so the restore fails after the host is created,
with a reason that says to rebuild the stage. Rebuild every stage from the
current checkout, with the same binaries.

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

1. It checks the fixture, including its signed history and stage pin, that
   every component snapshot is `READY`, and that the stage has a published
   SHA-256 equal to any pin. The receipt path, with and without `.failed`
   and `.reservation` added, must not exist yet, and the script reserves it
   before it creates anything.
2. It creates a `c4-standard-16` host with one thread per core, no service
   account, no external address, and a termination time. The default
   termination time is 12 hours, and `--hours` allows up to 36.
3. It creates one Hyperdisk Balanced disk per component from its snapshot,
   with the selected profile.
4. It installs a short-lived access token, downloads the stage, deletes the
   token, and checks the stage and binary hashes. The guest steps run from
   the stage, so the stage SHA-256 in the receipt identifies the code that
   ran.
5. It starts the guest restore as a systemd unit named for this attempt. The
   guest records the attempt on the host and refuses any second attempt, so
   a retried launch cannot rerun on disks the first attempt changed.
6. The guest mounts each component, with `catalog` and `oracle` disks
   read-only. With `--verify-hashes stores` or `all`, it reads every listed
   file of the selected components and compares it with the component's
   listing. The listing itself must match the SHA-256 in `fixture.json`.
   Reading the files fetches the restored disk blocks that hold them, so
   later timings do not include their first-read latency. `stores` does not
   read a separate `catalog` component.
7. With a catalog, the guest hashes the catalog's `current.json` and
   publisher key once and compares them with `fixture.json`, whatever
   `--verify-hashes` is set to. From those verified manifest bytes it derives
   the first and last height and the segment files, which must match the
   fixture's segment count and highest height.
8. It starts Zebra with no peers, then ingest, projector, query, and
   lightwalletd compatibility, with fresh control tokens and a fresh
   projector owner identity. With a catalog, the compatibility reader
   follows the publisher directory and trusts only the fixture's key, as
   single-host Compose configures it. Unlike Compose, it requires fs-verity on
   every segment when the component's filesystem is `ext4-verity`.
9. It waits for the compatibility reader's `/readyz`. It then checks that
   `GetLatestBlock` returns the fixture's visible height and hash, and records
   a fixed set of wallet responses.
10. With a catalog, it runs the route check described below.
11. The guest writes its outcome for this attempt once, on success or at any
    failing step. The workstation polls for it until `--guest-timeout`
    (three hours by default), then checks the guest receipt against the
    fixture and writes the receipt.

A fixture without a `signed_history` entry restores as before. Its reader
serves block ranges from the canonical store and from its in-memory live tail,
the guest log says so, and the receipt's `signed_history` is `null`. The
preflight refuses a fixture whose `catalog` component, or whose component path
named `catalog` or `publisher_key`, lacks a `signed_history` object. Such a
restore would serve from canonical storage while every other check passed.

### Route check

Identical wallet responses do not show which route served them, so the
restore checks the route directly. The check is an external proof. The pinned
reader exports no identity for the catalog generation it admitted, so the
check infers admission from the files the reader process holds open, and the
route from the reader's canonical storage counters. It passes when all of
these hold:

- **Admission.** The reader process holds open exactly the segment files the
  verified manifest lists, and `current.json` still has the hash verified
  before the reader started.
- **Covered range.** `GetBlockRange` over heights 1,730,000 to 1,730,099
  returns every height and adds no canonical MultiGet or iterator reads. When
  the catalog does not hold those heights, the check uses its first 100
  heights instead.
- **Catalog end.** The catalog's last height, requested alone, adds no
  canonical reads. The height above it, requested alone, adds at least one
  canonical MultiGet key, which shows that the reader read storage rather
  than a segment, not which code path built the response. A live tail that
  already held that height would fail this probe, never pass it. Before the
  probes, the wallet sample requests the 100 heights below the visible tip,
  so for a catalog that ends within those heights the probe can fail and the
  restore with it.
- **Canonical control.** The covered range requested with explicit
  transparent, Sapling, and Orchard pools adds at least one MultiGet key per
  height. The reader serves any selection other than the default from
  canonical storage, so this shows that the counters see canonical reads.
- **Trust identity.** The same reader configuration with a different, valid
  publisher key exits before it serves, with `shielded range manifest
  signature verification failed`.

When the fixture pins a wallet sample and the catalog holds heights 1,730,000
to 1,730,099, the covered response must also equal the sample's copy. The
fixture recorded those bytes from a reader without the catalog. Otherwise the
check makes no byte comparison, and the receipt says so.

The counters reach `/metrics` only when the wallet serving pair refreshes,
about once a second, so each request is bracketed by two refreshes. Every
counter the check uses must be present and must not decrease. The check also
reports, without using them for its verdict, the canonical point lookups that
the catalog follower's anchor check makes, and the segment pages present in
the page cache after eviction advice. Page residency can stay unchanged after
a correct read, or change because of unrelated reads, so it is supporting
evidence only.

The restore makes no promise about cache state. The check advises the kernel
to drop the catalog's pages, but the kernel can keep them, as it does on
tmpfs, and the probes, the wallet sample, and the hash read leave other
pages of the catalog, the stores, and the node in memory. The reader's live
tail also holds the top of the chain after the wallet sample. A benchmark
that needs a cold or a warm start must establish that state itself and
measure it before it records results.

The workstation does not trust the report's own pass flags. It recomputes
every part from the recorded measurements with the same predicates the check
uses, and it accepts the guest's outcome only for this attempt and for the
`--verify-hashes` mode and components requested.

If any part fails, the guest stops the compatibility reader, and the restore
exits with an error and writes the receipt path with `.failed` added, holding
the failing step, the reason, and the guest's report. The restore reserves the
receipt path before it creates the host, by writing the path with
`.reservation` added, so two restores cannot share one receipt path.

| Profile | Component disk IOPS | Component disk throughput |
| --- | --- | --- |
| `fast` | 12,000 | 375 MiB/s |
| `modest` | 3,000 | 140 MiB/s |

Services run on fixed CPUs of the 8-CPU host: Zebra on 0–1, ingest and the
projector on 2–3, and query and compatibility on 4–5. CPUs 6–7 stay free for
the load client.

## Check the result

The script exits with an error if any check fails. The receipt then exists
only with a `.failed` suffix. On success, the receipt shows the served tip,
`wallet_sample_matches_fixture`, and the route proof:

```bash
jq '{nothing_to_reader_seconds, served_tip: .guest.served_tip,
  wallet_sample_matches_fixture: .guest.wallet_sample_matches_fixture,
  route: (.guest.signed_history | if . == null then null else
    {proof, passed, manifest_sha256, segment_count, first_height, last_height,
     open_segment_files: .admission.open_segment_files} end)}' \
  restore-receipt-zfx-bench-1.json
```

`wallet_sample_matches_fixture` is `true` when the 21 recorded wallet
responses match the fixture byte for byte, and `null` when the fixture
records no sample digest. The workstation writes a success receipt only when
the route proof's manifest hash, key hash, segment count, and last height
equal the fixture's.

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
