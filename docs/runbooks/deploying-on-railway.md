# Railway canonical-writer validation

This Railway target runs only `zinder-ingest`, for isolated canonical
correctness, restart, or performance validation. It projects no wallet state
and serves no client protocol.

Use [Deploying on a VM](deploying-on-a-vm.md) for the complete topology, or
[Wallet-serving deployment (single-volume hosts)](deploying-wallet-serving.md)
for the native `WalletQuery` service.

## Image

Create the Railway service from a Docker image source and name a released
image by its exact tag:

```text
ghcr.io/zcashfoundation/zinder-canonical-runtime:vX.Y.Z
```

The package is public, so the service needs no registry credentials. Railway
pulls the image; it does not build Zinder. To deploy another release, change
the tag on the service.

The image is the `zinder-canonical-runtime` target of `deploy/Dockerfile`. It
is admitted only as a canary or diagnostic deployment:

```bash
bash scripts/validate-deployment-admission.sh \
  --deployment-class canary \
  --target zinder-canonical-runtime
```

The image starts `zinder-ingest` through
`deploy/canonical-runtime-entrypoint`, drops privileges to the `zinder` user,
and exposes the operational endpoint. It does not run projector, compatibility,
native query, explorer, or coherent restore services.

Railway cannot build this image from the repository. It builds the last stage
of a Dockerfile and has no target selection, and the last stage of
`deploy/Dockerfile` is a different target. To run an unreleased commit, build
the target yourself, push it to a registry the service can read, and point the
service at that image:

```bash
docker build -f deploy/Dockerfile --target zinder-canonical-runtime \
    -t <registry>/zinder-canonical-runtime:<commit> .
```

## Configuration

Attach one persistent volume at `/var/lib/zinder`. Configure the network, Zebra
endpoint and authentication, canonical path, wallet workload, wallet-serving
coverage, transaction retention, reorg policy, operations listener, and private
control listener through `ZINDER_*` variables.

`/healthz` proves process liveness. `/readyz` proves only the canonical writer
and mempool contract. It does not prove wallet projection, wallet-serving admission,
public TLS, coherent recovery, or client compatibility.

Record the image digest, network, starting and final fences, construction and
publication duration, peak memory, storage bytes, source settings, and restart
result outside the repository. Preserve the volume after a failed run until
the required diagnostic evidence is captured.

## Boundaries

- Do not label this target as a production wallet deployment.
- Do not add projection or serving processes to this image; the wallet-serving
  Railway topology is a separate target with its own volume and service.
- Do not use Railway service volumes as cross-service RocksDB replication.
- Do not infer full-topology capacity from the canonical directory alone.

See [ADR-0035](../adrs/0035-canonical-storage-topologies.md),
[Service boundaries](../architecture/service-boundaries.md), and
[Public interface configuration](../architecture/public-interfaces.md#configuration).
