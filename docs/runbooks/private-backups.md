# Backups and Edge Snapshots

Use this runbook for two distinct recovery and bootstrap workflows:

| Mechanism | Scope | When to use | Trust & cursor behavior |
| --- | --- | --- | --- |
| **Coherent private backup** (`zinder backup` / `zinder-projector`) | Canonical + wallet RocksDB stores together, plus `cursor_auth_key` | Private disaster recovery or replacing an existing wallet-serving host without rebuilding | Keeps the deployment's `cursor_auth_key`; requires matching schemas and private archive/pin storage |
| **Verified edge snapshot** ([ADR-0024](../adrs/0024-verified-edge-snapshot-bootstrap.md); `zinder snapshot` / `zinder-ingest`) | Canonical store at a settled height divisible by 4,096 + optional `sealed-history/` | Initializing a new independent single-host `rocksdb` writer from a publisher checkpoint | Publisher-signed (Ed25519) + validator-attested; generates a fresh `cursor_auth_key` and clears leases/mempool |

Explorer views rebuild separately from the canonical store in both workflows. Never copy live RocksDB directories with `cp` or `rsync` — copies taken across non-quiesced files or different timestamps are inconsistent.

## Coherent private backups (`zinder backup`)

### Capture a backup

Run `zinder backup create` (or `zinder-projector backup`) from the same build as the running projector, using its configuration (`ZINDER_*` or `--config`) and filesystem user, with access to the loopback control listener, checkpoint token, and checkpoint staging directory. Choose an absent archive path outside the checkpoint staging directory and live stores, using a unique recovery name (1–64 lowercase ASCII letters, digits, or interior hyphens):

```bash
set -euo pipefail
archive=/srv/zinder-backups/wallet-20261120
receipt=$(zinder backup create --archive "$archive")
```

Inside Compose (`zinder-index`) or a single-volume container (`zinder-serve`), configure both the running projector and the `backup create` process with matching shared `ZINDER_PROJECTOR_CONTROL__LISTEN_ADDR` (`127.0.0.1:9101`), `ZINDER_PROJECTOR_CONTROL__BEARER_TOKEN_PATH`, and `ZINDER_PROJECTOR_CONTROL__CHECKPOINT_STAGING_ROOT` (`/var/lib/zinder/checkpoints`) settings (rather than relying on an auto-generated internal token that an external `exec` invocation cannot read), mount `/srv/zinder-backups` writable by UID `10001` onto `zinder-index`, and pass the deployment's full Compose `--env-file` and `-f` arguments:

```bash
set -euo pipefail
archive=/srv/zinder-backups/wallet-20261120
receipt=$(docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml -f /etc/zinder/docker-compose.override.yml \
  exec -T -u 10001:10001 \
  -e ZINDER_PROJECTOR_CONTROL__LISTEN_ADDR=127.0.0.1:9101 \
  -e ZINDER_PROJECTOR_CONTROL__BEARER_TOKEN_PATH=/var/run/zinder-control/projector.token \
  -e ZINDER_PROJECTOR_CONTROL__CHECKPOINT_STAGING_ROOT=/var/lib/zinder/checkpoints \
  zinder-index zinder backup create --archive "$archive")
```

Capture waits up to 1 hour by default (`--capture-timeout-seconds` overrides it); if a wait times out, retry with the same `--archive` name to recover the receipt. Reserve disk space for the combined size of both stores plus external-sort validation scratch.

Save the returned manifest digest immediately under independent operator control, then copy the archive off-host and verify it before retiring old checkpoints:

```bash
pin_dir=/etc/zinder/backup-pins
pin=$(jq -er '.manifest_sha256' <<< "$receipt")
(set -o noclobber; printf '%s\n' "$pin" > "$pin_dir/${archive##*/}.sha256")
```

### Choose wallet validation limits

`backup`, `verify`, and `restore` re-derive address-ordered rows with two external sorts (scratch lives under the archive dir for `backup`, `TMPDIR` for `verify`, and the destination working dir for `restore`). Override defaults consistently across all three commands (for example, `--max-accounted-reorg-undo-bytes 1073741824`):

| Limit | Default | CLI flag |
| --- | --- | --- |
| Memory per sorter | 128 MiB | `--max-sort-memory-bytes-per-sorter` |
| Temporary file space per sorter | 192 GiB | `--max-temporary-file-bytes-per-sorter` |
| Accounted reorg undo data | 512 MiB | `--max-accounted-reorg-undo-bytes` |

### Restore on an empty host

Download the archive and saved pin to private storage on the replacement host (use `TMPDIR` if `verify` scratch should live outside `/tmp`), and run `restore` as the user that owns the stores into an absent destination directory:

```bash
set -euo pipefail
archive=/srv/zinder-backups/wallet-20261120
network=zcash-testnet
pin=$(cat /etc/zinder/backup-pins/wallet-20261120.sha256)
zinder backup verify --network "$network" --archive "$archive" --sha256 "$pin"
zinder backup restore --network "$network" --archive "$archive" --sha256 "$pin" \
  --into /srv/zinder/replacement
```

If interrupted before publication, repeat with the same paths. After publication:

1. Point `zinder` (`--data-dir /srv/zinder/replacement`, or standalone `zinder-ingest`, `zinder-projector`, `zinder-query`, and `zinder-compat-lightwalletd`) at `/srv/zinder/replacement/canonical` and `/srv/zinder/replacement/wallet` with unique `secondary_path` directories.
2. Give the replacement its own node config, control tokens, and a fresh `projector.build_owner_hex` (restore releases old retention leases; mempool hydrates from the node).
3. Start the replacement outside traffic, wait for catch-up, and compare block hashes and a wallet range against the node and reference.
4. At cutover, drain the old endpoint and stop its writers (`zinder-ingest` and `zinder-projector`) before exposing the replacement as the sole active deployment — both copies share the same `cursor_auth_key`, so running both active would let reconnecting clients switch between diverging event histories (clients whose cursor lies beyond the restored history must resume from a retained position or obtain a fresh head).
5. Keep the old deployment and verified off-host archive through your rollback window. To roll back after cutover, drain the replacement and stop its writers first, start the previous deployment outside traffic, repeat the block-hash and wallet-range checks, and then route clients back; once retention expires, dispose of retired copies as private key material.

Verification and restore refuse backups from another wallet schema before copying; rebuild from empty paths on a schema bump ([ADR-0004](../adrs/0004-storage-and-deployment-topologies.md)).

## Verified edge snapshot bootstrap (`ADR-0024`)

An edge snapshot (`zinder-edge-snapshot.v1`) lets a new regional `zinder-ingest` instance initialize its canonical store from a publisher's checkpoint and follow the remaining chain from its own Zcash validator.

### Export an edge snapshot (publisher)

Prepare a quiesced READY canonical store whose visible and settled tips name the **same block** at a height divisible by `4,096` and that retains each terminal completed subtree root at that boundary. Stop the source `zinder-ingest` writer, then run:

```bash
zinder snapshot export-edge-snapshot \
  --output-dir /srv/zinder-snapshots/mainnet-3592192 \
  --signing-key /etc/zinder/snapshot-publisher.pk8 \
  --include-sealed-history /srv/zinder-history/mainnet/key-1/catalog
```

- `--signing-key` takes a DER-encoded Ed25519 PKCS#8 key and writes `manifest.v1.json`, `manifest.v1.sig`, `canonical-rocksdb/`, and optional `sealed-history/`.
- Export clears projection build leases and mempool events and replaces the publisher's `cursor_auth_key` in the copy.
- `--include-sealed-history` is optional; the source history directory must not overlap `--output-dir` or its staging path and must not contain the signing key.

### Bootstrap a new writer (receiver)

Point the receiving writer (`ZINDER_*` or `--config`) at an absent or empty `storage.path` and your own configured `NodeSource` validator (`--source` accepts a local directory or an `http://` / `https://` base URL without credentials, query, or fragment):

```bash
zinder snapshot bootstrap-from-snapshot \
  --source https://snapshots.example.org/mainnet-3592192 \
  --public-key "$PUBLISHER_PUBKEY_HEX" \
  --sealed-history-output /srv/zinder-history/mainnet/key-1/catalog
```

- `--public-key` takes the publisher's 64-character hex raw Ed25519 public key (repeat `--public-key` during a key rotation).
- `--spot-check-blocks` sets the positive random block sample count compared against your validator (default `32`, plus the snapshot boundary).
- `--sealed-history-output` is optional and must point to an absent or empty directory separate from `storage.path`.
- Before writing or downloading payloads, bootstrap verifies the manifest signature, store config (`network`, `raw_blob_policy`, `reorg_window_blocks`, `checkpoint_height`), and file lengths/SHA-256 digests. After cold admission, it attests genesis, network-upgrade schedule, validator tip (at least `reorg_window_blocks` ahead of the snapshot), commitment roots, terminal subtrees, and spot-checked blocks against your validator, generates a fresh `cursor_auth_key`, and atomically publishes `storage.path`.

> [!IMPORTANT]
> **Re-enable `fs-verity` when `require_fs_verity = true`.** Snapshot bundles copy opaque `sealed-history/` files without filesystem fs-verity metadata. If `zinder-compat-lightwalletd` sets `compat.shielded_history.require_fs_verity = true`, enable fs-verity on every destination segment file on an `ext4 -O verity` filesystem before starting compatibility:
>
> ```bash
> for f in /srv/zinder-history/mainnet/key-1/catalog/*.bin; do
>   fsverity enable --hash-alg sha256 --block-size 4096 "$f"
> done
> ```

After bootstrap succeeds, start `zinder serve` / `zinder index` (or `zinder-ingest` + `zinder-projector` for `wallet` and `explorer` presets) normally to follow from the snapshot tip. If `bootstrap-from-snapshot` is interrupted, confirm no snapshot process is running and inspect both final destinations (`<storage.path>` and `<sealed_history_output>`) before retrying: if `<storage.path>` was already published, preserve it rather than re-running bootstrap; if `<sealed_history_output>` was published before `<storage.path>` could be renamed into place, move or remove that partial `<sealed_history_output>` directory alongside `<storage.path>.bootstrap.staging`, `<storage.path>.bootstrap.download`, and `<sealed_history_output>.bootstrap.staging` so the retry does not fail with `snapshot destination is occupied`.
