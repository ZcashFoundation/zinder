# Back up and restore a wallet-serving indexer

Use a private backup to recover the canonical and wallet RocksDB stores from
one captured block. The archive includes the indexer's cursor authentication
key. Keep both the staging area and the archive private to the deployment.
Explorer views rebuild separately.

## Capture a backup

Run the projector binary from the same build as the running projector, with
its configuration and environment. It needs access to the owner's loopback
control listener, checkpoint token, and checkpoint staging directory. Ingest
and projector must run under the same filesystem user for a private capture.
Choose an absolute archive path outside the checkpoint staging directory
and the live stores. Its parent must exist and be writable. Each new
recovery point needs a unique name of 1 to 64 lowercase ASCII letters,
digits, or interior hyphens.

```bash
set -euo pipefail
archive=/srv/zinder-backups/wallet-20261120
receipt=$(zinder-projector --config /etc/zinder/projector.toml backup --archive "$archive")
```

For a single-volume wallet container named `zinder-wallet`, mount the archive
parent writable by the runtime user. Run this alternative in your host shell.

```bash
set -euo pipefail
archive=/srv/zinder-backups/wallet-20261120
receipt=$(docker exec --user 10001:10001 zinder-wallet zinder-projector \
  --config /etc/zinder/projector.toml backup --archive "$archive")
```

The Compose projector uses `/etc/zinder/config.toml`. Capture can pin old
RocksDB files in the checkpoint staging directory while the live stores
advance. Allow space for the sealed archive, outstanding captures, and
wallet validation scratch space.

Both examples capture the JSON receipt from standard output. Progress goes to
standard error. In the same shell, use `jq` to save the digest from the first
capture. The example assumes an existing, writable pin directory under
independent operator control.

```bash
pin_dir=/etc/zinder/backup-pins
pin=$(jq -er '.manifest_sha256' <<< "$receipt")
(set -o noclobber; printf '%s\n' "$pin" > "$pin_dir/${archive##*/}.sha256")
```

Keep an independently retrievable copy of that pin. Transfer the complete
archive directory to operator-controlled storage outside the indexer's
failure domain. For immutable retention, configure the target to prevent
replacement or deletion for the required period. Download a copy and run
the offline verification below with the saved pin. A retry with the same archive name verifies the original
completed capture. Compare the returned digest with the saved pin. Use a
new name to capture a later block. If capture stopped before its manifest
was published, use a fresh name. Once the off-host copy verifies, retire the
corresponding checkpoint candidate and local archive according to your
retention policy.

## Restore on an empty host

Download the archive to a directory where only the recovery operator can
change it. Retrieve the pin separately and place it in the example's pin
directory on the replacement host. Supply the intended network.
Offline verification checks the exact manifest and payload files, opens both
databases read-only, and compares the stored chain and wallet state with
the captured fence. Wallet validation uses temporary space under `TMPDIR`.
Set `TMPDIR` to an existing, writable directory outside the archive if you
need to choose the verification volume. After a forced process kill, remove
abandoned verification scratch once its verifier has exited.

Run restore as the user that will own the replacement stores. Choose an
absent destination beneath an existing, privately controlled parent on a
local filesystem that supports atomic directory publication without replacement.
Reserve space for both databases and validation scratch on that filesystem.

```bash
set -euo pipefail
archive=/srv/zinder-backups/wallet-20261120
network=zcash-testnet
pin=$(cat /etc/zinder/backup-pins/wallet-20261120.sha256)
zinder-projector verify --network "$network" --archive "$archive" --sha256 "$pin"
zinder-projector restore --network "$network" --archive "$archive" --sha256 "$pin" \
  --into /srv/zinder/replacement
```

Restore checks each file against the pinned inventory as it copies, then
reads the installed files back and verifies both databases before publishing.
After interruption, repeat the command with the same paths.
The installer reclaims its own unpublished working directory. If the
destination was already published, inspect it before making another attempt.
Keep the hidden recovery work marker beside the destination while retries
are possible.

Point ingest and the projector's canonical reader at
`/srv/zinder/replacement/canonical`. Point the projector's wallet writer at
`/srv/zinder/replacement/wallet`. Configure query and lightwalletd compatibility
to read both replacement paths, with a fresh secondary directory for each
reader. Give the replacement its own node configuration, control credentials,
and projector owner identity. The private copy keeps
its chain history and cursor authentication identity. Restore releases
captured projector retention leases so a new owner can acquire its own.
The live mempool hydrates from the node on startup.

Start ingest, projector, query, and lightwalletd compatibility with the replacement
outside traffic. After catch-up, compare the node's block hash with the hash
served at the same height by the native and lightwalletd listeners. Complete
a wallet block range through the replacement and check the wallet projection
against a reference at the same fence. A ready reader can still serve an
earlier height during catch-up. At cutover, drain the old endpoint and stop
its writers before exposing the replacement as the sole active deployment.
Both copies recognize the same cursor identity. A client whose cursor lies
beyond the restored history must resume from a retained position or obtain
a fresh head and reconcile its view.

Keep the previous deployment and the verified off-host archive through the
rollback window. If the replacement fails its checks before cutover, keep
traffic on the original. To roll back after cutover, drain the replacement
and stop its writers. Start the previous deployment outside traffic and
repeat the same block-hash and wallet-range checks before routing clients
back. When retention expires, dispose of every stored copy as private key
material, including any downloaded verification copies.

This workflow uses matching current store schemas and a state bundle with a
writer-admitted upgrade schedule. A public bootstrap snapshot requires its own
export format and publisher authentication. [Storage and deployment topologies](../adrs/0004-storage-and-deployment-topologies.md)
