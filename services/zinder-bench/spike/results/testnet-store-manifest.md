# Testnet spike data manifest

All numbers are Zcash **Testnet**, measured on one host (32 cores, 125 GB RAM), from genesis to the tip by the existing `zinder-ingest` and `zinder-projector` paths. Nothing here is Mainnet.

Source revision: branch `t3/indexer-architecture-assessment`, HEAD `32099c41` (v0.11.0 prepare commit), clean tree. Release binaries from `cargo build --locked --release --bin zinder-ingest --bin zinder-projector` (1m22s) in `target/release/`.

Node: Zakura 1.8.6 Testnet in container `zakura-testnet-zinder` (JSON-RPC `127.0.0.1:49232`, indexer gRPC `127.0.0.1:49230`, cookie auth, `pruned=false`). Used read-only.

## Stores (closed, consistent, both writers exited with status 0)

Treat both stores as read-only. Copy or checkpoint before writing.

| Store | Path | On-disk size |
| --- | --- | --- |
| Canonical (RocksDB, READY) | `/home/ubuntu/.t3/worktrees/zinder/t3-c67a34ca-dd54fadf1533b1ea/.tmp/spike-data/canonical.rocksdb` | 11,135,199,359 bytes (11 GiB-class, `du -sh` 11G) |
| Wallet (RocksDB, READY) | `/home/ubuntu/.t3/worktrees/zinder/t3-c67a34ca-dd54fadf1533b1ea/.tmp/spike-data/wallet.rocksdb` | 3,189,509,444 bytes (`du -sh` 3.0G) |

Whole `spike-data` directory: 14 GB (includes logs, the empty projector secondary directory, and `wallet.rocksdb.build-lock`, a zero-byte lease file that must not be deleted). Staging directories (`canonical.rocksdb.building`, `wallet.rocksdb.projection-load-staging`, peak observed 2.5 GB for the wallet) were removed by the services after publication. The release build adds 1.2 GB under `target/`. Total footprint: about 15 GB.

### Tip and identity

- Canonical visible tip: height **4,620,122**, hash `000cb31fafb083eea524b9984e116b0faeabbc3a9996a6a58b3488a5b90ee3c2` (confirmed with the node's `getblockhash 4620122`). `zinder-ingest probe` after shutdown reported `store_tip=4620122`, `upstream_tip=4620122`, `gap_blocks=0`.
- Wallet tip: 4,620,122 (projector `/readyz` `current_height=4620122` immediately before the stop, same value as the canonical tip). The cold-validated build was published at height 4,620,095 (chain_epoch 13, event_sequence 13), then followed 27 blocks to 4,620,122.
- Wallet at build: `row_count=41,906,322`, `projection_digest=a35829a35bba3fec790301529499a68fe1915c3040cbc3b52aa66c2506c7a414`.
- Canonical baseline was published at `fixed_tip_height=4,620,083`, then the live follower appended to 4,620,122 (chain_epoch 40, event_sequence 40 at the last append).
- Coverage: `--wallet-serving` (checkpoint height 0, raw blob policy `transactions`). Reorg window 1000 on both services. The wallet cannot be followed by a narrower canonical coverage.

### Schema versions (from source at HEAD, constants)

- `CANONICAL_STORE_SCHEMA_VERSION` = 7 (`crates/zinder-store/src/canonical_store/mod.rs`)
- `CURRENT_ARTIFACT_SCHEMA_VERSION` = 20 (same file)
- `CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION` = 4
- Wallet projection declares `REQUIRED_CANONICAL_STORE_SCHEMA_VERSION` = 6 and `REQUIRED_CANONICAL_REPLAY_FORMAT_VERSION` = 1 (the canonical store constant above is 7; the projector accepted the store)
- `WALLET_PROJECTION_SCHEMA_VERSION` = `WALLET_ROCKSDB_SCHEMA_VERSION` = 1

These come from the source constants, not read back from the on-disk control records.

## Commands and config

Scripts (no secrets; the cookie is referenced by path only):

- `.tmp/spike-data/run-ingest.sh`
- `.tmp/spike-data/run-projector.sh`

```
ZINDER_NODE__AUTH__METHOD=cookie ZINDER_NODE__AUTH__PATH=<cookie file> \
ZINDER_NODE__INDEXER_GRPC_ADDR=http://127.0.0.1:49230 \
/usr/bin/time -v target/release/zinder-ingest --network zcash-testnet --node-source zebra-json-rpc \
  --json-rpc-addr http://127.0.0.1:49232 --node-auth-method cookie --node-auth-path <cookie file> \
  --storage-path .tmp/spike-data/canonical.rocksdb --wallet-serving \
  --ingest-control-listen-addr 127.0.0.1:19100 --ops-listen-addr 127.0.0.1:19105

ZINDER_NODE__AUTH__METHOD=cookie ZINDER_NODE__AUTH__PATH=<cookie file> \
/usr/bin/time -v target/release/zinder-projector --network zcash-testnet \
  --node-json-rpc-addr http://127.0.0.1:49232 --canonical-path .tmp/spike-data/canonical.rocksdb \
  --canonical-secondary-path .tmp/spike-data/projector-canonical-secondary --wallet-path .tmp/spike-data/wallet.rocksdb \
  --build-owner-hex 5a1ce5ba5eba11d0c0ffee5eed5eed01 --lease-duration-seconds 14400 --reorg-window-blocks 1000 \
  --ingest-control-addr http://127.0.0.1:19100 --ops-listen-addr 127.0.0.1:19110
```

All RocksDB resource budgets are the shipped defaults (canonical writer: 512 MiB block cache, 256 MiB WAL cap, 16 MiB x 2 write buffers, 2 background jobs, 256 MiB memtable budget, tickers statistics; direct I/O resolved for both stores). Binary built with host-default CPU flags, not the Docker `x86-64-v3` baseline.

Shutdown: no stop-at-height exists for the wallet and the projector needs live ingest control, so both ran together. After the projector reported ready at the live tip, the projector was sent SIGTERM (exited 0 at 17:47:45Z), then the ingest (exited 0 at 17:47:46Z). Both `time -v` records show `Exit status: 0`.

## Construction time and throughput (Testnet)

| Phase | Start (UTC, 2026-11-14) | End | Wall time | Throughput |
| --- | --- | --- | --- | --- |
| Canonical construction, genesis to baseline tip 4,620,083 | 17:32:51 | 17:37:49 (`canonical_fresh_construction_ready`, includes cold reopen) | about 298 s | about 15,500 blocks/s |
| Wallet construction, to height 4,620,095 | 17:37:59 | 17:47:31 (`wallet_projection_construction_complete`) | `build_seconds=572.5` | about 8,070 blocks/s, about 73,200 rows/s |

Notes on the numbers:

- Canonical wall time is derived from log timestamps (start of `canonical_fresh_construction_started` to `canonical_fresh_construction_ready`). The source fetch was concurrent JSON-RPC against a local node. 31 `source_chain_segment_split` warnings occurred (segments above the 64 MiB JSON-RPC response limit were halved and retried; start heights from 4,307,713 to 4,604,809), mostly in the heavy-block range described below.
- The wallet phase ran while the live ingest follower was appending blocks (about one block per 25 s) and serving ingest control, so it is not isolated from that load.
- Process lifetime (from `/usr/bin/time -v`): ingest 14:54.58 elapsed, 76% CPU average; projector 9:45.65 elapsed, 123% CPU average. These include the follow tail, so they are not construction times.
- Acceptance counters at the end of ingest: `zinder_ingest_canonical_historical_prevout_reads_total=0`, `zinder_ingest_canonical_cross_block_wallet_reads_total=0`.

## Peak RSS

Read from `VmHWM` in `/proc/<pid>/status` just before stop, and matched by `/usr/bin/time -v` "Maximum resident set size":

- `zinder-ingest`: 2,850,960 kB (about 2.72 GiB) over construction plus follow
- `zinder-projector`: 2,772,532 kB (about 2.64 GiB) over wallet build plus follow

## Density of the last 50,000 blocks (heights 4,570,123 to 4,620,122)

Counted via JSON-RPC `getblock` verbosity 2 against the same node (script and raw output: `logs/density-last-50k.json`). Testnet is far sparser than Mainnet; read these as Testnet only.

| Per block | Mean | Median | p95 | Max | Total |
| --- | --- | --- | --- | --- | --- |
| Transactions (incl. coinbase) | 1.77 | 1 | 4 | 52 | 88,376 |
| Transparent outputs | 28.73 | 1 | 1 | 42,001 | 1,436,592 |
| Transparent inputs | 23.08 | 1 | 2 | 6,001 | 1,154,044 |
| Sapling outputs | 0.009 | 0 | 0 | 8 | 447 |
| Sapling spends | 0.004 | 0 | 0 | 6 | 207 |
| Orchard actions | 0.010 | 0 | 0 | 10 | 513 |
| Block size (bytes) | 23,159 | 7,560 | 47,211 | 1,990,843 | 1,157,925,446 |

The mean is dominated by two transparent stress episodes, not a steady load. Median block has 1 transaction (coinbase only), 1 transparent output, and no shielded activity. Per-10,000-block means:

| Range | Tx | Transparent out | Transparent in | Block bytes |
| --- | --- | --- | --- | --- |
| 4,570,123 to 4,580,122 | 1.59 | 0.81 | 1.12 | 10,985 |
| 4,580,123 to 4,590,122 | 1.82 | 0.80 | 1.04 | 12,712 |
| 4,590,123 to 4,600,122 | 1.64 | 2.28 | 2.47 | 11,068 |
| 4,600,123 to 4,610,122 | 1.52 | 139.27 | 1.08 | 30,980 |
| 4,610,123 to 4,620,122 | 2.27 | 0.51 | 109.68 | 50,047 |

The second-to-last range is a transparent output fan-out wave and the last range is a consolidation wave (inputs spend those outputs), so the store's wallet rows are heavily transparent. Whole-chain average is about 9.07 wallet rows per block (41,906,322 rows over 4,620,095 blocks).

## Not measured or caveats

- No Mainnet data exists on this host; Mainnet capacity must come from the synthetic density arm, not from these stores.
- Per-column-family sizes were not extracted. Only per-store totals are recorded.
- Schema versions were taken from source constants at HEAD, not from the control records on disk.
- The wallet-only phase time excludes anything outside the `build_seconds` figure the projector reports, and the canonical phase time is from log timestamps, not an internal timer.
- Peak RSS spans build plus follow; there is no separate construction-only peak.

## Raw logs

All under `/home/ubuntu/.t3/worktrees/zinder/t3-c67a34ca-dd54fadf1533b1ea/.tmp/spike-data/logs/`: `build.log`, `ingest.log`, `projector.log` (each ends with the `time -v` record), `ingest-metrics-final.prom`, `ingest-readyz-final.json`, `projector-readyz-final.json`, `peak-rss-before-stop.txt`, `density-last-50k.json`.
