# Native compact-block scan experiment

A wallet that scans shielded pools can omit transparent inputs and outputs from its downloads. A second option is to send an extra public Pallas point for each Orchard and Ironwood action so a wallet can reduce the work of trial decryption. This experiment measures the two options independently on the native Zinder compact-block format. Both encodings are experimental. [Ztreamer’s shifted-key implementation](https://github.com/zcashlabs/ztreamer/pull/382) describes the point and its cost per action.

An incorrect hint can be a valid Pallas point and still hide a payment. Before treating a failed trial decryption as evidence that an action is unrelated, a wallet must verify that the hint equals `[2^64]EPK` or repeat trial decryption with the original EPK. The measured payload sizes include that original EPK.

[Shifted-key scanner results and reproduction](SCANNING.md) compares stock and assisted trial decryption in native Rust and Chromium on these public actions.

## Replaying a range

The exporter opens a read-only secondary of a schema v2 Testnet canonical store with the default reorg window and a recorded upgrade schedule. It accepts stores with or without full blocks by default. Pass `--raw-blob-retention all` to require a full-block store. The exporter checks the height and parent hash of each block. This command repeats the mixed-range measurement.

```sh
cargo run --locked --release -p wallet-scan-experiment --bin export-wallet-scan-corpus -- \
  --store /path/to/testnet/canonical --start 4597953 --end 4600000 --corpus mixed.scan
sha256sum mixed.scan
```

The command prints a JSON report. Omit `--corpus` to measure only. The [mixed-range corpus](evidence/testnet-4597953-4600000.scan) contains the public actions from these 2,048 blocks, including the precomputed points. Its SHA-256 is `3178713f3197d43a8c7e4693bf6b879953e5fad375cfa9ba74f476adcf4dee90`.

## Native payload measurements

| Testnet heights (inclusive) | Complete bytes | Shielded bytes | Shielded with hints | Orchard / Ironwood actions |
| --- | ---: | ---: | ---: | ---: |
| 2,100,000–2,102,047 | 676,923 | 319,488 | 319,488 | 0 / 0 |
| 4,597,953–4,600,000 | 1,206,536 | 1,070,103 | 1,213,345 | 12 / 4,201 |
| 4,580,011–4,600,000 | 13,593,471 | 11,414,646 | 12,989,458 | 291 / 46,027 |

On the 19,990-block range, omitting transparent data saved 16.0%. Including hints then added 1,574,812 bytes, bringing the shielded stream to 95.6% of the complete payload. On the shorter mixed range, hints brought it to 100.6%. The shorter mixed range is contained in the longer one. These Testnet ranges illustrate how the result depends on pool activity.

The exporter measures the uncompressed protobuf stream item for each block. The shielded projection preserves block identity, tree sizes, and the original index of each retained transaction. It drops transactions with no shielded fields. For each canonically encoded, nonidentity Orchard or Ironwood EPK, the exporter computes the compressed point `[2^64]EPK` and appends it as a 32-byte length-delimited experimental action field. Field 5 is local to this experiment. The original EPK is retained. Each extended stream item must decode to the same shielded projection with the current native schema.

The counts exclude range headers, gRPC framing, compression, TLS, and wallet processing. The [raw results](evidence/native-payloads.json) include source heights, hashes, corpus digests, and seven timings of point derivation on a single logical CPU of a shared Ryzen 9 9950X. Derivation timing includes point decoding, 64 doublings, and re-encoding inside the exporter. It excludes database reads and stream encoding. Estimating server capacity requires a concurrent serving workload.

## Corpus format

Files start with the five ASCII bytes `ZSCAN` and version byte 1. Each action has a one-byte pool tag (`O` for Orchard or `I` for Ironwood), the 32-byte nullifier, 32-byte commitment, 32-byte EPK, 52-byte compact ciphertext, a one-byte hint length, and that many hint bytes. The length is 0 or 32. Actions are ordered by block and transaction, then Orchard followed by Ironwood in their respective pool order. The corpus is a scanner input and contains no wallet keys.
