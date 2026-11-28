# Vendored Zebra indexer protocol

`indexer.proto` is vendored from [ZcashFoundation/zebra](https://github.com/ZcashFoundation/zebra)
at the release tag and commit recorded in `VERSION` and `COMMIT`.
Zinder's source layer consumes the
`zebra.indexer.rpc.Indexer` service to receive streaming notifications and
fetch raw best-chain block bytes by height.

## Updating

1. Resolve the peeled commit for Zebra's latest stable release that ships the
   `indexer.proto` shape Zinder requires. Do not retain an older provenance pin
   merely because the schema bytes have not changed.
2. Copy the file: `cp $ZEBRA/zebra-rpc/proto/indexer.proto crates/zinder-proto/proto/external/zebra/indexer.proto`
3. Record the release tag in `VERSION` and its peeled commit in `COMMIT`.
4. Regenerate the checked-in bindings and run the validation gate. The
   `vendored-proto` CI job downloads and diffs the schema at the recorded
   upstream commit.

## Boundary

Zinder's source adapters consume the generated `zebra.indexer.rpc.Indexer`
client. Mempool observations are hydrated through JSON-RPC before becoming
typed `MempoolSourceEvent` values. Historical block responses become typed
`SourceBlock` values only after their raw header and display-order response
hash agree. The adapters do not re-export Zebra's generated types across the
source boundary.
