# zinder-client

`zinder-client` is the remote-first typed Rust SDK for Zinder wallet-query
consumers. It keeps generated protobuf messages and transport status values
behind `ChainIndex`, `ChainSnapshot`, `EndpointBackedIndex`, `ServerInfo`, and
`IndexerError`.

Connect to a native `WalletQuery` endpoint and pin related reads to one chain
epoch:

```no_run
use zinder_client::{
    ChainIndex, IndexerError, Network, RemoteChainIndex, RemoteOpenOptions,
};

async fn read_visible_tip(endpoint: String) -> Result<(), IndexerError> {
    let index = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: Network::ZcashMainnet,
    })?;
    let snapshot = index.snapshot().await?;
    let visible_tip = snapshot.visible_tip_block().await?;
    let _ = visible_tip;
    Ok(())
}
```

Methods on `ChainIndex` are canonical or wallet-projection reads. Methods on
`EndpointBackedIndex`, including broadcast, chain events, live mempool reads,
and server metadata, require a reachable endpoint. `Capability` and
`ServerInfo::supports` provide exact-match feature discovery before optional
operations are called.

Zinder keeps account state, trial decryption, keys, signing, proving, and
wallet recovery policy in the consumer. The SDK supplies chain artifacts,
epoch and reorg semantics, event cursors, transaction status, and broadcast.

See the repository's [server-side wallet pattern](https://github.com/ZcashFoundation/zinder/blob/main/docs/reference/server-side-wallet-pattern.md)
for the full integration sequence and librustzcash ownership boundary.
