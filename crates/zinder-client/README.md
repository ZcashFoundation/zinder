# zinder-client

`zinder-client` is the remote-first typed Rust SDK for Zinder wallet-query
consumers. It keeps generated protobuf messages and transport status values
behind `ChainIndex`, `ChainSnapshot`, `EndpointBackedIndex`, `ServerInfo`, and
`IndexerError`.

The [quickstart](https://github.com/ZcashFoundation/zinder/blob/main/docs/reference/integration-surfaces.md#quickstart)
builds a complete program with this crate: the `Cargo.toml` dependencies, a
`main` function, the endpoint and network to pass, and the output to expect.
The SDK speaks Zinder's native `WalletQuery` protocol, so it needs a
`zinder-query` endpoint, not a lightwalletd one.

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

For a server behind a private CA and a SOCKS5 proxy, supply the connection
settings when opening the client:

```no_run
use zinder_client::{
    ChainIndex, IndexerError, Network, RemoteChainIndex, RemoteClientOptions, RemoteOpenOptions,
};

async fn read_private_tip(ca_pem: Vec<u8>, spki_sha256: [u8; 32]) -> Result<(), IndexerError> {
    let index = RemoteChainIndex::connect_with_options(
        RemoteOpenOptions {
            endpoint: "https://wallet.internal:443".to_owned(),
            network: Network::ZcashTestnet,
        },
        RemoteClientOptions::default()
            .with_tls_ca_pem(ca_pem)
            .with_tls_spki_pin(spki_sha256)
            .with_socks5_proxy("socks5h://127.0.0.1:9050")
            .with_read_retries(),
    )?;
    let _ = index.visible_tip_block(None).await?;
    Ok(())
}
```

See the [remote client settings](https://github.com/ZcashFoundation/zinder/blob/main/docs/reference/integration-surfaces.md#remote-client-settings)
for trust, proxy, retry, and WebAssembly behavior.

A wallet syncs a range of compact blocks through the same snapshot, so the
blocks share an epoch with the tree states and other reads it pairs them with:

```no_run
use tokio_stream::StreamExt as _;
use zinder_client::{BlockHeight, BlockHeightRange, ChainIndex, IndexerError};

async fn scan_range<T: ChainIndex>(index: &T) -> Result<(), IndexerError> {
    let snapshot = index.snapshot().await?;
    let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(100));
    let mut blocks = snapshot.compact_blocks_in_range(range).await?;
    while let Some(block) = blocks.next().await {
        let block = block?;
        let _ = block;
    }
    Ok(())
}
```

The stream yields every requested height in ascending order or ends with an
error. A server response that breaks the stream contract surfaces as
`IndexerError::MalformedResponse`.

Methods on `ChainIndex` are canonical or wallet-projection reads. Methods on
`EndpointBackedIndex`, including broadcast, chain events, live mempool reads,
and server metadata, require a reachable endpoint. `Capability` and
`ServerInfo::supports` provide exact-match feature discovery before optional
operations are called.

Zinder keeps account state, trial decryption, keys, signing, proving, and
wallet recovery policy in the consumer. The SDK supplies chain artifacts,
epoch and reorg semantics, event cursors, transaction status, and broadcast.

See the repository's [server-side wallets](https://github.com/ZcashFoundation/zinder/blob/main/docs/reference/integration-surfaces.md#server-side-wallets)
for the full integration sequence and librustzcash ownership boundary.
