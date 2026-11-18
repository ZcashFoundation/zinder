# zinder-compat-cipherscan

`zinder-compat-cipherscan` serves Cipherscan's REST and WebSocket API from Zinder's `ExplorerQuery` and `WalletQuery` services, so the Cipherscan frontend can use Zinder without changes. It opens no storage and keeps only bounded in-memory caches.

The adapter is optional and is not published as a release image. It needs a running `zinder-explorer` (with `[ingest].explorer_views = true`) and a reachable `zinder-query`. Many Cipherscan routes are degraded or unavailable against the current Explorer; check the [coverage matrix](../../docs/architecture/cipherscan-adapter.md) before deploying.

## Run it

Write a configuration file, for example `/etc/zinder/cipherscan.toml`:

```toml
[network]
name = "zcash-testnet"

[cipherscan]
listen_addr = "127.0.0.1:9070"
explorer_query_endpoint = "http://127.0.0.1:9068"
wallet_query_endpoint = "http://127.0.0.1:9102"
# bearer_token_path = "/run/secrets/zinder-reader-token"

[ops]
listen_addr = "127.0.0.1:9108"
```

The values shown for the listeners and endpoints are the defaults. The price endpoints default to CoinGecko and can be changed with `current_price_endpoint` and `historical_price_endpoint_template`. Settings load from defaults, then the TOML file, then `ZINDER_*` environment variables, then command-line flags. `--print-config` prints the resolved configuration.

Start the adapter after both gRPC endpoints are up, because it connects to them before it starts listening:

```bash
cargo run --release -p zinder-compat-cipherscan -- --config /etc/zinder/cipherscan.toml
```

The adapter refuses a public listen address unless `security.allow_public_bind = true`. It has no TLS or client authentication. Put public traffic behind your own TLS proxy with authentication, rate limits, and quotas. The optional bearer token is sent to both Zinder gRPC endpoints.

`/healthz` on the ops listener shows the process is alive, and `/readyz` shows it connected to both endpoints. Neither proves that the Explorer's views cover a requested range. To check real routes, follow the [verification runbook](../../docs/runbooks/cipherscan-adapter-verification.md).
