# Serve wallets over a Tor v3 onion service

Use the reference Compose profile to reach `CompactTxStreamer` and
`WalletQuery` over Tor from outside the host's network. For a home server,
configure Tor client authorization before you start the onion service. Tor
hides the client's IP address from Zinder. The [privacy boundary
matrix](#privacy-boundary-matrix) shows what the operator can still learn.

## Start with loopback listeners

Use the `wallet` preset to serve both interfaces. Follow [Deploying Zinder](deploying.md)
to prepare your node network, env file, storage, and control credentials.
Use Docker Compose with [`!reset` support](https://docs.docker.com/reference/compose-file/merge/#reset-value).
Run the commands from the repository root. Use the same Compose project
name and env file as your deployment when switching an existing stack.

```bash
export ZINDER_PRESET=wallet
env_file=deploy/.env.mainnet  # Use your deployment's env file.
compose=(docker compose --env-file "$env_file" -f deploy/docker-compose.yml
  -f deploy/docker-compose.tor.yml --profile tor)

"${compose[@]}" build --pull zinder-tor
"${compose[@]}" up -d zinder-index zinder-serve
"${compose[@]}" exec -T zinder-serve /usr/local/bin/zinder healthcheck \
  --ready --addr 127.0.0.1:9108
```

Wait for the readiness probe to succeed before serving wallets. The
[overlay](../../deploy/docker-compose.tor.yml) shares the writer's network
namespace with the readers and Tor. It binds the listeners there to
`127.0.0.1`, sets `security.allow_public_bind = false` on both Zinder
processes, and removes the base file's host port mappings. Leave the
observability profile off and run operational probes inside the namespace.
The base Compose host-port overrides do not change onion ports.

The [reference torrc](../../deploy/tor/torrc) uses `HiddenServiceVersion 3`
and maps onion ports `9067` and `9102` to the respective loopback wallet
listeners. It opens no SOCKS, control, or relay listener. Review the mapping
and keep ops, ingest and projector control, Explorer, and node ports out of
the onion service. The host needs outbound Tor connectivity. There are no
host ports to forward from a home router.

For a native installation, run Tor under a dedicated unprivileged account on
the Zinder host and use the same mapping in its torrc. Set `compat.listen_addr`
to `127.0.0.1:9067`, `query.listen_addr` to `127.0.0.1:9102`, and keep ops
and control on loopback. Leave `security.allow_public_bind` and
`trust_proxy_client_address` off. Use the Tor account's private, writable data
directory for `HiddenServiceDir`. See the [Tor setup guide](https://community.torproject.org/onion-services/setup/).

## Choose public or private access

- **Public operator.** Anyone who learns the onion address can connect.
  Verify both ports before announcing the address over a trusted channel.
  Apply connection and request limits appropriate for public access. Tor
  passes a loopback connection to Zinder. Zinder cannot enforce a quota by
  the original client IP. See the [Tor onion service DoS guidelines](https://community.torproject.org/onion-services/advanced/dos/).
- **Private home server.** Use a fresh onion identity and install one Tor v3
  client authorization key per device before starting Tor. An undisclosed
  hostname alone is insufficient for access control.

For private access, generate an X25519 key pair per device using the
[Tor client authorization guide](https://community.torproject.org/onion-services/advanced/client-auth/).
On the server, stage the guide's public key record in `phone.auth`, with
one `descriptor:x25519:<public-key>` line. Keep the private key on the
client. Install the public record while `zinder-tor` is stopped.

```bash
"${compose[@]}" run --rm -T --no-deps zinder-tor sh -ec '
  umask 077
  mkdir -p /var/lib/tor/zinder/authorized_clients
  cat > /var/lib/tor/zinder/authorized_clients/phone.auth
' < phone.auth
```

Tor enables authorization only when it loads at least one valid record.
Check the startup logs for authorization errors and verify that an
uncredentialed Tor client cannot connect. Stop Tor if that client reaches
either wallet port. When revoking the last credential, stop the service or
install a replacement before restarting it.

For either access mode, start Tor, check bootstrap progress, and read the
hostname from the state volume.

```bash
"${compose[@]}" up -d zinder-tor
"${compose[@]}" logs --tail=40 zinder-tor
"${compose[@]}" exec -T zinder-tor cat /var/lib/tor/zinder/hostname
```

Tor runs as `debian-tor`. A fresh `tor-state` named volume inherits the
image's ownership and directory mode `0700`. Keep the hidden service
directory at `0700` and its secret key at `0600`, owned by the Tor account.
Restrict any backup of that volume to the same trust boundary. Deleting the
volume without a backup loses the onion address. To update Tor, run
`"${compose[@]}" build --pull --no-cache zinder-tor` and recreate the sidecar
with `"${compose[@]}" up -d zinder-tor`.

## Connect and verify from outside

Use a separate machine with a running Tor client and a local SOCKS5 listener.
Pass the onion hostname through SOCKS5 so Tor resolves it. For private
access, install the device's `<onion-address-without-suffix>:descriptor:x25519:<private-key>`
record in a `.auth_private` file under the Tor client's `ClientOnionAuthDir`
as described in the Tor authorization guide. Keep the client authorization directory and file
private. [SOCKS5 domain-name addressing](https://www.rfc-editor.org/rfc/rfc1928)
lets the client hand off the name without a DNS lookup.

Run these empty-request gRPC probes with an HTTP/2-enabled curl. Set
`tor_socks` to the client's local SOCKS5 address and `onion` to the full
hostname. Expect `HTTP/2 200` and `grpc-status: 0` for both calls.

```bash
tor_socks=127.0.0.1:9050
onion='<your hostname including .onion>'
grpc_probe() {
  printf '\0\0\0\0\0' |
    curl -q --silent --show-error --fail --noproxy '' --max-time 120 \
      --socks5-hostname "$tor_socks" --http2-prior-knowledge \
      -H 'content-type: application/grpc' -H 'te: trailers' \
      --data-binary @- --dump-header - --output /dev/null "http://$onion:$1/$2"
}

grpc_probe 9067 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
grpc_probe 9102 zinder.v1.wallet.WalletQuery/ServerInfo
```

Repeat from a Tor client with no authorization for a private service.
Expect Tor to refuse the connection before any HTTP response. Also verify
that direct connections to the wallet ports fail on the host’s public and
LAN addresses. Verify that neither the host nor the onion address accepts
ops or control connections (`9100`, `9101`, `9105`, and `9108` in this
topology). Only the two configured onion ports should answer. If the probes fail after bootstrap,
check reader readiness, the port mappings, and client authorization first.

### Use a mobile wallet

Configure the phone's wallet to use its Tor integration or an on-device Tor
client. Confirm the app can send the onion hostname through SOCKS or a Tor
VPN, supports a custom gRPC endpoint, and uses the Tor client that holds the
authorization key for private access. Test from cellular data with the
custom endpoint selected. Confirm sync advances, then stop the endpoint
briefly and confirm the wallet reports a connection failure without falling
back to a bundled server. Wallet support claims require [end-to-end
certification](../reference/lightwalletd-compatibility.md#claim-boundary).

Tor v3 authenticates the onion service by its address and encrypts traffic
between the client and the service's Tor process. The reference mapping uses
plaintext HTTP/2 (h2c) from Tor to Zinder on local loopback. Use the exact
onion hostname from a trusted source. TLS over onion can satisfy a wallet
that requires HTTPS and extend encryption to a separate TLS termination
point. It requires an HTTP/2 TLS proxy, a certificate valid for the hostname
the wallet verifies, and a chain the wallet actually trusts. If you deploy
that setup, map the onion port to the TLS proxy on the shared loopback and
forward from there to Zinder using h2c. Keep the ops and control ports private. Both
options expose the decoded RPC to Zinder.

### Size timeouts for range streams

Measure Tor connection setup and `GetBlockRange` throughput from the wallet's
network. Budget connection setup for a cold rendezvous. Start with Tor's
adaptive circuit build defaults. `CircuitBuildTimeout`, `CircuitStreamTimeout`,
and `SocksTimeout` govern circuit setup or stream attachment. `MaxCircuitDirtiness`
controls reuse for new streams. See the [torrc manual](https://manpages.debian.org/trixie/tor/torrc.5.en.html).

Set the wallet's RPC deadline and any proxy's active-stream timeout above
the measured setup time plus the transfer time for a batch at its slowest
expected rate. For example, 20 seconds to connect plus 80 seconds to stream a
batch needs a deadline above 100 seconds. Use smaller contiguous batches
when the wallet cannot extend its deadline. Test a representative long range
and confirm the last requested height arrives with a successful gRPC status.
On interruption, reconnect and resume from the last verified height using the
wallet's continuity rules. Zinder's [send-stall limits](../reference/configuration.md#public-grpc-listeners)
still apply when the client or relay stops draining a stream.

Stop the route with `"${compose[@]}" stop zinder-tor`. Retain the state
volume if you plan to reuse the address.

## Privacy boundary matrix

| Observer | What the observer can learn |
| --- | --- |
| Indexer operator | Tor hides the wallet's source IP. The operator still sees RPC methods, parameters, connection reuse, request timing, response sizes, and any identifying application metadata. |
| Wallet's local network or ISP | Tor use, packet sizes, and timing are observable. The onion destination and RPC contents are encrypted on the Tor path. |
| Observer near both ends | Timing and traffic volume can support correlation. Matching distinctive queries or broadcasts can also link sessions without an IP address. |

See the [Tor v3 rendezvous specification](https://spec.torproject.org/rend-spec/index.html).
The tables below describe what the indexer receives in addition to the
transport metadata above. General chain reads can return the same public
data for identical requests against the same chain view. The selected
heights, pool filters, and resume positions are still visible. Local
trial decryption keeps the matching shielded notes on the wallet.
[ZIP 307](https://zips.z.cash/zip-0307#transaction-privacy) explains why
fetching only interesting full transactions leaks information.
[ZIP 314](https://zips.z.cash/zip-0314) is reserved, and
[ZIP 315](https://zips.z.cash/zip-0315) covers wallet practices for transparent
addresses.

### CompactTxStreamer

Availability depends on the [compatibility serving mode](../reference/lightwalletd-compatibility.md).

| RPC | What the operator learns and how to reduce disclosure |
| --- | --- |
| `GetLightdInfo`, `GetLatestBlock`, `GetLatestTreeState`, `GetStandardFee`, `GetMempoolStream` | General reads with no wallet-specific selector. Polling cadence and stream duration can identify usage patterns. |
| `GetBlock`, `GetBlockNullifiers`, `GetTreeState` | Selected block height or hash, which can indicate a birthday, checkpoint, or block of interest. Prefer shared checkpoints and scan wider ranges locally. |
| `GetBlockRange`, `GetBlockRangeNullifiers` | Inclusive height range, direction, and pool selection. Nullifier ranges request shielded data by block, without naming wallet nullifiers. Use contiguous shared scan ranges and match locally. |
| `GetSubtreeRoots` | Shielded protocol, start index, and entry limit. The starting index can reveal sync progress. |
| `GetMempoolTx` | Transaction-ID suffix exclusions and pool selection. Exclusions disclose what the client is filtering out and can help link polls. Omit exclusions when feasible and filter locally. |
| `GetTransaction` | One transaction ID, including during pending checks. Download all transactions in the relevant block where feasible or query your own indexer. |
| `GetTaddressTxids`, `GetTaddressTransactions` | Transparent address and height range. Both aliases stream raw transactions. Use your own indexer or scan chain data locally. |
| `GetTaddressBalance`, `GetTaddressBalanceStream`, `GetAddressUtxos`, `GetAddressUtxosStream` | Transparent addresses grouped within one request or stream. UTXO requests also reveal a height floor and result limit. Use your own indexer or scan locally. |
| `SendTransaction` | Full serialized transaction and submission time. Zinder forwards it to the configured node. Broadcast via your own node to avoid disclosing origination to a third-party wallet indexer. Node peers can still correlate propagation. |
| `Ping` | Requested test delay. Refused by default. Keep it disabled on wallet deployments. |

### WalletQuery

See the [native method availability](../architecture/wallet-data-plane.md#methods).

| RPC | What the operator learns and how to reduce disclosure |
| --- | --- |
| `ServerInfo`, `NetworkUpgradeActivations` | General reads with no wallet-specific selector. |
| `VisibleTipBlock`, `SettledTipBlock`, `LatestTreeStateCheckpoint` | General reads, with an optional epoch pin that can link related calls. |
| `BlockIdBySelector`, `BlockHeaderBySelector` | Selected block height or hash and any epoch pin. A targeted read can reveal a block of interest. |
| `CompactBlock`, `CompactBlocksInRange`, `FullBlock`, `FullBlocksInRange`, `TreeStateAtHeight` | Block height or range and any epoch pin. Prefer shared checkpoints and contiguous ranges, and match locally. Full blocks require retained block blobs. |
| `SubtreeRoots` | Shielded protocol, starting index, entry limit, and any epoch pin. |
| `MempoolSnapshot`, `MempoolEvents` | A page limit or start position, including cursors that link pages or resumed streams. These methods carry no address or transaction-ID filter. |
| `ChainEvents` | Stream family and start or resume position. An address filter discloses every selected transparent address and their grouping. Use an unfiltered stream and match locally. |
| `Transaction` | One transaction ID and any epoch pin, even when requesting status without transaction bytes. Use a local indexer for selective lookups. |
| `TransparentAddressUnspentOutputs`, `TransparentAddressTxIdsInRange` | One transparent address or script hash, the height selector, and any epoch pin. History requests also reveal pagination. A script hash can be matched against public scripts. Use your own indexer or scan locally. |
| `TransparentOutpointSpendStatus` | Exact transaction IDs and output indices, co-queried outpoints, and any epoch pin. Use your own indexer or track spends locally. |
| `BroadcastTransaction` | Full serialized transaction and submission time, as with `SendTransaction`. Use your own node to avoid third-party origination disclosure. |
| `ChainValuePoolsAtTip` | The release endpoint refuses this method as unavailable. The request carries no wallet-specific selector. |

[Oblivious HTTP (OHTTP)](https://www.rfc-editor.org/rfc/rfc9458.html#section-6)
can separate client addressing at a relay from request contents at a gateway
when they do not collude. The gateway still sees selectors. Private
information retrieval (PIR) can hide a selected record from an indexer if
the wallet and server implement a compatible PIR protocol. Neither is
provided by this profile. Self-hosting keeps queries within your own
operator boundary. It leaves public chain data and network broadcast
observations visible.
