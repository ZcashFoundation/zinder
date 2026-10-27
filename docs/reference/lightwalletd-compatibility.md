# Lightwalletd Compatibility Certification

Zinder can replace the server role of lightwalletd only for clients that speak
the vendored `CompactTxStreamer` protocol. It remains an adapter over
`WalletQueryApi`; it must not introduce a second chain-data model, wallet
state, or key-management surface.

## Claim boundary

The replacement claim applies to the network-facing server only. It excludes
wallet-local scanning, key custody, transaction construction, and user-facing
wallet UX.

Use a named level in release notes and operator docs:

| Level | Evidence |
| --- | --- |
| `protocol-compatible` | Pinned vendored proto, drift check, and generated-client integration coverage. |
| `reference-parity-compatible` | Live comparison against the pinned reference lightwalletd for the RPCs claimed. |
| `client-compatible` | A separately versioned lightwalletd client completes bootstrap, restore, transparent discovery, shielded-note discovery, send, and pending-transaction observation. |
| `public-operator-compatible` | The deployed endpoint has the required TLS/proxy, access, readiness, rate-limit, and redaction controls. |

Do not call the adapter a full or drop-in replacement without the protocol pin,
the claimed RPC set, and the highest evidence-backed level.

## Architecture

- `zinder-proto` owns the vendored protocol and its provenance.
- `zinder-query` owns the native `WalletQueryApi`, `WalletServingQuery`, and the
  shared exact-fence serving-pair publisher.
- `zinder-compat-lightwalletd` translates protocol messages, query values, and
  errors over an admitted exact-fence canonical/wallet-projection reader pair.
- `zinder-ingest` and `zinder-store` own compact blocks, tree states, subtree
  roots, and transaction bytes; `zinder-projector` owns the wallet projection
  that provides transparent UTXOs and transaction history.

In particular, `GetLightdInfo.taddrSupport` is an immutable serving-process
claim derived by the compatibility constructor. Construction admits it only
when the canonical pair retains transaction blobs (`transactions` or `all`)
and the exact wallet-serving pair provides the transparent reads; insufficient
retention fails before the listener binds. Later pair lag or provider outage
changes readiness without rewriting `GetLightdInfo`.

`GetLightdInfo` derives `consensusBranchId`, `upgradeName`, and
`upgradeHeight` from the network upgrade active at the served tip, and
`saplingActivationHeight` from the same table, on every request. In both
serving modes that table is the schedule the canonical writer admitted, so a
running compatibility process reports a newly admitted upgrade, such as NU7,
once its tip reaches the activation height. Before the writer's first
admission, it is the table discovered from the node at startup, which reader
admission requires to equal the store's construction table. See
[ADR-0039](../adrs/0039-writer-admitted-network-upgrade-schedules.md).

## Runtime serving selection

The compatibility binary selects its serving plane at runtime through the
operator configuration. Set `[compat].serving` in TOML, use the
`ZINDER_COMPAT__SERVING` environment variable, or pass `--serving` on the
command line. The only accepted values are `wallet` and `compact-blocks`;
omitting the setting preserves the migration-safe `wallet` default.

`wallet` admits the canonical and wallet projection pair and retains the
existing wallet-compatible behavior. `compact-blocks` admits only the
canonical secondary and rejects any `[wallet]` section or wallet settings;
its resolved configuration omits the wallet section. This selector is a
runtime operator contract, not a new Rust library export or a storage/proto
layout change.

Compact-block serving admits exactly `GetLightdInfo`, `GetLatestBlock`,
`GetBlock`, and `GetBlockRange`. Every other `CompactTxStreamer` RPC returns
`UNIMPLEMENTED`, including wallet queries, mempool methods, tree/subtree
methods, transparent-address methods, transaction submission, and ping.
`GetLightdInfo` reports the admitted network and tip with
`taddrSupport = false`; node discovery and liveness are still required for
that identity response.

## Certification work

1. Keep the protocol pin and generated-client coverage green. Updating the
   vendored proto is a protocol change, not an adapter-only change.
2. Run the live parity suite against pinned Zebra and reference-lightwalletd
   images. It compares observable values and status codes; intentional
   operator metadata differences remain allow-listed.
3. Keep the wallet-serving live tests focused on retained compact blocks, tree
   states, subtree roots, transaction bytes, transparent history and UTXOs, and
   strict below-floor failures.
4. Run the current Zcash Android SDK and ZODL lifecycle against the real
   Android-trusted TLS route. This is independent-client proof, not a
   substitute for protocol parity.
5. Before a public claim, validate the actual TLS/proxy deployment and record
   the exact commands, image digests, client version, network, and store floor
   with the release. No repository-side manifest schema is required.

## Reference inputs

The vendored protocol pin and parity workflow image digests are:

- `lightwallet-protocol` `v0.5.0`
  (`ac7cee052a1bf5d430985a478d39e8b513fc4bd4`);
- `lightwalletd` `v0.4.19`
  (`028401c4c4a7c8c386c81212324cc8083eed7510`) as the source reference;
- `electriccoinco/lightwalletd:v0.4.19@sha256:a3dfb04b4054b78ae3107dcc804c3a15a6e38d1f0dfcadeac48da482dd1d3448`;
- `docker.io/zfnd/zebra:6.2.2@sha256:f464a4bf44c3402b2c9063c6df686b9177a4659ea61aecdc8aeb6947f2173197`.

The pin is reproducibility, not evidence by itself. It makes a failing parity
run explainable and a passing one repeatable.
