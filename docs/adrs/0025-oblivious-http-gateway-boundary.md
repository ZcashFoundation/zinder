# ADR-0025: Terminate Oblivious HTTP at an external gateway

Status: Accepted

## Context

Direct wallet connections expose both the request and the client's network
address to the server. Oblivious HTTP (OHTTP) splits those observations
between an independent relay and a gateway. This decision defines the
boundary for [#707](https://github.com/ZcashFoundation/zinder/issues/707).
Gateway implementation and deployment qualification are pending.

## Decision

1. **Keep the gateway outside the binary.** An external sidecar or proxy in
   [`deploy/`](../../deploy/README.md) terminates RFC 9458 OHTTP, RFC 9180 HPKE,
   and RFC 9292 Binary HTTP in front of the `WalletQuery` and
   `CompactTxStreamer` gRPC-Web listeners
   ([#729](https://github.com/ZcashFoundation/zinder/pull/729)).
   `bin/zinder` holds zero HPKE private keys and opens no decapsulation
   listener. The gateway has no durable application or wallet state.

   The HPKE KEM is `DHKEM(X25519, HKDF-SHA256)` (`0x0020`) and the KDF is
   `HKDF-SHA256` (`0x0001`). The allowed AEADs are `ChaCha20Poly1305`
   (`0x0003`) and `AES-128-GCM` (`0x0001`).
2. **Publish gateway keys on the gateway.** Serve `application/ohttp-keys`
   at `GET /.well-known/ohttp-gateway` with `Cache-Control: max-age=3600`.
   Rotate every 7 days. Keep a retired key for decapsulation only for
   48 hours, then delete it. Wallet `ServerInfo` carries no HPKE key config.
3. **Allow bounded unary calls.** Use an explicit method allowlist that
   includes `SendTransaction` and `BroadcastTransaction`. Enforce request
   and response caps of 2 MiB and 4 MiB respectively, with bucketed padding.
   Refuse long-lived streams including `GetBlockRange`,
   `CompactBlocksInRange`, `ChainEvents`, and mempool streams.
4. **Reject replays and strip identifiers at the edge.** Gate forwarded
   calls with a bounded, time-windowed HPKE `enc` strike register. Validate
   the encapsulated request time and reject repeated `enc` values or
   expired requests before forwarding. Keep the register ephemeral and
   refuse broadcast when replay protection cannot be enforced.

   After a gateway restart or any loss of the strike register, disable
   forwarding `SendTransaction` and `BroadcastTransaction` under every
   retained HPKE key that could have accepted requests before the loss.
   Keep broadcast forwarding disabled under those keys until every
   previously acceptable request has expired. The holdoff covers the full
   replay window and all allowed clock skew.

   Strip client IPs and identifying headers before forwarding, and exclude
   them from gateway logs. The independent relay owns per-client rate limits.

## Evidence

- Decisions 1 and 4: External constraint. RFC 9458 defines the
  relay/gateway trust boundary and permits a time-windowed `enc` register
  for replay protection. RFC 9180 defines the HPKE algorithms and RFC 9292
  defines the message encoding. [RFC 9458 §6.5.1](https://www.rfc-editor.org/rfc/rfc9458.html#section-6.5.1),
  [RFC 9180](https://www.rfc-editor.org/rfc/rfc9180.html),
  [RFC 9292](https://www.rfc-editor.org/rfc/rfc9292.html).
- Decision 1, placement and suite selection: Judgment. Keeping private
  keys at the edge limits where they are held. Reopen if an independent
  relay and a supported wallet cannot interoperate through the gateway
  using these suites.
- Decisions 2 and 3: Judgment. The times and size limits bound stale-key
  acceptance and work per call. Sustained rotation failures or a required
  unary wallet flow that cannot fit within the caps would reopen the
  corresponding limit.
- Decision 4, rate-limit placement and replay bounds: Judgment. The relay
  sees the client address and the gateway sees the request. Reopen if
  independent-relay tests expose a client identifier or allow a replay
  to trigger a second broadcast.
- Decision 4, recovery after replay-state loss: Judgment. Retained HPKE
  keys can still decrypt pre-restart requests after the strike register
  clears. Qualification requires a gateway restart/replay test that retains
  an affected key and replays a previously forwarded broadcast while its
  timestamp is still valid. Verify that forwarding stays blocked through
  the latest allowed expiry, including clock skew. After the holdoff, a
  fresh broadcast must forward and the expired replay must still be refused.
  Reopen Decision 4 if the test permits duplicate broadcast forwarding.

## Consequences

- Gateway operators rotate keys and qualify requests through an independent
  relay. Privacy relies on the relay and gateway remaining independent.
- Wallets use a separate transport for long-lived streams.
- The gateway sees query contents. A private lookup can compose with a
  [PIR sidecar](0026-private-information-retrieval-sidecar-boundary.md).

## Alternatives rejected

- **Decapsulate OHTTP in `bin/zinder`.** It would put HPKE keys and an
  additional public listener in the canonical serving binary.
- **Forward long-lived streams through the bounded gateway.** They cannot
  fit the request/response buffering and padding contract.
- **Rate-limit on client addresses at the gateway.** It would recreate the
  identity link that the independent relay removes.
