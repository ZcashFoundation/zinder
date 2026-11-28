# Qualify a signed wallet on Android or iOS

Run one row per wallet build, platform, and network. The row identifies the signed product a user can install. The Zingo CLI, Zkool library, Vizor Rust engine, and Cake GraphQL engine have their own rows in the [release coverage baseline](coverage-v0.22.0.json).

## Identify the build and chain

Record the wallet name, app version and build number, package or bundle identifier, signing-certificate fingerprint or Apple team identifier, OS version, and whether the target is physical or emulated. Hash the exact installable artifacts. Android split installs need the hash of each APK. Verify the identifier and signature against the wallet distributor’s release information before using a funded fixture.

Set up the app using a network and endpoint selection supported by its signed build. Record the Zinder release commit, immutable fixture receipt, and a shared canonical block height and hash. Use the app’s normal TLS configuration. The controlled hostname and certificate chain must be trusted by the app, including any trust roots bundled by its networking library. Keep the control-node address and mining interface private to the operator. For a Testnet-only build, use an authorized Testnet account and record the observed confirmation depth. A peer-isolated Regtest chain supplies controlled mining and reorganization when the build supports it.

## Verify the user-visible result

1. Open a fresh wallet and record its public receiving identity as a fingerprint. Seed a separate source wallet and restore its existing activity into a fresh app installation or app data directory. Check the recovered account, history entry, and balance. Keep seed phrases and recovery files private.
2. Connect to the selected endpoint and verify the chain anchor. Record the app process’s connection to that endpoint. Stop the controlled route during a sync and verify that progress resumes when the route returns. Keep the wallet’s fallback behavior and any unrelated operating-system network activity identifiable separately.
3. Have an independent wallet send a shielded payment to the app. Record its pending and confirmed entry, amount, decrypted memo when exposed, and eventual spendable balance. Use the fixture node to verify the actual transaction’s canonical block.
4. Spend part of the received amount through the app’s normal signing and confirmation UI. Have the independent recipient verify the same transaction, amount, and memo. Reconcile the app’s debit with its displayed or independently verified fee, recording which source proved the fee.
5. Terminate the app process, reopen the persisted wallet, and advance the chain. Verify exactly one entry for each payment and that the displayed balances match. If testing background recovery, record backgrounding, reconnection, and renewed progress separately from a process restart.

A device adapter can emit the [schema-v2 lifecycle evidence](../../docs/runbooks/testing.md#gate-a-real-wallet-lifecycle) with `implementation: app`. Preserve the actual wallet observation behind each assertion. A node mempool entry alone leaves the app’s pending observation open. A server-side connection count alone leaves the app’s fallback behavior open. Keep raw traces and screenshots private and reference sanitized receipt digests from the release matrix. Mark a row blocked when a capable signed build, device, or trusted endpoint is unavailable.
