# lightwalletd Protocol Provenance

The vendored `compact_formats.proto` and `service.proto` files in this
directory are pinned to the upstream content below, with trailing whitespace
normalized for the repository hygiene gate:

- Repository: `https://github.com/zcash/lightwallet-protocol`
- Commit: `ac7cee052a1bf5d430985a478d39e8b513fc4bd4` (tag `v0.5.0`)
- Source paths:
  - `walletrpc/compact_formats.proto`
  - `walletrpc/service.proto`
- Verified on: 2026-07-05

Use the adjacent `COMMIT` file as the machine-readable source of this pin when
deciding whether a future upstream protocol change is an intentional Zinder
compatibility update or unreviewed drift.

The adjacent `VERSION` file holds the release tag that `COMMIT` points to.
`GetLightdInfo.lightwalletProtocolVersion` reports it, so update both files
together. Wallets such as zingolib parse that field as `vMAJOR.MINOR.PATCH` and
refuse to sync below the protocol version they require.

Verification:

```bash
# The tag must resolve to the pinned commit.
git ls-remote https://github.com/zcash/lightwallet-protocol \
  "refs/tags/$(cat crates/zinder-proto/proto/compat/lightwalletd/VERSION)^{}"

diff -u \
  <(perl -pe 's/[ \t]+$//' crates/zinder-proto/proto/compat/lightwalletd/compact_formats.proto) \
  <(curl -fsSL https://raw.githubusercontent.com/zcash/lightwallet-protocol/ac7cee052a1bf5d430985a478d39e8b513fc4bd4/walletrpc/compact_formats.proto | perl -pe 's/[ \t]+$//')

diff -u \
  <(perl -pe 's/[ \t]+$//' crates/zinder-proto/proto/compat/lightwalletd/service.proto) \
  <(curl -fsSL https://raw.githubusercontent.com/zcash/lightwallet-protocol/ac7cee052a1bf5d430985a478d39e8b513fc4bd4/walletrpc/service.proto | perl -pe 's/[ \t]+$//')
```
