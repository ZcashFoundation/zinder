# Darkside protocol provenance

`darkside.proto` is vendored from [zcash/lightwalletd](https://github.com/zcash/lightwalletd)
at the revision recorded in `COMMIT`, with trailing whitespace normalized.
The source path is `walletrpc/darkside.proto`.

It imports the published wallet messages from `../lightwalletd/service.proto`.
The standalone `zcash/lightwallet-protocol` repository contains those wallet
schemas and does not distribute the test-control schema.
