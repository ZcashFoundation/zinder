# NU7 parser gate fixture

`zakura-1.6.0-regtest-v6-block.hex` is the raw block returned by `getblock`
from Zakura `v1.6.0+gebda15e23ed0` (source commit
`ebda15e23ed0189787e21b4625c6e4f62d10932f`) on an isolated Regtest whose
activation table sets `NU7 = 3`. The block is height 3 and contains a V6
coinbase with assigned NU7 branch ID `0x77190ad9`.

The captured hex text has SHA-256
`06169171e02aad50e3e9dcd1da417f5bd30d4cbc9d21da4b7a25fabd4ccb15f6`.
The decoded 319-byte block has SHA-256
`7976a427307fc3a86e63f9dabec1db2b25e738d2c727d848b07e6c72564f0fc4`.
The Zakura v1.6.0 Linux x86_64 release archive used for the capture has
SHA-256 `12842638072feb3b2fab500b60da979738d884b484194c4d2aef5220f7a3f372`.

The fixture is V6-only. No NU7 V5 fixture is available, so the parser gate
does not synthesize one or claim V5 assigned-branch coverage from this block.
