# Testnet performance

On one Testnet machine, Zinder's speed depends on the job:

- **Small block summaries only**: current Zinder was faster than
  lightwalletd-rs v0.1.1. Across 3 runs it took about 32% less time,
  which is about 1.47 times faster.
- **Full wallet index, including transparent-address history**: Zinder
  was slower than lightwalletd-rs in the 1 run we measured, because it
  builds that index in a second pass after it copies the chain.
- **Against Zaino v0.7.0**: Zinder finished that full-index job much
  faster in that same 1 run. We did not re-run Zaino in the
  small-summary mode.

lightwalletd-rs v0.1.1 and Zaino v0.7.0 are two other Zcash indexers.
These Testnet results used one machine, the same Zebra full node, and
the same point on the chain. They are not Mainnet numbers, and they
do not prove Zinder is ready for production.

## The two jobs we timed

An indexer copies Zcash history from a full node, then answers wallet
requests from that copy. We timed two different ready points.

Small-summary job: copy the chain, then serve the small block
summaries wallets use to catch up. Current Zinder can do this without
building the extra wallet index. lightwalletd-rs v0.1.1 stores those
summaries, and we compared Zinder with that product. See
[Lightwalletd compatibility](lightwalletd-compatibility.md) for the
serving mode.

Full-index job: copy the chain, then also build Zinder's transparent
wallet index so the server is ready to answer full wallet queries,
including transparent-address history. lightwalletd-rs still stored
summaries only. Zaino v0.7.0 stored a broader finished index.

The clock started when the process started on an empty disk, and it
stopped when a request for one block returned the expected hash. That
one-block request is `GetBlock`. The clock measures time to that first
answer, not later request speed.

## Small summaries: faster than lightwalletd-rs (3 runs)

On 17 August 2026 we ran the small-summary job 3 times. Zinder copied
the chain and started the summary server. It did not build the wallet
index, and it did not build explorer pages. We compared it only with
lightwalletd-rs v0.1.1, the same binary as the full-index test. Zaino
was not in this test.

| Run | Zinder | lightwalletd-rs | Zinder took less time by |
| --- | ---: | ---: | ---: |
| 1 | 248.152 s | 364.255 s | 31.87% |
| 2 | 243.152 s | 361.422 s | 32.72% |
| 3 | 243.147 s | 356.164 s | 31.73% |

Zinder took about 32% less time on average, or about 1.47 times
faster. Almost all of Zinder's time was the chain copy (about 242 to
247 seconds). Starting the summary server added about 1 second.

Zinder kept copying new Testnet blocks up to the newest block on the
node, near 4.28 million blocks, while lightwalletd-rs was stopped soon
after the comparison block. Zinder still finished sooner.

### Memory and disk (2 of the 3 runs)

Memory and disk numbers come from runs 2 and 3 only, because run 1 did
not record trustworthy resource figures.

| Run | Product | Peak memory | Store while running | Writes |
| --- | --- | ---: | ---: | ---: |
| 2 | Zinder | 1.49 GiB | 6.16 GB | 7.04 GB |
| 3 | Zinder | 1.36 GiB | 6.11 GB | 7.04 GB |
| 2 | lightwalletd-rs | 1.20 GiB | 2.33 GB | 4.35 GB |
| 3 | lightwalletd-rs | 1.19 GiB | 2.33 GB | 4.33 GB |

After we stopped the processes, each Zinder store used 5.7G on disk,
and each lightwalletd-rs store used 2.1G. Zinder writes a full chain
copy plus a summary view, and lightwalletd-rs stores less and was
stopped earlier.

## Full wallet index: slower than lightwalletd-rs, faster than Zaino (1 run)

On 13 and 14 August 2026 we ran the full-index job once. Zinder copied
the chain and then built the transparent wallet index.
lightwalletd-rs v0.1.1 stored the small summaries only. Zaino v0.7.0
stored a broader finished index.

Time from an empty disk until the products could answer the wallet
catch-up requests that all 3 share:

| Product | Time to ready |
| --- | ---: |
| lightwalletd-rs v0.1.1 | 360.845 s (about 6 minutes) |
| Zinder | 892.782 s (about 15 minutes) |
| Zaino v0.7.0 | 20,396.699 s (about 5 hours 40 minutes) |

Zinder finished the chain copy in 214.130 seconds, already faster than
lightwalletd-rs at 360.845 seconds. A second wallet-index pass then
took 674.637 seconds, and that second pass is why full-index Zinder
was 2.47 times slower than lightwalletd-rs.

Against Zaino, Zinder reached the same ready point 22.85 times faster,
with lower memory, disk, writes, and CPU.

| Product | Peak memory | Store while running | Writes |
| --- | ---: | ---: | ---: |
| lightwalletd-rs v0.1.1 | 1.16 GiB | 1.73 GiB | 4.04 GiB |
| Zinder | 2.35 GiB | 10.69 GiB | 22.62 GiB |
| Zaino v0.7.0 | 9.69 GiB | 20.43 GiB | 146.96 GiB |

## What matched

In both jobs, the one-block request for the comparison block returned
the same hash. In the full-index run, a 100-block slice also matched
across Zinder, lightwalletd-rs, and Zaino. In the small-summary runs,
that 100-block slice did not match: the bytes are written differently
in this mode, so it is not yet a proven match.

Requests that report the newest block or the server's name still
differ, because those answers include a moving chain tip and the
product's name.
Those two requests are `GetLatestBlock` and `GetLightdInfo`; the
100-block slice request is `GetBlockRange`.

## What these numbers do not prove

Each method got 1 request after the store was ready, so these times are
not a study of later request latency. Results on another host, another
disk, or Mainnet can differ.

## For Zinder contributors

This section is for people who change Zinder, not for operators choosing
a deployment.

The small-summary runs used current Zinder after summary-only serving
shipped. They ran chain ingest and the compatibility server in that
mode, with no wallet-index builder and with explorer pages off. The
full-index run used the serial wallet-index pass after the chain copy,
and that second pass is the measured gap versus lightwalletd-rs when
the product must answer transparent-address history.

The unmatched 100-block slice in summary-only mode is a byte-layout
difference, not a failed one-block check.
