# ADR-0010: The reorg window matches the node's finalization bound

Status: Accepted

## Context

Zebra 7.1.0 and Zakura 1.8.6 finalize a block once their best chain grows 1000 blocks past it. Zebra can also finalize earlier under index pressure, which only makes the reorgs it can present shallower. A certified node therefore never presents a reorg deeper than 1000 blocks.

Zinder's reorg window bounds how far below the visible tip a replacement may start. A deeper replacement parks ingest with `reorg_window_exceeded`. Any window below the node's bound parks on reorgs the node itself produces: a 114-block Testnet reorg exceeded a 100-block window.

The window is part of store identity. The canonical store and the wallet projection both record it, so changing it used to mean rebuilding both stores. Two invariants constrain any change in place:

- The settled tip never moves backward. Chain-event consumers may discard undo state at or below it.
- Wallet undo rows cover exactly the blocks above the settled tip, and that suffix never exceeds the wallet's supported depth.

When a replacement commits, the best-chain indexes no longer hold the displaced branch. An Explorer process that tried to capture it afterwards would race the replacement and could mix epochs.

## Decision

1. **The default window is 1000 blocks.** Ingest, the projector, native query, and lightwalletd compatibility must all configure the same window.
2. **The window can be raised in place.** The settled tip holds still until the visible tip is a full new window above it, so no history is rewritten, the settled tip never moves backward, and the projector backfills no undo rows.
3. **The window is never lowered in place.** The writer and the projector refuse a smaller window without changing the store. Lowering requires a rebuild.
4. **Readers require the exact effective window.** A reader configured with a different window refuses to open the store, and an open reader refuses to catch up past a raise until it restarts with the new window.
5. **Ingest parks only on evidence of a fork.** A node that is merely behind Zinder's chain, as after a node restart loses its unfinalized blocks, makes ingest wait instead of park.
6. **A parked writer stays up and refuses wallet traffic.** It keeps its control plane, so readers refuse requests with the typed `REORG_WINDOW_EXCEEDED` instead of failing to connect. The halt is sticky: a later writer-status failure does not replace it with a cause that admits traffic.
7. **A restarted writer does not vouch for its fence until the node confirms it.** Wallet-serving readers refuse traffic until then, because Zinder fails closed rather than serve a fence nobody has confirmed.
8. **A deep replacement has its own memory budget.** A replacement holds its whole suffix in memory before its atomic commit. `ingest.follow.max_replacement_bytes` bounds it, and exceeding the bound ends ingest with a typed error before any write.
9. **The writer archives displaced blocks atomically.** The write batch that commits a replacement also records the displaced blocks' header facts, transaction ids, coinbase payouts, and any retained raw bytes. The archive is kept permanently. Its public methods report the height from which coverage starts and never claim to reconstruct earlier branches.

[Chain ingestion](../architecture/chain-ingestion.md) describes replacement and parking. [Readiness](../reference/readiness.md#readiness-causes) lists the causes readers report.

## Consequences

- The settled tip trails the visible tip by up to 1000 blocks, so surfaces that report finality from it report later.
- A raise is one-way. Earlier releases cannot read a raised store, so a binary rollback needs a rebuild.
- An operator who raises the window must restart every reader with the same value. A reader left on the old value fails closed.
- Wallet undo rows grow with the window. Projector construction bounds them with its own memory budget and fails rather than grow past it.
- Every writer restart leaves wallet readers unready until the writer's first node observation, and for as long as the node is unreachable.
- A replacement over its memory budget makes ingest exit, and a restart fails the same way until the operator raises the budget or the node leaves that branch.
- The displaced-block archive grows with every accepted reorg and needs capacity monitoring.

## Alternatives rejected

- **Backfill undo rows and settle at the new depth at once.** Blocks already settled would become replaceable again, which moves the settled tip backward and breaks consumers that discarded undo state.
- **Rebuild both stores on every window change.** It costs a full construction and a serving outage for a change that needs no history rewrite.
- **A smaller default, such as 300 blocks.** Any value below the node's bound still parks on reorgs the node can produce.
- **Restart the container after a park.** The restarted writer parks on the same reorg, and clients get refused connections instead of a typed refusal.
- **Capture displaced blocks later in the Explorer.** It races the atomic replacement and can mix epochs.
