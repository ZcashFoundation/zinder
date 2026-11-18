# ADR-0020: Build Explorer views as rebuildable, exactly versioned projections

Status: Accepted

## Context

The Explorer serves reads that canonical storage does not index, such as
fees, address activity, and spenders. Zinder builds these as materialized
views: derived tables in a separate RocksDB store, written as canonical blocks
arrive. Every view needs the same block facts, reorg handling, and crash
safety. If each view solved these itself, the views would drift apart and
replay cost would grow with every view.

Views must also survive change. A change to one view should not force a
rebuild of the others, and a release must never open a store it would
misread. A view added later must fill in history without blocking the live
chain, and readers must be able to tell partial results from complete ones.

## Decision

1. **Views are rebuildable projections of canonical data.** Ingest's replay
   host is the only writer, and views derive only from canonical data, never
   from the node. Canonical storage never waits on view progress and never
   deletes data because of view rows.
2. **One shared block context per height.** The replay host loads each
   block's facts once and passes them to every view. Views never fetch block
   data themselves.
3. **A closed registry declares every bundled view.** Each entry names the
   view, its schema version, its column families, and how replay drives it.
   The store manifest and the replay host's dispatch derive from that table.
4. **Rows and progress commit together.** A view's rows, its state, and its
   chain-event cursor are written in one batch. A batch lost in a crash loses
   its cursor too, so replay re-derives exactly those rows.
5. **Key encodings live in the core wire module.** A view that needs a new
   key shape adds a shared helper there instead of encoding bytes inline.
6. **Reverts are path independent.** After any sequence of reorgs, every
   view must hold the same bytes as a fresh store that applied only the
   winning branch, however blocks were batched. A view whose key does not
   start with height keeps a per-height undo index of what it wrote, kept
   only for the reorg window's newest heights, because no revert reaches
   below the settled tip
   ([ADR-0010](0010-reorg-window-and-displaced-blocks.md)).
7. **Admission is exact.** Every open, primary or secondary, requires the
   store's recorded container format and view manifest to match the running
   release exactly: the same view set, column families, and schema versions.
   A mismatch fails before the store is changed. Opening never clears rows,
   resets cursors, or creates missing views.
8. **Schema changes need a fresh store.** A key or payload change raises that
   view's schema version, and the operator rebuilds the views on a new path
   from canonical history that is complete from height 1. A view may add a
   payload field without a version change only when the current reader still
   decodes every older row.
9. **Coverage is explicit.** A view that fills history separately from its
   live tail records its epoch, tip, revision, and contiguous covered range.
   A request reads that record and the rows from one snapshot, and paging
   cursors fail closed when that fence goes stale. A response that claims
   completeness requires verified coverage through the fenced tip with a
   matching hash.
10. **Backfills are writer-owned background work.** They read canonical
    storage only, resume after a restart, and start only after canonical
    ingest follows the tip and view replay has caught up. A backfilled range
    joins the live tail only across a contiguous, hash-consistent boundary.

The mechanism, the bundled views, and the rebuild procedure are in
[Materialized-view plane](../architecture/materialized-view-plane.md).

## Consequences

- A new view implements apply and revert for one block, declares its schema,
  and adds one registry entry.
- Replay cost per height stays one fact load, however many views exist.
- Upgrading to a release that changes any view's schema, or adds or removes a
  view, requires rebuilding the views on a fresh path. Canonical storage and
  the wallet store are unaffected.
- A checkpoint-bootstrapped canonical store cannot host the views, because
  they need history from height 1.
- An additive payload field is forward-only. Rolling back past it requires
  rebuilding the affected views.
- Ingest readiness does not wait for backfills. Clients see partial coverage
  in the response and must not read it as complete.

## Alternatives rejected

- **Let each view load its own block data.** Replay cost would multiply by
  the number of views, and loading logic would drift.
- **One version for the whole store.** A change to one view would invalidate
  all of them.
- **Migrate a mismatched store on open.** Open would gain migration
  branches, while rebuilding from canonical history is already supported.
- **Revert by re-reading the replaced block.** After a reorg, canonical
  storage returns the new block, not the one whose rows the view wrote.
- **Infer completeness from a shared tip.** A shared tip cannot prove that
  one view is contiguous or that its rows come from the same snapshot.
