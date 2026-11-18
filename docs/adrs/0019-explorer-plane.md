# ADR-0019: Run the Explorer as an optional runtime over canonical data

Status: Accepted

## Context

Block explorers, dashboards, and analytics need reads that wallets do not,
such as spenders, fee and value-pool history, search, and time ranges. In the
wallet runtime, every wallet deployment would carry their storage and replay
cost.

Explorer responses also combine several independent kinds of freshness: the
canonical chain epoch, how far the materialized views have replayed, what the
node reports, and the mempool snapshot. One "current height" number cannot
tell "caught up" from "unknown".

Search takes free-form input, including shielded addresses and viewing keys.
An empty result for a shielded address would wrongly suggest it has no
history, when its history is private by protocol design.

Block header times can repeat and can go backwards. A height-ordered scan
cannot prove that every block in a wall-clock interval was included.

## Decision

1. **The Explorer is its own optional runtime.** `zinder-explorer` serves
   `ExplorerQuery`. Its binary, `[explorer]` config section,
   `ZINDER_EXPLORER__*` variables, `explorer.*` capabilities, and
   `zinder_explorer_*` metrics share one name. The reusable view library keeps
   `MaterializedView*` names, because it describes a pattern rather than this
   product ([ADR-0020](0020-materialized-views.md)).
2. **Canonical data is the only authority.** The Explorer reads canonical
   facts through `WalletQuery` and through materialized views that ingest
   derives from canonical events. Its own node use is limited to parsing
   transaction bytes and optionally observing node health for freshness. It
   never follows the chain itself and never falls back to the node when a
   canonical fact is missing. A view that needs a new fact extends the node
   and canonical boundaries first.
3. **Every response states its freshness on independent axes.** A shared
   `ChainView` carries the chain epoch, the views' indexed tip, the node's
   observed tip, and view replay health. An absent axis means unknown or not
   applicable, never "at tip". All axes come from the same pinned snapshots
   as the response data. `ExplorerFreshness` adds the mempool snapshot age,
   the capability that produced the response, and a typed list of fields that
   could not be filled. Zero is never used to mean absent.
4. **Missing optional data degrades the response, not the request.** When a
   view's coverage does not reach the response's tip, the handler leaves the
   affected field empty and names it in the unavailable list. A request
   that cannot be answered without a dependency the deployment lacks fails
   with a typed reason from the
   [error vocabulary](../reference/error-vocabulary.md).
5. **Search returns typed candidates and refuses private inputs.** Each
   candidate names its kind: block, transaction, transparent address, TEX
   address, unified address, shielded address, viewing key, or unclassified.
   Shielded addresses, viewing keys, and shielded receivers inside a unified
   address get a successful, typed `NotPubliclyIndexable` answer. The
   classifier never opens storage for those inputs and never echoes a viewing
   key. A TEX address is shown with its transparent form, which has public
   history.
6. **Time-range reads are indexed by time, height, and hash.** A view keeps
   one row per canonical block under that key, so equal and out-of-order
   timestamps are handled without special cases. Completeness is proved by
   coverage over heights, not by assuming timestamps increase. A continuation
   cursor binds the request bounds, the epoch, and the view's frozen tip. Paging
   continues while the chain grows, and a reorg at or below the frozen tip
   invalidates the cursor.
7. **Product labels stay in adapters.** Zinder exposes canonical payout
   facts and exact fees. A product adapter maps them to labels such as mining
   pool names.

Methods, capabilities, and response fields are described in
[Explorer plane](../architecture/explorer-plane.md).

## Consequences

- Wallet deployments do not run the Explorer or its view store.
- Every Explorer view can be rebuilt from retained canonical inputs. A new
  fact that canonical storage lacks needs a canonical schema change first.
- Clients can tell privacy refusals, incomplete source facts, unsupported
  parsing, and lag apart without reading message text.
- Search needs no view store and no search index.
- Long time-range scans have stable membership while the chain grows, and
  fail closed across a reorg that touches them.

## Alternatives rejected

- **Let the Explorer read the node for missing facts.** Its answers would
  mix chain views and stop being reproducible from canonical data.
- **Name the view library after the Explorer.** Other consumers of the
  pattern would inherit a product name they do not own.
- **A single height or lag number for freshness.** It cannot distinguish
  canonical visibility, node progress, and view coverage, or tell absent
  from zero.
- **Empty results or gRPC errors for shielded search inputs.** An empty
  result implies no history. An error breaks the response shape for a valid
  answer.
- **A persisted search index.** Classification is local and point lookups
  already use canonical indexes.
- **Height-ordered scans for time ranges.** They cannot prove completeness
  when header times repeat or go backwards.
