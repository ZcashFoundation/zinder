# ADR-0038: Wallet runtime composition and capability discovery

| Field | Value |
| --- | --- |
| Status | Accepted |
| Product | Zinder |
| Domain | Native wallet serving, capability discovery, consumer admission |
| Related | [Wallet data plane](../architecture/wallet-data-plane.md), [Public interfaces](../architecture/public-interfaces.md), [Canonical storage topology](0035-canonical-storage-topologies.md), [Canonical storage access](0003-canonical-storage-access-boundary.md), [ADR-0043](0043-owned-transparent-wallet-query-surface.md) |

## Context

The native wallet runtime historically had two query implementations. A
primary-store implementation exposed the complete method set, while the
standalone release binary used an exact canonical-and-wallet secondary pair.
Several exact-pair methods were not implemented, so the binary selected
`WalletCapabilityProfile::ExactPair` to suppress their capability strings.

That profile described an implementation accident rather than the running
endpoint. Capability discovery also accepted independently configured booleans
for storage retention and optional providers. The resulting descriptor could
disagree with persisted storage, the admitted serving pair, installed handlers,
or ingest-control wiring. It could advertise structurally unavailable
operations and omit operations whose retained artifacts were present.

Zinder controls its native consumers and can make a coordinated breaking
change. Compatibility aliases would preserve two vocabularies and the invalid
states this decision removes.

## Decision

Zinder has one production native wallet-query composition. It reads through an
admitted `WalletServingReadPair`; every request captures one immutable pair at
an exact canonical fence.

The admitted query privately owns one immutable
`NativeWalletEndpointCapabilities` value. That value is derived once, after
composition and admission, from concrete evidence:

- the serving pair's authenticated persisted `RawBlobRetention` and projection
  evidence;
- the methods implemented by the production query;
- admitted upstream-node capabilities and the concrete tree-state and
  broadcaster dependencies; and
- any authenticated ingest-control dependencies whose methods the native
  adapter exposes.

There are no public setters, capability-string constructors, deserialized
profiles, or caller-populated support booleans. `WalletCapabilityProfile`,
`Complete`, `ExactPair`, and `WalletAdvertiseInputs` are removed without
aliases. Adding a native capability requires updating the protocol registry,
the concrete implementation, evidence derivation, positive and negative
contract tests, and a current production-consumer flow.

`ServerInfoSettings` becomes `WalletEndpointMetadata`. It contains endpoint
identity and descriptive facts such as network, build revision, schema
version, and reorg window; it contains no support decisions. The wallet query
opens no materialized-view store, so its metadata names no materialized-view
identity. Retention-duration fields with no authenticated writer evidence are
removed rather than populated from reader configuration. The production gRPC
adapter accepts the admitted query and metadata,
obtains the query-owned capability set, and encodes that exact set in
`WalletQuery.ServerInfo`. The operational HTTP endpoint is bound only after
composition and admission and receives the same immutable value. Native and
operational discovery therefore cannot be configured or paired independently.

The static table in `zinder-proto::capabilities` remains the source of protocol
identifiers, owning surface, method association, and semantic version. It does
not decide which subset a process contains.

The descriptor is deliberately native-scoped. `WalletServingQuery` is a domain
read implementation shared by the native and lightwalletd compatibility
adapters, so an in-process domain method may exist for compatibility without
being admitted on the native endpoint. Method existence is not a native support
claim. Each protocol adapter owns its admission guard and must derive its
advertised contract from the concrete providers installed for that adapter.

`WalletServingPairSlot` is an opaque read handle. Only
`WalletServingPairPublisher` can replace its pair, and every request captures
one pair before reading. Test and library callers cannot mutate the slot or
install a replacement that bypasses retention, fence, or source-identity
admission.

The release query admits one probed `NodeSource` and installs clones of that
same source as its tree-state and broadcast providers. A
node-backed capability requires the corresponding probed node feature, the
concrete provider, and `TipId`, which is the liveness prerequisite. The release
query also admits one `AdmittedIngestControl` before opening storage or binding
traffic. Admission opens an authenticated channel once, calls `ServerInfo`,
and requires the exact `zinder-ingest` service identity, network, contract
revision, and five control methods needed by the composition:
`ServerInfo`, `WriterStatus`, `MempoolSnapshot`, `MempoolTransaction`, and
`MempoolEvents`. The query, serving-pair publisher, live
wallet handlers, and readiness probe clone that same accepted channel. They do
not accept independently configured provider identities or perform a second
dial at their use sites.

This evidence admits native transaction lookup and mempool snapshot and
events. Transaction-byte support additionally requires authenticated
transaction-blob retention. The native transparent surface contains only the
methods a packaged consumer calls
([ADR-0043](0043-owned-transparent-wallet-query-surface.md)); the transparent
mempool point lookups and the native transparent balance are not part of the
protocol. Chain value pools remain omitted: discovering an OpenRPC method name does
not prove that the node returns the required `valuePools` payload, and the
readiness loop does not retain that semantic evidence. Method presence in the
protocol, adapter, or upstream schema is not support.

Every native RPC except `ServerInfo` checks its method capability in the native
adapter before invoking a domain method, touching storage, or contacting a
provider. Optional response fields have their own field capability checks. This
keeps direct native calls honest even when a client ignores discovery without
incorrectly applying native policy to the compatibility adapter. Bounded range
capabilities also carry enforced production limits; an advertised range method
is not permission for unbounded work. `SubtreeRoots` accepts at most 1,024
entries per request across both query implementations and rejects a larger
range before pair capture or storage access with
`SUBTREE_ROOT_RANGE_TOO_LARGE`.

## Support, readiness, and request outcomes

Structural support is immutable for the process lifetime. A structurally
supported operation is advertised even while one of its admitted dependencies
is temporarily unhealthy; readiness becomes false and the request returns
`UNAVAILABLE` with `SERVICE_NOT_READY` plus the stable `readiness_cause`
metadata. A structurally absent operation is omitted and a direct call returns
a precise unsupported or failed-precondition result. An expired chain view
retains its capability and returns the typed epoch-expiry outcome. A
replacement pair that changes the process contract is rejected, leaves the
prior pair authoritative where safe, and makes readiness false.

Capability discovery is not a health check. Pair publication, provider health,
and replica lag may change readiness but never rewrite the endpoint contract.

`WalletServingReadiness` retains pair state, admitted-node state, and
ingest-control health independently and projects their conjunction into the
shared operational and gRPC readiness handle. Shutdown dominates later writes.
The node liveness task uses `tip_id` plus upstream synchronization health
through the exact admitted source; it never repeats capability discovery or
mutates the cached structural feature set. The ingest-control task calls
`WriterStatus` and `MempoolSnapshot(max_entries = 1)` through the admitted
channel, validates coherent network and tip evidence, and publishes
`ingest_control_unavailable` on failure. It does not repeat `ServerInfo`
admission. The task logs only outage and recovery transitions, and shares the
serving-pair refresh cadence because both observe the same control plane. An
unexpected serving-pair publisher, node-readiness task, or
ingest-control-readiness task exit drains readiness, cancels the runtime, and
returns a typed process failure.

## Storage and pair admission

`RawBlobRetention` is part of canonical store identity and is authenticated
through construction, append, replacement, reopen, secondary catch-up, and
serving-pair admission. The reader declares the retention it expects and fails
without mutation when the persisted identity differs.

`RawBlobRetention::All` makes serialized full-block reads structurally
available. `RawBlobRetention::Transactions` retains transaction bytes but omits
full-block capabilities. The query never repairs missing historical blobs from
the upstream node. Changing retention on a non-empty store requires building a
new store and performing a blue-green cutover; it is not a runtime toggle.

Pair rotation may advance the canonical fence but must preserve retention and
every other input to the immutable endpoint contract. A candidate that changes
those inputs is not published.

The serving-pair publisher opens a secondary generation slot the first time it
uses it, with the full cold admission that
[ADR-0003](0003-canonical-storage-access-boundary.md) requires. Once the pair
a generation published is neither retained nor held by a request, the
publisher takes back sole ownership of that generation's canonical and wallet
secondaries and catches them up in place for a later candidate. That candidate
passes the same writer attestation, exact-fence, and source-identity checks as
a cold one before it is published. Catch-up authenticates store control, build
identity, and the replay rows that the chain events since the last catch-up
changed. It does not repeat the cold scan of retained history. A generation
whose pair is still held is never caught up. A generation whose catch-up or
publication fails is closed, and its slot is opened cold when it is next used.

A drained generation stays open, and an open secondary keeps open the
primary files its table cache holds, including files the primary has since
compacted away. The publisher therefore reuses drained generations in turn,
starting after the generation it last published, so every opened generation is
caught up again within a few publications and releases the files it no longer
references. It opens a slot it has not used only when no opened generation
has drained, so the number of cold opens stays fixed once every slot is open.

A secondary holds at most its budget's `max_open_files` table files open, so
its view can also name a file it does not hold. When the canonical writer's or
the wallet projector's compaction deletes such a file, a read through that
generation that needs the file fails with a missing-file I/O error. The error
lasts until the secondary catches up past the deletion or is reopened. A
published pair is never caught up, so it would keep failing those reads until
the writer's fence moved. A read that fails this way therefore marks its pair:

- The marked pair leaves retention at once. A pin no longer captures it, and
  a capture returns it only while it is still the current pair.
- The read that marks the pair wakes the publisher. The publisher prepares a
  candidate as it does for a new block, even when the writer's fence has not
  moved, and publishes it at the writer's fence. It prepares none while the
  writer is parked, because readiness already refuses traffic then. Later
  reads that find the marked pair missing a file wait for the same
  publication without waking the publisher again. When the replacement cannot
  be published yet, such as while every inactive generation is taken, the
  periodic refresh retries it.
- The read waits up to five seconds for that publication. It then runs once
  more on the pair that answers its epoch. An unpinned read uses the new
  current pair. A pinned read, and a stream that continues at its own epoch,
  uses only a pair at that epoch.
- A pinned read whose epoch only the marked pair served fails with
  `CHAIN_EPOCH_PIN_UNAVAILABLE`. A read whose replacement is not published in
  time fails with its storage error, which clients retry.

Corruption, admission, and every other storage failure still fail the read
without marking the pair. A marked generation drains and is recycled like any
released one, and its catch-up drops the deleted files from its view. A
candidate whose RocksDB open or catch-up fails because the primary deleted a
file it had just listed is a candidate failure: like replica lag, it leaves
the published pair serving until the staleness ceiling. The canonical
store-control check that follows each open and catch-up reports such a
deletion as an admission refusal, so that refresh fails closed like any other
admission failure.

Marking a pair does not change readiness. The refresh that replaces it reports
readiness as any refresh does, so a candidate failure that does not leave the
published pair serving fails closed even when the writer's fence has not
moved.

Each publication carries the writer's fence at the moment its candidate
converges. The publisher does not publish one pair for each height, so blocks
that commit within one refresh share one publication at the later height.

Blocks also share a publication when they arrive faster than retention allows.
Every generation other than the current one may hold a replaced pair inside
its retention window or a pair that a request still holds. While all of them
do, the publisher publishes nothing. The first refresh after a generation
frees publishes the writer's latest fence, and the blocks committed in the
meantime get no publication of their own. The publisher never opens an extra
generation to keep up.

## Consumer admission and certification

Native consumers such as Zallet own the exact capability requirements for each
packaged feature combination. They fetch `ServerInfo` and report the complete
missing set before opening or migrating wallet storage. A capability profile
named after a consumer is not added to Zinder.

Zallet treats an expired exact view as a typed retry condition and reacquires
the whole workflow boundary. Zinder retains a bounded history of serving pairs.
Each replaced pair answers reads pinned to its epoch for one minute after it
was replaced, however fast later blocks arrive. Publication never releases a
replaced pair before its minute ends; it waits for a free generation instead.
Retention lets a multi-request workflow whose reads carry `at_epoch_id`, such
as a transparent UTXO refresh across a wallet's receivers, finish across new
blocks without changing the expiry contract. A pair republished at its own
epoch, as a schedule revision does, leaves retention at once. The pair that
replaced it answers the same epoch for longer. A transparent-history first
page carries no wire pin and is answered from the current pair, so a history
walk that starts a new receiver after a publication still restarts. Consumer
tests must prove that a retry does not commit a partial block batch.

A request holds the pair it reads, and a held pair keeps its secondary
generation open. Once the publisher releases a pair from retention, requests
may keep reading it for at most 30 seconds more. Past that read limit the
publisher revokes reads on the pair: the reads that keep a pair across several
pages, the transparent unspent-output and full-block streams, stop at their
next page or while they wait for a slow client and end with
`CHAIN_EPOCH_PIN_UNAVAILABLE`, the same typed expiry as a released pin. A
consumer treats the partial result as void and restarts at a newer epoch.
These streams also read storage only as fast as the client consumes and stop
reading when the client cancels or disconnects, so an abandoned read releases
its pair at once. A slow or abandoned read therefore delays publication by at
most the read limit after its pair leaves retention. Retention and slow reads
together delay a publication by at most the retention window, the read limit,
and one page read, about 90 seconds, and never stop publication.

ZODL continues to consume the separate lightwalletd-compatible
`CompactTxStreamer` surface through trusted TLS at the deployment edge. Native
capability strings and full-block requirements do not leak into that protocol.
The compatibility runtime derives its own structural support from retained
storage and concrete providers and fails before binding when a required
provider is absent.

Zinder contract tests prove internal storage, composition, discovery, error,
and protocol invariants. Real current Zallet and ZODL binaries provide separate
consumer certification. Fixture-backed parity tests remain useful contract
evidence but are not downstream certification.

## Consequences

- Operators configure storage retention and dependency endpoints, not an
  implementation profile or a set of overlapping support flags.
- Invalid capability claims become unconstructable at the release composition
  root, and missing structural requirements fail before traffic is accepted.
- Full-block service has explicit disk, ingestion, secondary catch-up, and
  migration cost.
- Tests and alternate compositions must supply the same concrete evidence as
  production or make no support claim.
- Capability additions require a named production flow and end-to-end evidence;
  speculative provider registries, consumer modes, and generic capability
  crates are unnecessary.

## Rejected alternatives

### Configure or rename the profile

Making the profile configurable, or renaming it to a runtime or storage mode,
preserves a shadow model that can disagree with the composed service.

### Configure individual capability booleans

Independent booleans maximize invalid states and make operational intent
ambiguous. Persisted facts and admitted providers are the authority.

### Derive support from the protocol registry alone

The registry proves vocabulary and descriptor coverage. It cannot prove that
storage retained an artifact or that a required provider was composed.

### Add a generic capability-provider framework

There is one production composition root. A provider hierarchy adds
indirection without removing the need to bind each claim to its concrete
handler and evidence.

### Retain historical pairs indefinitely

Unbounded pair retention has unbounded storage and resource semantics. Every
retained pair keeps its own canonical and wallet secondaries open. One minute
of retention for each replaced pair covers a multi-request workflow across a
burst of blocks, and typed view expiry with whole-workflow retry covers the
rest.

### Release the oldest retained pair at each publication

A count bound beside the time bound kept a generation free for every block,
but it tied a pin's lifetime to block spacing. While each publication waited
about 25 seconds for a cold open, the next two publications took about a
minute, so the count bound never cut a pin short. Once publication followed
each block within about a second, four Testnet blocks published within 11
seconds released a pinned epoch about six seconds after it was replaced. A
wallet whose transparent UTXO refresh reads about 30 receivers under one pin
in about 21 seconds lost its pin in 2 of 11 refreshes. A time-only bound keeps
a pin's lifetime independent of block spacing. It costs publication latency
only when more than three blocks arrive within one minute.

### Retain more pairs

Each extra generation keeps another canonical and wallet secondary pair open
with its own block caches, against the memory a wallet-serving reader budgets
for four. More pairs still bound a pin by a number of blocks, so a fast enough
burst still releases it.

### Protect only epochs that a read has pinned

Waiting only for pairs that have answered a pinned read would keep one
publication per block when no client pins. A client learns its epoch from an
unpinned read and pins it in a later request, so a burst between those two
requests could still release the epoch. A pinned read can also race the
publication that releases its pair. The time-only rule needs no per-pair mark
and has no such race.

### Answer a pin on an ancestor epoch from the current pair

Unspent outputs, transaction status, tree state, and balances differ between
epochs, so such an answer would mix two epochs, which a pin forbids. Only
height-addressed blocks at or below the pinned tip are equal in both, and the
current pair still cannot attest the pinned epoch's chain view.

### Let requests hold a released pair until they finish

A read cannot be bounded by its request shape alone: an address can hold
millions of unspent outputs, and a client can stop consuming a stream without
cancelling it. Unbounded holds let three such reads occupy every spare
generation, after which no pair is published for any client until the reads
end or the process restarts. Revoking reads on a released pair after a fixed
limit bounds that delay, and the typed expiry already tells consumers to
restart.

### Open a new generation for every publication

Cold admission scans the retained canonical history, so its cost grows with
the store rather than with the block. On a Testnet store it took about 25
seconds, which delayed every publication by that long. All the blocks that
committed during one open shared its publication, so one Testnet deployment
published pairs at only 304 of 605 heights. Catching a drained generation up
costs only what the new blocks changed, which shrinks the span of blocks that
share a publication from one cold open to one refresh.

### Pre-open a warm candidate

Opening the next generation before the block arrives still pays one cold
admission per block in reader I/O and memory, and a block that arrives before
the open finishes still waits for it. Reusing a drained generation removes the
cold open from the steady state instead.

### Reuse the lowest-numbered drained generation

Once a burst of blocks has opened all four slots, a search that always starts
at the first slot finds the first or second slot drained at every
publication spaced more than one retention window apart, and alternates
between them. The other two stay open without being caught up, and they keep
their compacted-away primary files for as long as the blocks stay spaced.

### Close a drained generation after an idle window

Closing a generation that stays drained past a fixed window also releases its
files, but block intervals vary. With 75-second blocks, the generation the
next block would reuse regularly sits drained longer than any window short
enough to matter, and that block waits for a cold open again. Reusing drained
generations in turn bounds the files they keep by publications and adds no
cold open.

### Serve only the current pair

Expiring a pin at the next publication fails any workflow that spans a block.
On a chain with 25-second blocks, a wallet that reads its transparent
receivers under one pin in about 20 seconds fails about half of its walks.

### Keep every table file of a secondary open

A secondary opened with `max_open_files = -1` holds every table file open, so
a file the primary deletes stays readable through its descriptor. It also
keeps every table's metadata resident and every compacted-away file on disk
for as long as the generation is open, for four generations at once. The
bounded resource budget
([ADR-0020](0020-bounded-rocksdb-resource-budget.md)) refuses negative values
for that reason.

### Catch the published pair up in place

Catching the current pair up would drop the deleted file from its view, but
it would also move the fence that in-flight reads and pins on that pair rely
on. A replacement pair at the writer's fence costs one recycled catch-up and
keeps every published pair immutable.

### Retry on the current pair regardless of the pin

A pinned read retried on a pair at another epoch would mix two epochs, which a
pin forbids. The pin error tells the client to restart the workflow instead.

### Add compatibility aliases

Aliases keep obsolete names and constructors available for new code. The
controlled consumer cutover uses the current contract without shims.

## Revision history

- 2026-11-16: A read that finds a table file of its serving pair deleted by
  the primary's compaction marks the pair. The publisher replaces the pair at
  the writer's fence even when the fence has not moved, and the read runs once
  more on the replacement when that pair answers the read's epoch. A pinned
  read whose epoch only the marked pair served fails with
  `CHAIN_EPOCH_PIN_UNAVAILABLE`. A candidate whose RocksDB open or catch-up
  fails on such a file keeps the published pair serving.
- 2026-11-16: Each replaced serving pair answers reads pinned to its epoch for
  one minute after it was replaced, however fast later blocks arrive.
  Publication no longer releases the oldest retained pair, and a retained pair
  no longer yields its generation to a held one. While no generation is free,
  the publisher publishes nothing, and the first refresh after one frees
  publishes the writer's latest fence. A pair republished at its own epoch
  leaves retention at once.
- 2026-11-16: The serving-pair publisher catches a drained generation up in
  place for its next publication, instead of opening new secondaries for every
  block. Cold admission runs when a slot is first used and after a failed
  catch-up or publication. Drained generations are reused in turn, so each
  one releases its compacted-away primary files within a few publications.
  Each publication still carries the writer's latest fence, so blocks that
  commit within one refresh share it.
- 2026-11-15: Requests may keep reading a released serving pair for at most
  30 seconds. The publisher then revokes reads on it, and the transparent
  unspent-output and full-block streams end with
  `CHAIN_EPOCH_PIN_UNAVAILABLE`. The unspent-output stream reads storage one
  bounded page at a time as the client consumes it and stops when the client
  goes away.
- 2026-11-14: The two most recently replaced serving pairs keep answering
  reads pinned to their epochs, each for at most one minute after it was
  replaced, so a workflow whose reads carry `at_epoch_id` survives a new
  block. A transparent-history continuation resumes on the pair that issued it
  while that pair is retained; a first history page still answers from the
  current pair.
- 2026-11-12: Ingest-control admission requires five control methods instead
  of seven, and `WalletEndpointMetadata` no longer carries a materialized-view
  preset. The transparent mempool point lookups, the native transparent
  balance, and the unserved canonical outpoint readers were removed from the
  protocol ([ADR-0043](0043-owned-transparent-wallet-query-surface.md)).
