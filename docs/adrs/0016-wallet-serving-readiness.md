# ADR-0016: Wallet-serving readiness follows the serving pair

Status: Accepted

## Context

A wallet reader answers almost every request from its published serving pair. Only a few requests call anything else: a transaction broadcast and the lightwalletd `GetStandardFee` call the node, and mempool reads and lookups of unmined transactions call ingest control. Compact-blocks compatibility serving calls neither. Light-wallet compatibility serving also sends mined transaction lookups and transparent address reads to the node ([ADR-0023](0023-deployment-profiles.md)).

If readiness required the node and ingest control as well as the pair, any node outage or syncing node would refuse every stored read without making any request succeed. Deployment tooling would spread the outage further. A proxy that checks `/readyz` before each request would close the public route. A container supervisor would restart the container, and a restart during a node outage becomes a full outage, because ingest needs the node to start.

The Explorer and the Cipherscan adapter already keep serving through upstream outages for the same reason.

## Decision

1. **The serving pair alone admits traffic.** For native query and compatibility wallet serving, admission is the published canonical-and-wallet pair. For compact-blocks serving, it is the published canonical generation. For light-wallet serving, it is the published canonical generation at a fence the writer attests, with an upgrade schedule the writer attests. Every rule that guards the served chain still refuses traffic; [Readiness](../reference/readiness.md#wallet-serving-readers) lists them.
2. **Dependencies degrade instead of refusing.** A failure of the node or ingest control is listed under `degraded` and never closes the traffic gate. The one exception is a parked writer's halt, which refuses traffic from any input.
3. **Each request fails on its own.** A node-backed request always calls the node, whatever the last probe saw, and a failed call returns `UNAVAILABLE` with `NODE_UNAVAILABLE`. A mempool request fails when ingest control does not answer. A probe observation can be up to a poll interval old, and request timeouts already bound each call.
4. **Mempool streams end while the writer's mempool waits on the node.** `MempoolEvents` replays retained events and then ends with `NODE_UNAVAILABLE` instead of staying silent, because silence reads as an unchanged mempool. An ordinary tip change keeps the stream open for up to 30 seconds while a replacement certifies. After that, the same refusal applies even if the source stays connected. This revised stream semantics in place, so it raised `contract_revision` ([ADR-0013](0013-chain-event-streams.md)).
5. **Losing ingest control is bounded by the staleness ceiling.** Without writer status, the reader keeps serving only until the staleness ceiling passes, because an unattested fence is a question about the served chain, not about a dependency.
6. **A halt seen through ingest control is sticky.** The pair publisher adopts it and refuses traffic. It reopens only after a writer status shows a writer that follows its node with a confirmed tip and attests the pair's fence.
7. **Readiness reports staleness.** The report and metrics carry `served_epoch_age_seconds`, the time since the writer committed the served epoch. `/readyz` answers 200 while the runtime admits traffic, whatever its dependencies report. Operators alert on served-epoch age and on degraded dependencies, not on a reader's 503.
8. **Lightwalletd tip calls carry the network height and refuse a stale tip.** The lightwalletd protocol has no field for the age of the served chain, so the compatibility runtime signals it through the tip calls its clients already check. `GetLightdInfo.estimatedHeight` is the height Zebra would estimate from the served tip, which keeps rising while the tip stands still. `GetLatestBlock` and `GetLightdInfo` fail with `UNAVAILABLE` once the served tip is older than `compat.stale_tip_limit_ms`, 25 minutes by default, counted from its block time or from when the process first served it, whichever is earlier, and the writer cannot vouch that the served tip is the network's tip, because the node is unreachable, reports that its peers show it behind the network, or has a newer block than the writer, the writer is starting or parked, or ingest control does not answer. A tip a reachable, in-sync node still reports is the chain tip however old it is, so a quiet chain keeps answering, as lightwalletd answers while its node is up. The vouching comes from the writer's status on ingest control, which carries the node's committed tip while the writer reaches its node. Every other call keeps serving stored data, and readiness is unchanged. [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md#stale-tip) lists how each wallet engine reacts.
9. **The single-volume container gives ingest a separate budget for an unreachable node.** A reachable node that reports it is syncing can finish without a restart. The 24-hour tolerance lets readers keep serving through an outage and bounds how long a stuck node client can go without a restart. If ingest reports `syncing` and query relays the mempool's `node_unavailable`, the longer budget applies only while ingest waits for a mempool snapshot at the node's tip. The writer can start preparing the next block while its reported heights still match. Its current wait reason keeps that work on the ordinary restart budgets.
10. **gRPC health reports the same admission.** The native and lightwalletd listeners serve `grpc.health.v1.Health` from the readiness state `/readyz` reads, with no second model, so `SERVING` means `/readyz` answers 200 and a degraded dependency does not change it. Health and gRPC reflection stay outside the traffic gate. Shutdown publishes `shutting_down` before the listener drains, so `/readyz` and health turn to `NOT_SERVING` together. [gRPC health](../reference/readiness.md#grpc-health) describes the service.

[Readiness](../reference/readiness.md#readiness-causes) lists the causes and report fields. [Service operations](../architecture/service-operations.md#recovery) describes recovery.

## Consequences

- Through a node outage, a syncing node, or an ingest-control outage within the staleness ceiling, readers keep serving stored data, as [Readiness](../reference/readiness.md#during-a-node-outage) lists. Wallets keep scanning.
- Broadcast fails per request while none of a reader's node endpoints answers. A load balancer keeps routing to such a reader, and nothing steers a broadcast to another reader.
- During a node outage, a light-wallet reader keeps serving block ranges, nullifiers, tree states, subtree roots, and the tip, so shielded scanning continues. Lookups of mined transactions whose bodies the store does not hold, transparent address reads, the fee, and broadcast fail per request with `UNAVAILABLE`. `/readyz` stays 200 and lists `node_source` under `degraded` once a probe sees the outage, up to one poll interval later. Readiness has no separate signal for node-backed methods, so a load balancer keeps sending them to that reader.
- During a node outage, lightwalletd wallets see `estimatedHeight` pull away from the served tip, and once the tip is 25 minutes old their tip calls fail, so they stop syncing and report the server unreachable instead of building transactions on a tip that has stopped. Wallets expire transactions 40 blocks after the tip they were told, 120 from NU7, which is 50 minutes of target time, so the default leaves half that window as margin.
- A wallet that cannot reach the tip calls stops syncing even though block ranges and tree states still answer. A private deployment that prefers to keep its wallets syncing through a long outage sets `compat.stale_tip_limit_ms` to 0, and its wallets then build transactions that expire on arrival once the tip is about 50 minutes old.
- A writer behind its node, catching up or restored from an old snapshot, also refuses the tip calls until its tip is within the limit or matches the node's, while `/readyz` reports it ready.
- On a quiet chain, Regtest included, the reachable node vouches for the tip, so the limit needs no per-network default. A Regtest node without peers whose `/ready` check is enforced on test networks reports `insufficient peers` and does not vouch.
- A node that answers but reports that its peers show it behind the network (Zebra's `syncing`, `insufficient peers`, or `no tip`) does not vouch for its tip, so the tip calls refuse an old tip there too. Not-ready reports computed from the tip's age (`tip_age`, `lag`, and the JSON-RPC fallback's) are ignored, because a quiet chain raises them too. Go lightwalletd keeps answering in that state, but a wallet shown that tip builds transactions that expire on arrival, and every engine in the compatibility evidence recovers from `UNAVAILABLE` on the tip calls.
- Native clients are unaffected: every native response carries the chain epoch, whose age they read.
- Native query never reports a node or ingest-control cause as its own. Those causes appear only under `degraded`.
- Wallet-mode compatibility admits ingest control at startup, so it fails to start against an ingest of another network or contract revision.
- An ingest restart during a node outage leaves readers refusing, because ingest needs the node to open its store and confirm its tip.

## Alternatives rejected

- **Keep readiness as the conjunction of every input.** It refuses every stored read without making any request succeed.
- **Close the light-wallet traffic gate while its node is down.** Shielded scanning needs no node, so closing the gate would refuse the profile's main workload while it still succeeds.
- **A new traffic-admitting cause such as `serving_degraded`.** It would compete with `serving_pair_stale` for the single cause slot and add an enum value consumers must learn. A separate list composes with every cause.
- **Fail node-backed requests fast while the probe reports an outage.** The observation can be stale, and the request timeout already bounds the call.
- **Serve indefinitely without ingest control.** The staleness ceiling bounds how long a reader serves an unattested fence, whatever the reason.
- **Never restart the container on an ingest node outage.** An ingest stuck in its own node client, with a healthy node, would never restart.
- **Keep `estimatedHeight` equal to the served tip.** No wallet's stale-server check can fire, and a wallet on a stopped tip builds transactions that expire on arrival.
- **Pass the node's own estimate through ingest control.** The node computes it with the same estimator from its own tip, so while the served tip follows the node the value does not change, and during an outage there is no node estimate to read.
- **Fail only `GetLatestBlock`.** The ZODL iOS SDK ranks servers by `GetLightdInfo` alone, so a stale server would keep ranking first.
- **Treat every `upstream_not_ready` report as behind.** Zebra's `tip_age` and `lag` reports and the JSON-RPC fallback's are computed from the tip's age, so a quiet Mainnet or Testnet chain would raise them and refuse the tip calls of every server serving the true tip.
- **Refuse on tip age alone.** Testnet often goes longer than 25 minutes without a block, and Mainnet miners occasionally stall, so every server of a quiet chain would refuse at once while it serves the true tip.
- **A limit counted in blocks.** NU7 shortens blocks to 25 seconds while the default expiry stays 50 minutes, so a block count would shrink the margin threefold.
- **Bound `upstream_not_ready` the same way.** A node that answers with its sync state rules out a stuck client, so a restart during a long resync only turns a serving container into a full outage.

## Revision history

- 2026-11-20. Bound how long a caught-up mempool stream can wait for a replacement generation.
- 2026-11-20. Decision 9 ties the node-outage budget to ingest’s current wait reason.
- 2026-11-23. Decision 1 adds light-wallet admission on an attested canonical generation, and the consequences state its node-outage behavior.
