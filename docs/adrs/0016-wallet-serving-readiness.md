# ADR-0016: Wallet-serving readiness follows the serving pair

Status: Accepted

## Context

A wallet reader answers almost every request from its published serving pair. Only a few requests call anything else: a transaction broadcast calls the node, and mempool reads and lookups of unmined transactions call ingest control. Compact-blocks compatibility serving calls neither.

If readiness required the node and ingest control as well as the pair, any node outage or syncing node would refuse every stored read without making any request succeed. Deployment tooling would spread the outage further. A proxy that checks `/readyz` before each request would close the public route. A container supervisor would restart the container, and a restart during a node outage becomes a full outage, because ingest needs the node to start.

The Explorer and the Cipherscan adapter already keep serving through upstream outages for the same reason.

## Decision

1. **The serving pair alone admits traffic.** For native query and compatibility wallet serving, admission is the published canonical-and-wallet pair. For compact-blocks serving, it is the published canonical generation. Every rule that guards the served chain still refuses traffic; [Readiness](../reference/readiness.md#wallet-serving-readers) lists them.
2. **Dependencies degrade instead of refusing.** A failure of the node or ingest control is listed under `degraded` and never closes the traffic gate. The one exception is a parked writer's halt, which refuses traffic from any input.
3. **Each request fails on its own.** A node-backed request always calls the node, whatever the last probe saw, and a failed call returns `UNAVAILABLE` with `NODE_UNAVAILABLE`. A mempool request fails when ingest control does not answer. A probe observation can be up to a poll interval old, and request timeouts already bound each call.
4. **Mempool streams end while the writer's mempool waits on the node.** `MempoolEvents` replays retained events and then ends with `NODE_UNAVAILABLE` instead of staying silent, because silence reads as an unchanged mempool. This revised stream semantics in place, so it raised `contract_revision` ([ADR-0013](0013-chain-event-streams.md)).
5. **Losing ingest control is bounded by the staleness ceiling.** Without writer status, the reader keeps serving only until the staleness ceiling passes, because an unattested fence is a question about the served chain, not about a dependency.
6. **A halt seen through ingest control is sticky.** The pair publisher adopts it and refuses traffic. It reopens only after a writer status shows a writer that follows its node with a confirmed tip and attests the pair's fence.
7. **Readiness reports staleness.** The report and metrics carry `served_epoch_age_seconds`, the time since the writer committed the served epoch. `/readyz` answers 200 while the runtime admits traffic, whatever its dependencies report. Operators alert on served-epoch age and on degraded dependencies, not on a reader's 503.
8. **The single-host container does not restart on node outages.** Its entrypoint does not count an ingest that reports `upstream_not_ready` against its restart budgets, because that node answers. An ingest that reports `node_unavailable` for 24 hours ends the container, so a stuck node client still restarts eventually.

[Readiness](../reference/readiness.md#readiness-causes) lists the causes and report fields. [Service operations](../architecture/service-operations.md#recovery) describes recovery.

## Consequences

- Through a node outage, a syncing node, or an ingest-control outage within the staleness ceiling, readers keep serving stored data, as [Readiness](../reference/readiness.md#during-a-node-outage) lists. Wallets keep scanning.
- Broadcast fails per request while none of a reader's node endpoints answers. A load balancer keeps routing to such a reader, and nothing steers a broadcast to another reader.
- During a node outage the tip stops advancing, but `GetLightdInfo.estimated_height` still equals the served tip, so a lightwalletd client cannot see that the chain stopped. A wallet that applies the ZIP 203 default expiry of 40 blocks to a tip more than about 40 blocks behind builds transactions the network rejects. The served-epoch age alert must fire well inside that window on Mainnet.
- Native query never reports a node or ingest-control cause as its own. Those causes appear only under `degraded`.
- Wallet-mode compatibility admits ingest control at startup, so it fails to start against an ingest of another network or contract revision.
- An ingest restart during a node outage leaves readers refusing, because ingest needs the node to open its store and confirm its tip.

## Alternatives rejected

- **Keep readiness as the conjunction of every input.** It refuses every stored read without making any request succeed.
- **A new traffic-admitting cause such as `serving_degraded`.** It would compete with `serving_pair_stale` for the single cause slot and add an enum value consumers must learn. A separate list composes with every cause.
- **Fail node-backed requests fast while the probe reports an outage.** The observation can be stale, and the request timeout already bounds the call.
- **Serve indefinitely without ingest control.** The staleness ceiling bounds how long a reader serves an unattested fence, whatever the reason.
- **Never restart the container on an ingest node outage.** An ingest stuck in its own node client, with a healthy node, would never restart.
- **Bound `upstream_not_ready` the same way.** A node that answers with its sync state rules out a stuck client, so a restart during a long resync only turns a serving container into a full outage.
