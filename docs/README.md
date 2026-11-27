# Zinder documentation

Pick the section that matches what you are doing. Each page states at the top what it answers and who it is for. For the full sidebar navigation tree, see [SUMMARY.md](SUMMARY.md).

## Start & Evaluate

- [Features and roadmap](features.md): why choose Zinder, reliability and verification guarantees, deployment profiles, packaging options, and what is supported in `v0.26.0` versus in flight or planned.
- [Use cases](use-cases.md): consumer integration patterns for light wallets, browser wallets, native backends, payment trackers, faucets, and explorers.
- [Indexer comparison](reference/indexer-comparison.md): Zinder beside Zebra, Zakura, ztreamer, Zingod, and Go lightwalletd, feature by feature.
- [How Zinder works](architecture/overview.md): the runtimes, the stores, and how a block becomes a wallet response.

## Deploy & Operate

Start with [Deploying Zinder](runbooks/deploying.md) for preset selection, `v0.26.0` host sizing, ports, cloud options, and the zero-TOML Docker Compose quickstart (or browse [deploy/README.md](../deploy/README.md)).

- [Deploying on a VM](runbooks/deploying-on-a-vm.md): Compose + systemd, Zakura, node failover, and `zinder` binary verification.
- [Deploying the single-volume container](runbooks/deploying-single-volume.md): single-volume `zinder serve` deployments on PaaS platforms (e.g., Railway) and Kubernetes `StatefulSet`.
- [Exposing over TLS](runbooks/exposing-over-tls.md): Caddy TLS proxy, client rate limits, and browser-wallet CORS origins.
- [Serve wallets over Tor](runbooks/tor-onion-service.md) with public or private access and see what wallet RPCs disclose.
- [Initial sync](runbooks/initial-sync.md): first-sync phases, progress metrics, and recovery.
- [Bulk-catchup resource tuning](runbooks/bulk-catchup-resource-tuning.md): tuning CPU, memory, and sort budgets during construction.
- [Upgrading the node](runbooks/upgrading-the-node.md): upgrading Zebra or Zakura without rebuilding stores.
- [Backups and edge snapshots](runbooks/private-backups.md): coherent private backups and `ADR-0024` verified edge-snapshot bootstrap.

## Integrate Clients

Start with [Integration surfaces](reference/integration-surfaces.md) for protocol selection, public Foundation endpoints, client error handling, and the [Rust quickstart](reference/integration-surfaces.md#quickstart).

- [Lightwalletd compatibility](reference/lightwalletd-compatibility.md): `CompactTxStreamer` serving modes, claim boundaries, and method behavior.
- [Wallet data plane](architecture/wallet-data-plane.md): native `WalletQuery` methods, epoch pinning, and range limits.
- [Chain events](architecture/chain-events.md): the resumable, reorg-aware event stream.
- [Explorer plane](architecture/explorer-plane.md): `ExplorerQuery` setup, freshness, and privacy refusals.
- [Cipherscan adapter](architecture/cipherscan-adapter.md): REST and WebSocket compatibility for an unchanged Cipherscan UI.

## Reference

- [Configuration](reference/configuration.md): settings, profiles, environment variables, and public listener limits.
- [Readiness](reference/readiness.md): `/healthz`, `/readyz`, gRPC health, and every readiness cause.
- [Metrics and logs](reference/metrics.md): Prometheus metrics, alert signals, and structured log fields.
- [Capabilities](reference/capabilities.md): `ServerInfo` capability strings and advertisement rules.
- [Error vocabulary](reference/error-vocabulary.md): `ErrorInfo` reasons, status codes, and retry dispositions.
- [Chain data catalog](reference/chain-data-catalog.md): what chain data Zinder stores and which methods serve it.

## Architecture, ADRs & Contribute

### Architecture and design decisions

- [Service operations](architecture/service-operations.md): startup phases, shutdown, security boundary, recovery, and runtime images.
- [Chain ingestion](architecture/chain-ingestion.md): canonical commit pipeline, reorgs, and node failover.
- [Node source boundary](architecture/node-source-boundary.md): how `zinder-source` talks to Zebra and Zakura.
- [Storage backend](architecture/storage-backend.md): RocksDB primaries, secondaries, and bounded memory.
- [Materialized-view plane](architecture/materialized-view-plane.md): rebuildable Explorer views and schema admission.
- [Public interfaces](architecture/public-interfaces.md): vocabulary spine, naming rules, and wire conventions.
- [Zcash chain workload eras](architecture/zcash-chain-workload-eras.md): Mainnet density eras for interpreting sync benchmarks.
- [Architecture decision records](adrs/README.md): why each lasting decision was made (ADR-0001 through ADR-0024).

### Contribute and maintainer operations

- [Testing](runbooks/testing.md): test tiers, the validation gate, CI, and live node tests.
- [Scripts index](../scripts/README.md): repository inventory of CI gates, release/SBOM scripts, benchmark runners, and wallet certification harnesses.
- [Releasing Zinder](runbooks/releasing.md): version bumps, changelogs, signed artifacts, and release recovery.
- [Measure a wallet-serving deployment](runbooks/serving-census.md): retained-interval availability and telemetry collection.
- [Compare wallet reader performance](runbooks/wallet-performance-qualification.md): paired reader regression and frozen Testnet benchmarks.
- [Capturing node fixtures](runbooks/capturing-node-fixtures.md): refreshing checked-in Zakura Regtest fixtures.
- [Restoring the Mainnet benchmark fixture](runbooks/mainnet-benchmark-fixture.md): bringing up frozen Mainnet GCP snapshots for drills and benchmarks.
- [Verify the Cipherscan adapter](runbooks/cipherscan-adapter-verification.md): end-to-end verification against a Cipherscan checkout.

## Keeping the documentation useful

- **Describe the code that exists.** Update a page in the same change that alters the behavior it describes. Keep migration history, dated evidence, and commit hashes out; issues and Git history hold those.
- **One page owns each topic.** Explain a topic once, in the page listed above, and link to it from everywhere else. Do not restate it.
- **Match the page to the reader's task.** A runbook gives steps and the result that confirms success. A reference page gives facts to look up. An architecture page explains how something works. An ADR records why.
- **Put the common case first.** Start with what most readers need, in few words. Exceptions and detail come after, under their own headings.
- **Update an ADR in place** when a decision in its domain changes. Add a new one only for a new domain.
- **Format for both GitHub and static doc sites.** Keep one `# H1` title per page, use repo-relative `.md` links rather than hardcoded commit URLs, label support status explicitly (`Supported`, `In flight`, or `Planned`), and keep tables narrow enough to scan on a phone.
