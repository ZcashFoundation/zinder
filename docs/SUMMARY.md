# Summary

[Zinder documentation](README.md)

# Start & Evaluate

- [Features and roadmap](features.md)
- [Use cases](use-cases.md)
- [Indexer comparison](reference/indexer-comparison.md)
- [How Zinder works](architecture/overview.md)

# Deploy & Operate

- [Deploying Zinder](runbooks/deploying.md)
  - [Deploying on a VM](runbooks/deploying-on-a-vm.md)
  - [Deploying the single-volume container](runbooks/deploying-single-volume.md)
  - [Exposing over TLS](runbooks/exposing-over-tls.md)
- [Initial sync](runbooks/initial-sync.md)
- [Bulk-catchup resource tuning](runbooks/bulk-catchup-resource-tuning.md)
- [Upgrading the node](runbooks/upgrading-the-node.md)
- [Backups and edge snapshots](runbooks/private-backups.md)

# Integrate Clients

- [Integration surfaces](reference/integration-surfaces.md)
- [Lightwalletd compatibility](reference/lightwalletd-compatibility.md)
- [Wallet data plane](architecture/wallet-data-plane.md)
- [Chain events](architecture/chain-events.md)
- [Explorer plane](architecture/explorer-plane.md)
- [Cipherscan adapter](architecture/cipherscan-adapter.md)

# Reference

- [Configuration](reference/configuration.md)
- [Readiness](reference/readiness.md)
- [Metrics and logs](reference/metrics.md)
- [Capabilities](reference/capabilities.md)
- [Error vocabulary](reference/error-vocabulary.md)
- [Chain data catalog](reference/chain-data-catalog.md)

# Architecture, ADRs & Contribute

- [Service operations](architecture/service-operations.md)
  - [Chain ingestion](architecture/chain-ingestion.md)
  - [Node source boundary](architecture/node-source-boundary.md)
  - [Storage backend](architecture/storage-backend.md)
  - [Materialized-view plane](architecture/materialized-view-plane.md)
  - [Public interfaces](architecture/public-interfaces.md)
  - [Zcash chain workload eras](architecture/zcash-chain-workload-eras.md)
- [Architecture decision records](adrs/README.md)
  - [ADR-0001: Store chain data in RocksDB](adrs/0001-rocksdb-canonical-store.md)
  - [ADR-0002: Serialization and byte order](adrs/0002-serialization-and-byte-order.md)
  - [ADR-0003: Epoch-bound reads and storage roles](adrs/0003-epoch-bound-reads-and-storage-roles.md)
  - [ADR-0004: Storage and deployment topologies](adrs/0004-storage-and-deployment-topologies.md)
  - [ADR-0005: Bounded process memory](adrs/0005-bounded-process-memory.md)
  - [ADR-0006: Node source boundary](adrs/0006-node-source-boundary.md)
  - [ADR-0007: Node failure recovery and transport policy](adrs/0007-node-failure-recovery-and-transport-policy.md)
  - [ADR-0008: Consensus parameters from the node](adrs/0008-consensus-parameters-from-the-node.md)
  - [ADR-0009: Ingest lifecycle and construction](adrs/0009-ingest-lifecycle-and-construction.md)
  - [ADR-0010: Reorg window and displaced blocks](adrs/0010-reorg-window-and-displaced-blocks.md)
  - [ADR-0011: Ingest control transport security](adrs/0011-ingest-control-transport-security.md)
  - [ADR-0012: Mempool topology and retention](adrs/0012-mempool-topology-and-retention.md)
  - [ADR-0013: Chain event streams](adrs/0013-chain-event-streams.md)
  - [ADR-0014: Consumer-neutral wallet data plane](adrs/0014-consumer-neutral-wallet-data-plane.md)
  - [ADR-0015: Wallet runtime composition](adrs/0015-wallet-runtime-composition.md)
  - [ADR-0016: Wallet-serving readiness](adrs/0016-wallet-serving-readiness.md)
  - [ADR-0017: Transparent wallet queries](adrs/0017-transparent-wallet-queries.md)
  - [ADR-0018: Transaction facts, status, and broadcast](adrs/0018-transaction-facts-status-and-broadcast.md)
  - [ADR-0019: Explorer plane](adrs/0019-explorer-plane.md)
  - [ADR-0020: Materialized views](adrs/0020-materialized-views.md)
  - [ADR-0021: Shared configuration sections](adrs/0021-shared-configuration-sections.md)
  - [ADR-0022: Release artifacts](adrs/0022-release-artifacts.md)
  - [ADR-0023: Deployment profiles](adrs/0023-deployment-profiles.md)
  - [ADR-0024: Verified edge snapshot bootstrap](adrs/0024-verified-edge-snapshot-bootstrap.md)
- [Testing](runbooks/testing.md)
  - [Measure a wallet-serving deployment](runbooks/serving-census.md)
  - [Compare wallet reader performance](runbooks/wallet-performance-qualification.md)
  - [Capturing node fixtures](runbooks/capturing-node-fixtures.md)
  - [Restoring the Mainnet benchmark fixture](runbooks/mainnet-benchmark-fixture.md)
  - [Verify the Cipherscan adapter](runbooks/cipherscan-adapter-verification.md)
- [Releasing Zinder](runbooks/releasing.md)
