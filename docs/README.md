# Zinder documentation

The documentation describes the code that exists in this repository. Proposed
work, implementation handoffs, validation diaries, and superseded transition
documents do not live here. Use Git history and GitHub issues for that context.

Start with these documents:

- [Indexer and wallet boundary](architecture/indexer-wallet-boundary.md) explains when Zinder is the right integration boundary.
- [Service boundaries](architecture/service-boundaries.md) assigns runtime and storage ownership.
- [Public interfaces](architecture/public-interfaces.md) is the vocabulary spine for Rust APIs, protocol fields, configuration, errors, and capabilities.
- [Canonical and materialized-view architecture](architecture/canonical-materialized-view-architecture.md) explains how canonical storage, wallet state, and explorer materialized views fit together.
- [Service operations](architecture/service-operations.md) defines health, readiness, metrics, security, and recovery behavior.
- [Testing](runbooks/testing.md) defines the validation tiers and commands.
- [Testnet performance](reference/testnet-performance.md) reports measured Testnet build times against lightwalletd-rs and Zaino.

## Architecture

- [Canonical and materialized-view architecture](architecture/canonical-materialized-view-architecture.md)
- [Chain ingestion](architecture/chain-ingestion.md)
- [Chain events](architecture/chain-events.md)
- [Storage backend](architecture/storage-backend.md)
- [Wallet data plane](architecture/wallet-data-plane.md)
- [Materialized-view plane](architecture/materialized-view-plane.md)
- [Explorer plane](architecture/explorer-plane.md)
- [Node source boundary](architecture/node-source-boundary.md)
- [Protocol boundary](architecture/protocol-boundary.md)
- [Public interfaces](architecture/public-interfaces.md)
- [Service boundaries](architecture/service-boundaries.md)
- [Service operations](architecture/service-operations.md)
- [Cipherscan adapter](architecture/cipherscan-adapter.md)
- [Zcash chain workload eras](architecture/zcash-chain-workload-eras.md)

Extension guides:

- [Extending artifacts](architecture/extending-artifacts.md)
- [Extending the wallet data plane](architecture/extending-the-wallet-data-plane.md)

## Reference

- [Chain data catalog](reference/chain-data-catalog.md)
- [Error vocabulary](reference/error-vocabulary.md)
- [Integration surfaces](reference/integration-surfaces.md)
- [Lightwalletd compatibility](reference/lightwalletd-compatibility.md)
- [Testnet performance](reference/testnet-performance.md)

## Runbooks

- [Initial sync](runbooks/initial-sync.md)
- [Bulk-catchup resource tuning](runbooks/bulk-catchup-resource-tuning.md)
- [Deploying on a VM](runbooks/deploying-on-a-vm.md)
- [Deploying on Railway](runbooks/deploying-on-railway.md)
- [Wallet-serving deployment (single-volume hosts)](runbooks/deploying-wallet-serving.md)
- [Trusted TLS and ZODL compatibility admission](runbooks/zodl-trusted-tls-certification.md)
- [Cipherscan adapter verification](runbooks/cipherscan-adapter-verification.md)
- [Releasing Zinder](runbooks/releasing.md)
- [Testing](runbooks/testing.md)

## Architecture decision records

The [ADR directory](adrs/) contains accepted decisions that still explain a
current invariant. Architecture documents describe the current system; ADRs
explain why an invariant exists. When an ADR no longer applies, replace its
remaining current truth in architecture or reference documentation and delete
the ADR instead of leaving a superseded record in the active documentation set.

## Maintenance rules

- Update documentation in the same change that alters a boundary, public name,
  protocol field, storage contract, configuration key, readiness rule, or
  operator workflow.
- State current behavior directly. Do not preserve migration phases, completed
  checklists, branch names, commit hashes, or dated evidence logs.
- Keep future work in tracked issues. Do not add `plans/`, `investigations/`, or
  `prd/` directories back to this documentation tree.
- Use names from [Public interfaces](architecture/public-interfaces.md), and
  prefer links to one authoritative explanation over repeating it.
