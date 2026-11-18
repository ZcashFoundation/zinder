# Zinder documentation

Pick the section that matches what you are doing. Each page says at the top
what it answers and who it is for.

## Evaluate

- [Use cases](use-cases.md): what Zinder gives each kind of product, and what
  to enable for it.
- [Indexer comparison](reference/indexer-comparison.md): Zinder beside Zebra,
  Zakura, ztreamer, Zingod, and Go lightwalletd, feature by feature.
- [How Zinder works](architecture/overview.md): the processes, the stores, and
  how a block becomes a wallet response.

## Operate

Start with [Deploying Zinder](runbooks/deploying.md). It covers requirements,
how to choose a deployment, and a Docker Compose quickstart.

- [Deploying on a VM](runbooks/deploying-on-a-vm.md)
- [Deploying the single-volume container](runbooks/deploying-single-volume.md),
  including Railway
- [Exposing over TLS](runbooks/exposing-over-tls.md)
- [Initial sync](runbooks/initial-sync.md): what the first sync does and how to
  recover it
- [Upgrading the node](runbooks/upgrading-the-node.md)
- [Bulk-catchup resource tuning](runbooks/bulk-catchup-resource-tuning.md)

Look up:

- [Configuration](reference/configuration.md): settings, environment
  variables, and listener limits
- [Readiness](reference/readiness.md): `/healthz`, `/readyz`, and every
  readiness cause
- [Metrics](reference/metrics.md): metric names, alert signals, and log fields
- [Service operations](architecture/service-operations.md): security boundary,
  recovery, and supported deployments

## Integrate

Start with [Integration surfaces](reference/integration-surfaces.md). It shows
how each kind of client connects.

- [Lightwalletd compatibility](reference/lightwalletd-compatibility.md):
  `CompactTxStreamer` behavior, method by method
- [Wallet data plane](architecture/wallet-data-plane.md): what each native
  `WalletQuery` method returns
- [Chain events](architecture/chain-events.md): the resumable event stream
- [Explorer plane](architecture/explorer-plane.md): `ExplorerQuery`
- [Capabilities](reference/capabilities.md), [error vocabulary](reference/error-vocabulary.md),
  and [chain data catalog](reference/chain-data-catalog.md)

## Understand the design

- [How Zinder works](architecture/overview.md)
- [Chain ingestion](architecture/chain-ingestion.md)
- [Node source boundary](architecture/node-source-boundary.md)
- [Storage backend](architecture/storage-backend.md)
- [Materialized-view plane](architecture/materialized-view-plane.md)
- [Cipherscan adapter](architecture/cipherscan-adapter.md)
- [Public interfaces](architecture/public-interfaces.md): vocabulary, naming
  rules, and wire conventions
- [Zcash chain workload eras](architecture/zcash-chain-workload-eras.md)
- [Architecture decision records](adrs/README.md): why each lasting decision
  was made

## Contribute

- [Testing](runbooks/testing.md): test tiers, the validation gate, and live
  tests
- [Releasing](runbooks/releasing.md)
- [Capturing node fixtures](runbooks/capturing-node-fixtures.md)
- [Cipherscan adapter verification](runbooks/cipherscan-adapter-verification.md)

## Keeping the documentation useful

- **Describe the code that exists.** Update a page in the same change that
  alters the behavior it describes. Keep plans, migration history, dated
  evidence, and commit hashes out; issues and Git history hold those.
- **One page owns each topic.** Explain a topic once, in the page listed
  above, and link to it from everywhere else. Do not restate it.
- **Match the page to the reader's task.** A runbook gives steps and the result
  that confirms success. A reference page gives facts to look up. An
  architecture page explains how something works. An ADR records why.
- **Put the common case first.** Start with what most readers need, in few
  words. Exceptions and detail come after, under their own headings.
- **Update an ADR in place** when a decision in its domain changes. Add a new
  one only for a new domain.
