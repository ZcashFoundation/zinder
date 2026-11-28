# Scripts inventory

Every tracked script, harness, fixture helper, and policy asset under `scripts/`, grouped by role. For workflow triggers and test tiers, see [Testing](../docs/runbooks/testing.md) and [Releasing Zinder](../docs/runbooks/releasing.md).

## CI & Policy Gates

| File | Purpose |
| --- | --- |
| [check-native-proto-closure.sh](check-native-proto-closure.sh) | Verifies the native `zinder-proto` descriptor closure has no missing or drifting imports |
| [check-sdk-dependency-requirement.sh](check-sdk-dependency-requirement.sh) | Checks dependency requirements for publishable SDK crates |
| [check-sdk-package-policy.sh](check-sdk-package-policy.sh) | Enforces MSRV, metadata, and workspace policy for publishable SDK crates |
| [check-zcash-upstream-alignment.sh](check-zcash-upstream-alignment.sh) | Checks pinned `zebra-chain`, `librustzcash` crate cohort, and Zebra/Zakura digests |
| [generate-api-artifacts.sh](generate-api-artifacts.sh) | Builds API artifacts and packages the versioned archive when given a release tag |
| [check-api-artifact.sh](check-api-artifact.sh) | Compares an API archive with its source and runs Buf and protoc from a standalone extraction |
| [check-published-proto-compatibility.sh](check-published-proto-compatibility.sh) | Checks protobuf compatibility against the most recently published release and latest stable release carrying the contract archive |
| [install-ci-tool.sh](install-ci-tool.sh) | Installs SHA-256 verified prebuilt CI tools pinned in `.github/ci-tools.lock` |
| [regenerate-zinder-proto.sh](regenerate-zinder-proto.sh) | Regenerates (`--write`) or drift-checks (`--check`) `crates/zinder-proto/generated/` |
| [runbook-lint.sh](runbook-lint.sh) | Lints documented test commands and `bash -n` syntax of fenced `bash` blocks in runbooks |
| [test-canonical-storage-defaults.py](test-canonical-storage-defaults.py) | Tests canonical storage configuration defaults and `data_dir` path derivation |
| [test-changelog.sh](test-changelog.sh) | Self-tests changelog fragment validation and release prep scripts |
| [test-deployment-admission.sh](test-deployment-admission.sh) | Self-tests deployment admission checks against passing and failing deployment fixtures |
| [test-compat-lightwalletd-caddyfile.sh](test-compat-lightwalletd-caddyfile.sh) | Validates the reference Caddyfile and checks its TLS protocols, readiness gate, and upstream forwarding with Docker |
| [test-install-ci-tool.sh](test-install-ci-tool.sh) | Self-tests `install-ci-tool.sh` checksum verification and download failures |
| [test-lthash16-backends.sh](test-lthash16-backends.sh) | Verifies `lthash16` backend equivalence across feature flags |
| [test-personal-compose.sh](test-personal-compose.sh) | Starts the personal-node Compose bundle on a disposable Regtest chain and checks readiness |
| [test-nextest-selectors.sh](test-nextest-selectors.sh) | Verifies `.config/nextest.toml` profile selectors and test tier filters |
| [test-runbook-lint.sh](test-runbook-lint.sh) | Tests Bash syntax checks for top-level fences and fences inside numbered steps |
| [test-workflow-action-pins.sh](test-workflow-action-pins.sh) | Verifies every external GitHub Action under `.github/` is pinned to a full commit SHA |
| [validate-changelog.sh](validate-changelog.sh) | Validates `.changes/unreleased/*.yaml` fragments or the exact PR no-note waiver |
| [validate-deployment-admission.sh](validate-deployment-admission.sh) | Static policy gate validating deployment classes and Compose, Kubernetes, Prometheus, and release-packaging contracts |
| [verify-published-sdk.sh](verify-published-sdk.sh) | Smoke-tests published `zinder-core`, `zinder-proto`, and `zinder-client` crates |
| [verify-sdk-packages.sh](verify-sdk-packages.sh) | Packages and offline-verifies workspace SDK crates via `cargo package` |

## Release Build, SBOM & Provenance

| File | Purpose |
| --- | --- |
| [build-release-binary-archive.sh](build-release-binary-archive.sh) | Builds reproducible Linux and macOS binary archives containing `bin/zinder` and checksums |
| [check-release-binary-archive.sh](check-release-binary-archive.sh) | Verifies release binary archive layout, checksums, and binary execution checks |
| [check-release-image-evidence.sh](check-release-image-evidence.sh) | Validates container image manifest, digest, and admission evidence |
| [check-release-sbom.sh](check-release-sbom.sh) | Validates generated SPDX/CycloneDX SBOM artifacts for releases |
| [extract-release-notes.sh](extract-release-notes.sh) | Extracts section release notes for a tag from `CHANGELOG.md` |
| [find-github-release.sh](find-github-release.sh) | Looks up existing GitHub releases and asset metadata by tag |
| [generate-release-sbom.sh](generate-release-sbom.sh) | Generates release SBOM artifacts for binaries and container images |
| [install-release-gh.sh](install-release-gh.sh) | Installs pinned `gh` CLI tooling for release workflows |
| [package-release-images.sh](package-release-images.sh) | Packages and manifests `ghcr.io/zcashfoundation/zinder` OCI release images |
| [prepare-changelog-release.sh](prepare-changelog-release.sh) | Rolls pending `.changes/unreleased/*.yaml` fragments into `CHANGELOG.md` for a release |
| [release-binary-platform.sh](release-binary-platform.sh) | Resolves release platform names, CPU baselines, and native library metadata for the archive scripts |
| [render-personal-compose.sh](render-personal-compose.sh) | Renders the release Compose asset with both images pinned by digest |
| [resolve-release-image-manifest.sh](resolve-release-image-manifest.sh) | Resolves `deploy/release-images.json` into concrete image build matrix entries |
| [test-github-release-lookup.sh](test-github-release-lookup.sh) | Unit-tests `find-github-release.sh` |
| [test-release-binary-archive.sh](test-release-binary-archive.sh) | Unit-tests `build-release-binary-archive.sh` and `check-release-binary-archive.sh` |
| [test-release-image-publication.sh](test-release-image-publication.sh) | Tests release image publication manifest resolution and verification logic |
| [test-release-sbom.sh](test-release-sbom.sh) | Unit-tests release SBOM generation and checks |
| [test-release-tag.sh](test-release-tag.sh) | Unit-tests `validate-release-tag.sh` |
| [validate-release-tag.sh](validate-release-tag.sh) | Validates SemVer release tag syntax, branch/commit lineage, and changelog state |
| [verify-release-image-attestations.sh](verify-release-image-attestations.sh) | Verifies Cosign signatures and SLSA build provenance attestations on published OCI images |

## Observability & Live Certification

| File | Purpose |
| --- | --- |
| [capture-zakura-shielded-fixtures.sh](capture-zakura-shielded-fixtures.sh) | Captures deterministic shielded Regtest fixtures from a live Zakura node |
| [generate-live-indexer-tls.sh](generate-live-indexer-tls.sh) | Generates ephemeral local TLS certificates for live node/indexer testing |
| [mine-live-regtest.sh](mine-live-regtest.sh) | Advances blocks on a live Regtest Zebra or Zakura stack |
| [observability-smoke.sh](observability-smoke.sh) | Smoke-tests Prometheus scrape targets, Grafana dashboards, and alert rules |
| [prepare-wallet-tls-smoke.py](prepare-wallet-tls-smoke.py) | Prepares local Caddy TLS config and smoke checks for wallet endpoints |
| [report-publication-delay.py](report-publication-delay.py) | Measures block-tip to served-epoch publication delay across streams and readers |
| [run-wallet-lifecycle.py](run-wallet-lifecycle.py) | Drives automated wallet sync and spend lifecycle scenarios against Zinder |
| [test-container-resource-evidence.sh](test-container-resource-evidence.sh) | Tests container resource-usage evidence capture and threshold validation |
| [test-metric-names.py](test-metric-names.py) | Verifies Prometheus metric names match documented rules and exporter contracts |
| [test-observability-rules.py](test-observability-rules.py) | Lints Prometheus alert and recording rule definitions under `observability/` |
| [test-observability-smoke-safety.sh](test-observability-smoke-safety.sh) | Tests `observability-smoke.sh` cleanup and safety guards |
| [test-publication-delay.py](test-publication-delay.py) | Unit-tests `report-publication-delay.py` |
| [test-wallet-lifecycle.py](test-wallet-lifecycle.py) | Unit-tests `run-wallet-lifecycle.py` |
| [trace-wallet-endpoint.py](trace-wallet-endpoint.py) | Captures diagnostic gRPC/HTTP traces against a live wallet endpoint |
| [verify-wallet-tls.sh](verify-wallet-tls.sh) | Verifies TLS, ALPN (`h2`), HTTP/2, and gRPC-Web headers on a public proxy |
| [wallet-lifecycle.example.json](wallet-lifecycle.example.json) | Example lifecycle scenario manifest for `run-wallet-lifecycle.py` |

## Benchmarks & Chain Fixtures

### Top-level benchmark & fixture runners

| File | Purpose |
| --- | --- |
| [bench-checkpoint-policy-mainnet.py](bench-checkpoint-policy-mainnet.py) | Replays the hash-pinned Mainnet archive for the four-arm checkpoint policy comparison |
| [compact-range-campaign.example.json](compact-range-campaign.example.json) | Example campaign manifest for compact-range benchmark runs |
| [compare-benchmark-campaign.py](compare-benchmark-campaign.py) | Compares candidate and baseline benchmark campaigns against regression thresholds |
| [compare-wallet-readers.py](compare-wallet-readers.py) | Runs paired reader performance comparisons against frozen Testnet oracle endpoints |
| [regtest-fixture-relay.py](regtest-fixture-relay.py) | Relays and controls Regtest node state during reproducible wallet certification runs |
| [regtest-fixture.py](regtest-fixture.py) | Brings up disposable Regtest fixture nodes and Zinder runtimes for testing |
| [run-benchmark-campaign.py](run-benchmark-campaign.py) | Orchestrates storage benchmark campaigns (`zinder-bench`) and writes campaign artifacts |
| [run-postgres-testing-ground.sh](run-postgres-testing-ground.sh) | Boots and verifies the containerized PG 17/18 + PgBouncer + Mock R2 + Caddy stateless-topology testing ground |
| [run-rocksdb-storage-lifecycle.sh](run-rocksdb-storage-lifecycle.sh) | Executes RocksDB compaction, recovery, and space-lifecycle qualification probes |
| [test-benchmark-campaign.py](test-benchmark-campaign.py) | Unit-tests `run-benchmark-campaign.py` |
| [test-cbb-interop-report.sh](test-cbb-interop-report.sh) | Unit-tests CBB interop benchmark report checks |
| [test-compare-benchmark-campaign.py](test-compare-benchmark-campaign.py) | Unit-tests `compare-benchmark-campaign.py` |
| [test-light-wallet-qualification.sh](test-light-wallet-qualification.sh) | Tests light-wallet qualification report checks |
| [test-mainnet-fixture.py](test-mainnet-fixture.py) | Unit-tests `scripts/mainnet-fixture/` manifest schema and helpers |
| [test-regtest-fixture.py](test-regtest-fixture.py) | Unit-tests `regtest-fixture.py` |
| [test-storage-benchmark-campaign-validator.sh](test-storage-benchmark-campaign-validator.sh) | Unit-tests `validate-storage-benchmark-campaign.sh` |
| [test-wallet-reader-comparison.py](test-wallet-reader-comparison.py) | Unit-tests `compare-wallet-readers.py` |
| [validate-cbb-interop-report.sh](validate-cbb-interop-report.sh) | Validates CBB interop benchmark report schema and invariants |
| [validate-rocksdb-storage-lifecycle-report.sh](validate-rocksdb-storage-lifecycle-report.sh) | Validates RocksDB storage lifecycle qualification reports |
| [validate-storage-benchmark-campaign.sh](validate-storage-benchmark-campaign.sh) | Validates benchmark campaign reports before admission decisions |

### Wallet reader benchmark presets & oracles (`scripts/benchmarks/wallet-reader/`)

- [archives.sha256](benchmarks/wallet-reader/archives.sha256): pinned SHA-256 digests for frozen wallet reader qualification archives
- [boundary-oracle.json](benchmarks/wallet-reader/boundary-oracle.json): boundary-range oracle fixture queries
- [covered-oracle.json](benchmarks/wallet-reader/covered-oracle.json): covered-range oracle fixture queries
- [frontier-oracle.json](benchmarks/wallet-reader/frontier-oracle.json): tip-frontier oracle fixture queries
- [historical_control-oracle.json](benchmarks/wallet-reader/historical_control-oracle.json): historical-control oracle fixture queries
- [managed-testnet.json](benchmarks/wallet-reader/managed-testnet.json): benchmark target manifest for managed Testnet endpoints
- [policy.json](benchmarks/wallet-reader/policy.json): latency and regression admission thresholds for `compare-wallet-readers.py`
- [standalone-testnet.json](benchmarks/wallet-reader/standalone-testnet.json): benchmark target manifest for standalone Testnet runs

### Mainnet benchmark fixture (`scripts/mainnet-fixture/`)

See [Restoring the Mainnet benchmark fixture](../docs/runbooks/mainnet-benchmark-fixture.md) for host lifecycle and GCP snapshot procedures.

- **Control & schema:**
  - [build-stage.sh](mainnet-fixture/build-stage.sh): packages Zinder role binaries, the pinned Zebra binary, tools (`jq`, `grpcurl`), guest scripts, and protocol files into a stage tarball (and optionally uploads it to GCS)
  - [fixture.schema.json](mainnet-fixture/fixture.schema.json): JSON Schema for signed Mainnet fixture receipts and manifests
  - [restore-fixture.sh](mainnet-fixture/restore-fixture.sh): launches and restores a benchmark guest VM from the frozen Mainnet disk image
- **Guest scripts & unit configs (`scripts/mainnet-fixture/guest/`):**
  - [archive-sha256sums.py](mainnet-fixture/guest/archive-sha256sums.py): hashes sealed archive trees for receipt verification
  - [fixture-gate.py](mainnet-fixture/guest/fixture-gate.py): checks guest fixture invariants against `fixture.schema.json`
  - [hash-tree.sh](mainnet-fixture/guest/hash-tree.sh): computes directory digest manifests inside the guest
  - [measure.sh](mainnet-fixture/guest/measure.sh): collects CPU, memory, and disk measurements during benchmark runs
  - [objstore.sh](mainnet-fixture/guest/objstore.sh): object-storage helper for staging and reading Mainnet fixture artifacts
  - [offhost-download.sh](mainnet-fixture/guest/offhost-download.sh): pulls benchmark inputs or snapshots from off-host object storage
  - [offhost-upload.sh](mainnet-fixture/guest/offhost-upload.sh): uploads benchmark receipts and artifacts to off-host storage
  - [outcome.sh](mainnet-fixture/guest/outcome.sh): gathers run metrics and emits structured guest outcome JSON
  - [restore-guest.sh](mainnet-fixture/guest/restore-guest.sh): mounts disks, sets up Zebra/Zinder services, and restores guest state
  - [route-check.py](mainnet-fixture/guest/route-check.py): verifies network routing and endpoint reachability in the guest harness
  - [route_proof.py](mainnet-fixture/guest/route_proof.py): proves request routing isolation across guest benchmark endpoints
  - [rusage.py](mainnet-fixture/guest/rusage.py): captures process resource-usage summaries
  - [space-sampler.sh](mainnet-fixture/guest/space-sampler.sh): samples disk space usage during store construction and compaction
  - [stack.sh](mainnet-fixture/guest/stack.sh): starts, stops, and inspects Zebra and Zinder processes on the guest VM
  - [wallet-sample.sh](mainnet-fixture/guest/wallet-sample.sh): runs representative wallet sync probes against the restored fixture
  - [writer-status.sh](mainnet-fixture/guest/writer-status.sh): polls writer construction progress on the guest VM
  - [zebrad.toml](mainnet-fixture/guest/zebrad.toml): guest Zebra configuration template
  - [zfx-zebrad.service](mainnet-fixture/guest/zfx-zebrad.service): systemd unit template for the fixture Zebra daemon
- **Checked-in fixture testdata (`scripts/mainnet-fixture/testdata/`):**
  - [fixture-905dac4e-signed-history-6cd01968.json](mainnet-fixture/testdata/fixture-905dac4e-signed-history-6cd01968.json): pinned signed-history variant fixture manifest
  - [fixture-905dac4e-signed-history.json](mainnet-fixture/testdata/fixture-905dac4e-signed-history.json): pinned Mainnet signed-history fixture manifest
  - [guest-outcome-fx490-m2.json](mainnet-fixture/testdata/guest-outcome-fx490-m2.json): reference guest benchmark outcome receipt
  - [guest-receipt-476-zfx476-d.json](mainnet-fixture/testdata/guest-receipt-476-zfx476-d.json): reference fixture build receipt

## Wallet Client Certification & Matrix (`scripts/wallet-clients/`)

### Top-level certification runners & GitHub Action

| File | Purpose |
| --- | --- |
| [release-wallet-images.json](release-wallet-images.json) | Pinned client/test container images for release wallet qualification |
| [release-wallet-roundtrip.md](release-wallet-roundtrip.md) | Procedures and pass rules for multi-wallet release roundtrip qualification |
| [release-wallet-roundtrip.py](release-wallet-roundtrip.py) | Executes release roundtrip qualification suites across certified wallets |
| [release-wallet-summary.py](release-wallet-summary.py) | Summarizes wallet certification matrix results for release checks |
| [release-wallet-tests.py](release-wallet-tests.py) | Runs automated release checks configured by `release-wallet-images.json` |
| [run-wallet-roundtrip-regtest.py](run-wallet-roundtrip-regtest.py) | Runs local multi-wallet send/receive roundtrip certification against Regtest |
| [run-wallet-roundtrip.py](run-wallet-roundtrip.py) | Runs multi-wallet send/receive roundtrip verification against Testnet/Regtest |
| [run-zkool-restore.py](run-zkool-restore.py) | Exercises Zkool restore and rescan behavior against Zinder |
| [release-wallet-action/action.yml](release-wallet-action/action.yml) | Composite GitHub Action encapsulating wallet certification runs |

### Wallet certification docs & baseline fixtures (`scripts/wallet-clients/`)

- [README.md](wallet-clients/README.md): overview, preconditions, and usage for wallet client qualification harnesses
- [mobile-qualification.md](wallet-clients/mobile-qualification.md): qualification steps for mobile wallet builds (YWallet, Zashi/Zally, Cake, etc.)
- [cake-regtest.md](wallet-clients/cake-regtest.md): Cake Wallet Regtest payment harness usage
- [payment-recovery.md](wallet-clients/payment-recovery.md): payment recovery matrix contract and scenarios
- [payment-roles.md](wallet-clients/payment-roles.md): sender/receiver role assignment matrix specification
- [roundtrip.md](wallet-clients/roundtrip.md): multi-wallet send/receive roundtrip harness documentation
- [subtree.md](wallet-clients/subtree.md): subtree-index parity verification documentation
- [transport-payment.md](wallet-clients/transport-payment.md): gRPC / gRPC-Web / TLS transport payment test documentation
- [vizor.md](wallet-clients/vizor.md): Vizor wallet certification guide
- [zkool-payment.md](wallet-clients/zkool-payment.md): Zkool payment matrix harness guide
- [coverage-v0.22.0.json](wallet-clients/coverage-v0.22.0.json): historical baseline coverage matrix for wallet client certification
- [subtree-testnet.json](wallet-clients/subtree-testnet.json): Testnet subtree root reference data

### Matrix orchestration, coverage & protocol harnesses (`scripts/wallet-clients/`)

- **Matrix & shared fixtures:**
  - [audit-wallet-roundtrip.py](wallet-clients/audit-wallet-roundtrip.py): audits roundtrip evidence bundles for completeness
  - [counterparty_builds.py](wallet-clients/counterparty_builds.py): resolves counterparty wallet build specs for matrix runs
  - [coverage.py](wallet-clients/coverage.py): computes and validates wallet certification coverage matrices
  - [coverage_counterparty_roles.py](wallet-clients/coverage_counterparty_roles.py): checks counterparty role coverage across certified clients
  - [coverage_evidence.py](wallet-clients/coverage_evidence.py): parses and verifies structured evidence bundles from test runs
  - [coverage_payment_recovery.py](wallet-clients/coverage_payment_recovery.py): checks payment-recovery coverage rules
  - [coverage_roundtrip.py](wallet-clients/coverage_roundtrip.py): aggregates roundtrip certification outcomes into coverage reports
  - [coverage_test.py](wallet-clients/coverage_test.py): unit tests for core coverage matrix logic
  - [coverage_zkool_payments.py](wallet-clients/coverage_zkool_payments.py): checks Zkool payment matrix coverage rules
  - [make-manifest.py](wallet-clients/make-manifest.py): builds JSON run manifests for multi-wallet certification runs
  - [roundtrip_fixture.py](wallet-clients/roundtrip_fixture.py): Regtest/Testnet chain fixture helpers for roundtrip certification
  - [roundtrip_wallets.py](wallet-clients/roundtrip_wallets.py): wallet driver registry and adapters for roundtrip runs
  - [wait-serving-fence.py](wallet-clients/wait-serving-fence.py): polls `/readyz` / `ServerInfo` until a target chain height fence is served
  - [wallet_fixture.py](wallet-clients/wallet_fixture.py): shared wallet funding, setup, and state helpers
- **Protocol & behavior checks:**
  - [lightwalletd-identity.py](wallet-clients/lightwalletd-identity.py): checks `GetLightdInfo` / server identity parity between Zinder and lightwalletd
  - [native-anchor.py](wallet-clients/native-anchor.py): checks anchor/tree-state selection behavior across native wallet scenarios
  - [native-expiry.py](wallet-clients/native-expiry.py): tests expiry-height transaction handling and rejection diagnostics
  - [payment-recovery.py](wallet-clients/payment-recovery.py): runs interrupted-sync and reorg payment recovery scenarios
  - [payment-recovery-test.py](wallet-clients/payment-recovery-test.py): tests payment-recovery scenario selection and validation
  - [payment-roles.py](wallet-clients/payment-roles.py): runs parameterized sender/receiver payment role suites
  - [subtree-ab.py](wallet-clients/subtree-ab.py): compares `GetSubtreeRoots` outputs across A/B endpoints
  - [subtree-test.py](wallet-clients/subtree-test.py): unit/integration checks for subtree verification helpers
  - [subtree_proxy.py](wallet-clients/subtree_proxy.py): local diagnostic proxy used by subtree fault-injection tests
  - [transport-payment.py](wallet-clients/transport-payment.py): executes payment verification across gRPC, gRPC-Web, and TLS transports
  - [transport-payment-test.py](wallet-clients/transport-payment-test.py): tests `transport-payment.py` CLI and checks

### Client-specific drivers & unit tests (`scripts/wallet-clients/`)

- **Cake, devtool, Vizor, Zally, Zingo & Zkool harnesses:**
  - [build-vizor-regtest.py](wallet-clients/build-vizor-regtest.py): builds Vizor Regtest client harness binaries
  - [vizor-manifest.py](wallet-clients/vizor-manifest.py): generates Vizor certification test manifests
  - [vizor-payment.py](wallet-clients/vizor-payment.py) / [vizor-payment.rs](wallet-clients/vizor-payment.rs): Python wrapper and Rust driver for Vizor payment tests
  - [vizor-regtest.py](wallet-clients/vizor-regtest.py) / [vizor-regtest.rs](wallet-clients/vizor-regtest.rs): Python wrapper and Rust driver for Vizor Regtest runs
  - [cake-manifest.py](wallet-clients/cake-manifest.py): generates Cake Wallet certification manifests
  - [cake-payment.py](wallet-clients/cake-payment.py): drives Cake payment certification flows
  - [cake-regtest.py](wallet-clients/cake-regtest.py): drives Cake Wallet Regtest scenarios
  - [devtool-payment.py](wallet-clients/devtool-payment.py): executes librustzcash devtool payment flows against Zinder
  - [devtool-regtest.py](wallet-clients/devtool-regtest.py): drives devtool Regtest initialization and sync checks
  - [run-devtool-regtest.py](wallet-clients/run-devtool-regtest.py): end-to-end runner for devtool Regtest scenarios
  - [zally-payment.py](wallet-clients/zally-payment.py) / [zally-payment.rs](wallet-clients/zally-payment.rs): Python wrapper and Rust driver for Zally payment checks
  - [zingo-payment.py](wallet-clients/zingo-payment.py): drives Zingo CLI payment verification flows
  - [zingo-regtest.py](wallet-clients/zingo-regtest.py): drives Zingo Regtest scenarios
  - [interrupted-zkool.py](wallet-clients/interrupted-zkool.py): tests Zkool sync interruption and recovery
  - [reorg-zkool.py](wallet-clients/reorg-zkool.py): tests Zkool handling under chain reorganizations
  - [zkool-payment-matrix.py](wallet-clients/zkool-payment-matrix.py): runs Zkool multi-pool payment matrix checks
  - [zkool-payment.py](wallet-clients/zkool-payment.py) / [zkool-payment.rs](wallet-clients/zkool-payment.rs): Python wrapper and Rust driver for Zkool payments
  - [zkool-regtest.py](wallet-clients/zkool-regtest.py) / [zkool-regtest.rs](wallet-clients/zkool-regtest.rs): Python wrapper and Rust driver for Zkool Regtest suites
- **Python unit tests for certification harnesses:**
  - [test-audit-wallet-roundtrip.py](wallet-clients/test-audit-wallet-roundtrip.py): tests `audit-wallet-roundtrip.py`
  - [test-coverage-counterparty-roles.py](wallet-clients/test-coverage-counterparty-roles.py): tests counterparty role coverage checks
  - [test-coverage-evidence.py](wallet-clients/test-coverage-evidence.py): tests evidence parsing and verification
  - [test-coverage-payment-recovery.py](wallet-clients/test-coverage-payment-recovery.py): tests payment-recovery coverage rules
  - [test-coverage-zkool-payments.py](wallet-clients/test-coverage-zkool-payments.py): tests Zkool payment coverage rules
  - [test-devtool-endpoint-identity.py](wallet-clients/test-devtool-endpoint-identity.py): tests devtool endpoint identity checks
  - [test-payment-roles.py](wallet-clients/test-payment-roles.py): tests `payment-roles.py`
  - [test-roundtrip-regtest-command.py](wallet-clients/test-roundtrip-regtest-command.py): tests Regtest roundtrip command assembly
  - [test-roundtrip-wallets.py](wallet-clients/test-roundtrip-wallets.py): tests `roundtrip_wallets.py`
  - [test-wallet-fixture.py](wallet-clients/test-wallet-fixture.py): tests `wallet_fixture.py`
  - [test-wallet-roundtrip.py](wallet-clients/test-wallet-roundtrip.py): tests top-level roundtrip helpers
  - [test-zally-payment.py](wallet-clients/test-zally-payment.py): tests `zally-payment.py`
  - [test-zkool-payment-matrix.py](wallet-clients/test-zkool-payment-matrix.py): tests `zkool-payment-matrix.py`
