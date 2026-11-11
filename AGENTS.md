# Repository Guidelines

[CLAUDE.md](CLAUDE.md) contains the canonical repository instructions. Read and
follow it before making changes; this file is a concise project reference.

## GitHub Delivery and Project Management

Open delivery work is tracked in the public [Zinder delivery Project 7](https://github.com/users/gustavovalverde/projects/7). Project metadata is part of the delivery contract, not a secondary documentation layer.

- Use GitHub's native parent and sub-issue hierarchy for decomposition. Use native `blocked by` relationships for hard prerequisites; do not maintain `Parent`, `Blocks`, `Blocked by`, or hand-written issue maps in issue bodies. Project `Blocked by` mirrors the native direct dependencies for board visibility.
- Every executable Zinder issue has one primary issue-kind label, exactly one `type:*` label, every relevant `area:*` label, one milestone, and Project 7 Status, Priority, and Track. Cross-repository PRs may omit a milestone when no Zinder exit gate applies, but they still require Project 7 Status, Priority, and Track.
- `type:afk` means every acceptance criterion can be completed from repository, fixture, or isolated-test evidence. `type:hitl` means at least one criterion requires a named human decision, credential, external system, public-network effect, operator-owned environment, or official interaction. `type:epic` is reserved for grouping issues with native executable sub-issues; it is not an implementation type.
- Use one workflow-state label where applicable: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, or `wontfix`. Apply `ready-for-agent` only to a sufficiently scoped autonomous issue; do not use it for epics, human-controlled qualification, or unresolved scope decisions.
- Apply area labels from the Zinder taxonomy: `area:ingest`, `area:storage`, `area:wallet`, `area:compat`, `area:operations`, `area:release`, `area:postgres`, and `area:explorer`. Create a new area label only when an outcome cannot be represented by these existing ownership boundaries.
- Issue bodies state the actor outcome, domain problem, delivery scope, acceptance criteria, execution boundary, durable references, and out-of-scope claims. They must not duplicate native hierarchy, native dependencies, or Project fields.

Project 7 uses these delivery fields:

- Priority: `P0 Now`, `P1 Next`, `P2 Later`, `P3 Backlog`, `Epic`.
- Track: `Wallet adoption`, `Single-host production`, `Query scalability`, `PostgreSQL topology`, `Explorer`, `Release engineering`.
- Milestones: M0 Consumer adoption, M1 v0.6.x reliability, M2 Operator-ready RocksDB, M3 Horizontal topology, and M4 Explorer. A milestone is an exit gate, not merely a topic label.

Wallet compatibility evidence and production-topology qualification are separate claims. The supported candidate remains `rocksdb-single-host` until release-specific capacity, recovery, replacement, and operator evidence admit another topology. PostgreSQL tracer or benchmark work remains diagnostic and non-production until the measured single-host evidence and the explicit horizontal-admission decision close.

Project 7's native workflows are enabled for item addition, sub-issue addition, issue closure, pull-request linkage, and pull-request merge. Preserve those workflows. After any issue, pull request, milestone, label, dependency, or Project mutation, verify the native hierarchy, direct dependencies, labels, milestone, Project Status/Priority/Track, and mirrored `Blocked by` value.

## Project Structure & Module Organization

This is a Rust 2024 workspace.

Domain crates live under `crates/`: `zinder-core` for shared types, `zinder-store` for canonical storage contracts and the RocksDB adapter, `zinder-rocksdb-bulk-load` for the bulk-load and SST mechanics the storage engines share, `zinder-source` for upstream node adapters and the shared `NodeTarget` config type, `zinder-proto` for generated protocol modules, `zinder-materialized-views` for the materialized-view plane SDK and its bundled consumers, `zinder-wallet-projection` for wallet projection domain and encoding contracts, `zinder-wallet-rocksdb` for wallet projection storage and construction, `zinder-client` for the typed consumer client surface, `zinder-runtime` for the operational HTTP surface and config loader, and `zinder-testkit` for fixtures and the `live::` test-helper module.

Service crates live under `services/`. Three are release runtimes: `zinder-ingest` constructs and follows the canonical store as its only writer, `zinder-projector` builds wallet projections, and `zinder-compat-lightwalletd` translates the lightwalletd protocol. `zinder-query` owns the wallet and application query boundary and ships as both a library and a binary. `zinder-explorer` serves the explorer plane, `zinder-compat-cipherscan` serves a Cipherscan-compatible REST surface, and `zinder-bench` is the storage benchmark harness. `tools/zinder-proto-codegen` regenerates the checked-in protocol modules.

Integration tests sit beside each crate in `tests/{integration,live,perf}/` as documented in the [Testing Runbook](docs/runbooks/testing.md). Architecture, ADRs, references, and runbooks live under `docs/`; update them when a change alters boundaries, protocol bytes, storage semantics, or public vocabulary.

## Build, Test, and Development Commands

- `cargo fmt --all --check`: verify formatting.
- `cargo check --workspace --all-targets --all-features`: type-check all crates and test targets.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: run the strict lint gate.
- `cargo nextest run --profile=ci`: run T0/T1 (unit + integration) tests.
- `cargo nextest run --profile=ci-parity`: run T4 consumer-contract certification tests.
- `cargo nextest run --profile=ci-perf`: run T2 (perf) tests.
- PostgreSQL driver gate: start a fresh disposable database, set `ZINDER_TEST_POSTGRES_DATABASE_URL`, then run `cargo nextest run -p zinder-bench --profile=ci-postgres --run-ignored=all`.
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps`: validate docs.
- `cargo deny check` and `cargo machete`: check dependency policy and unused dependencies.
- `cargo llvm-cov --workspace --all-features --no-report`: run coverage locally before risky storage/parser changes.
- T3 (live) tests: see [CLAUDE.md §Live Node Tests](CLAUDE.md). Set `ZINDER_TEST_LIVE=1` plus the unified `ZINDER_NETWORK` and `ZINDER_NODE__*` env vars, then `cargo nextest run --profile=ci-live --run-ignored=all`.

## Coding Style & Naming Conventions

Use workspace-managed Rust 2024 settings and `rustfmt.toml`. The workspace MSRV is Rust 1.95; keep `Cargo.toml`, `rust-toolchain.toml`, `clippy.toml`, CI toolchain actions, and Docker `RUST_VERSION` args aligned whenever it changes. The lint baseline denies warnings, unsafe code, `unwrap`, `expect`, `panic`, `todo`, debug prints, unreachable public API, and `std::sync::{Mutex,RwLock}`. Prefer `parking_lot` for synchronous shared state and `tokio::sync` only when the guard must cross an async boundary. Prefer domain names from `docs/architecture/public-interfaces.md`: `ChainEpoch`, `ChainEvent`, `NodeSource`, `WalletQueryApi`, and related terms. Avoid generic modules such as `utils`, `helpers`, or `manager`. (`tests/common/` is the one exception: per-crate shared test helpers, included via `mod common;` in `tests/acceptance.rs`.)

Test functions under `tests/live/` use plain `snake_case_describing_behavior` names. Do not include `live`, `regtest`, `testnet`, `mainnet`, or `z3` in the function name; the directory and runtime parameterization handle that.

## Testing Guidelines

Tests should exercise public boundaries and contract shapes: append, reorg, settlement, cursor validation, storage recovery, and parser edge cases. Tier organization is by directory: T0 unit (`#[cfg(test)] mod tests` in `src/`), T1 integration (`tests/integration/`), T2 perf (`tests/perf/`), T3 live (`tests/live/`). T3 tests are double-gated by `#[ignore = LIVE_TEST_IGNORE_REASON]` and `zinder_testkit::live::require_live()`; mainnet is rejected by default. Mutation testing is targeted at critical storage and parser functions through the CI workflow; expand that target set when changing those contracts.

## Commit & Pull Request Guidelines

Use full Conventional Commits syntax for commits and pull request titles, for
example `fix(store): reject invalid reorg replacement`. Every pull request must
follow the [release-note instructions](CLAUDE.md#pull-request-release-notes) and
provide either a present `.changes/unreleased/*.yaml` fragment or the exact
no-note waiver. Pull requests should summarize behavior changes, list
validation commands run, link related docs or ADR updates, and call out any
deferred production gap.

## Security & Configuration Tips

Never print secrets or raw authorization material. `--print-config` output must show explicit redaction markers. Production-like storage should be opened with an explicit network, and schema, network, and reorg-window mismatches should fail closed.
