# Releasing Zinder

This runbook is for maintainers who cut a Zinder release. It covers preparing
the version and changelog, tagging, approving publication, verifying the
result, and recovering when a release fails. Why the artifacts are built and
signed this way is recorded in
[ADR-0022](../adrs/0022-release-artifacts.md).

## What a release publishes

Every first-party crate and service shares one product version, the root
`[workspace.package].version`. A tag `vX.Y.Z` publishes:

| Artifact | Contents |
| --- | --- |
| Runtime image | The single distroless OCI image in `deploy/release-images.json` (`ghcr.io/zcashfoundation/zinder`), for amd64 and arm64 |
| Binary archives | One archive per platform (x86-64-v3 with BMI2+ADX Pasta assembly, and AArch64 GNU/Linux) with the unified `bin/zinder` executable, plus an SPDX 2.3 SBOM for each |
| Rust SDK | `zinder-core`, `zinder-proto`, and `zinder-client` on crates.io |
| API artifacts | `zinder.v1.descriptor.bin` and a `zinder-api-docs` archive with the native `.proto` sources and OpenAPI documents |
| GitHub Release | All of the above files, `SHA256SUMS`, and its signature `SHA256SUMS.sigstore.json` |

The tag must be `v` followed by the workspace version. A prerelease adds a
SemVer suffix, such as `v0.28.0-rc.1`. Build metadata (`+...`) is rejected,
because an OCI image tag cannot carry it.

## Before you start

- You can push tags and approve the protected `release` environment.
- Immutable releases are enabled on the repository.
- The crates.io trusted publisher for all three SDK crates names repository
  `ZcashFoundation/zinder`, workflow `release.yml`, and environment `release`.
  Do not add a `CARGO_REGISTRY_TOKEN` secret; the workflow gets a short-lived
  token through OIDC.
- Changie v1.26.0, GitHub CLI 2.94 or newer, and Cosign are installed.
- Every change for the release has merged with its `.changes/unreleased/`
  fragment, as [`.changes/README.md`](../../.changes/README.md) describes.

## Prepare the release

1. Ask Changie for the version that the pending fragments imply:

   ```console
   next_version="$(changie next auto)"
   printf '%s\n' "$next_version"
   ```

2. Set `[workspace.package].version` in `Cargo.toml` to that version without
   the `v`. Set the same version in every `version = "..."` requirement on
   `zinder-core`, `zinder-proto`, and `zinder-client`: the root `Cargo.toml`
   and the `zinder-proto` and `zinder-client` manifests.

3. Batch the fragments into `CHANGELOG.md` and check the result:

   ```console
   scripts/prepare-changelog-release.sh "${next_version#v}"
   cargo check --workspace --all-targets --all-features
   scripts/check-sdk-package-policy.sh
   scripts/verify-sdk-packages.sh
   git diff -- Cargo.toml Cargo.lock CHANGELOG.md .changes
   ```

   The script fails unless the requested version matches the fragments'
   SemVer impact and every first-party package. For a prerelease, it keeps
   the fragments, so the next candidate contains the whole release, and it
   replaces an earlier candidate's section. For a stable version, it removes
   the prerelease sections and consumes the fragments. Rerunning it for the
   same version changes nothing.

4. Open a pull request with this change and check `No release note required`
   in its template. Merge it to `main` once the required checks pass.

## Confirm both node runtimes

Zinder certifies Zebra 7.1.0 and Zakura 1.8.6. A failure against either node
blocks the release. On the release-preparation commit, these four jobs must
be green:

- `live-tests`: `live regtest (zebra)` and `live regtest (zakura)`
- `parity-compat`: `parity diff (regtest, zebra)` and
  `parity diff (regtest, zakura)`

Both workflows run nightly and on node-facing pull requests, so they may not
have run on that commit. A manual run starts on a branch or tag, so start
both while `main` still points at the release-preparation commit:

```console
git switch main
git pull --ff-only
release_commit="$(git rev-parse HEAD)"
git log -1 --oneline "$release_commit"
gh workflow run live-tests.yml --ref main -f network=zcash-regtest
gh workflow run parity-compat.yml --ref main
```

Check that the log line shows the release-preparation commit. Then find the
manual runs by commit, not by recency, and wait for each. `gh run watch
--exit-status` exits non-zero when any job fails:

```console
gh run list --workflow live-tests.yml --commit "$release_commit" --event workflow_dispatch --json databaseId,status,conclusion
gh run list --workflow parity-compat.yml --commit "$release_commit" --event workflow_dispatch --json databaseId,status,conclusion
gh run watch <databaseId> --exit-status
```

If `main` moved before the runs started, both lists stay empty. In that case,
push the tag first, run both workflows with `--ref` set to the tag, and
approve the release only after all four jobs pass on the tag's commit.

## Tag and approve

1. Create and push an annotated tag at the release commit:

   ```console
   git tag -a v0.28.0 -m "v0.28.0" "$release_commit"
   git push origin v0.28.0
   ```

2. The `release` workflow starts. Before it asks for approval, it runs these
   jobs without registry credentials:

   - `validate release identity` checks that the tag matches Cargo metadata
     and is reachable from `main`, that `CHANGELOG.md` has a non-empty section
     for it, and that no published GitHub Release has the tag. A stable tag
     must have no pending fragments. A prerelease tag must keep them.
   - `build API release artifacts` and `verify public SDK packages`, which
     also dry-runs the crate publication plan. Missing crate versions are
     expected in that dry run.
   - Per platform, `compile release binaries` and
     `reproduce release binaries` build the archive twice in independent,
     uncached jobs. The reproduction also packages every image from its
     archive without pushing, with `scripts/package-release-images.sh
     check`. Each image binary must match its archive binary byte for byte
     and be a root-owned file with mode `0755`. It must run `--version` and
     `--help` as UID 10001, and its `healthcheck` subcommand must exit 1
     against a port with no listener. `compare independent release builds`
     then requires identical archive and binary hashes.

3. GitHub requests approval for `publish public SDK crates`, the only job in
   the `release` environment. Every later job depends on it. Before you
   approve, check that:

   - the run belongs to your tag, and its commit is the release commit;
   - both `compare independent release builds` jobs passed;
   - the `verify public SDK packages` summary shows the plan you expect for
     the three crates; and
   - the four node jobs passed on that commit.

   Approve within 7 days of the tag push. The release notes, API artifacts,
   and binary archives that later jobs download expire after 7 days. If they
   expire, rerun all jobs, which requests a new approval.

4. After approval, the workflow publishes the missing SDK crates and checks
   them from the registry, packages and pushes the unified `zinder` image from
   the compared archive binary, attests and signs it, signs `SHA256SUMS`, and
   publishes the GitHub Release. For a stable tag only, it then moves the `latest`
   image tag. Prereleases never move `latest`.

The workflow publishes one tag at a time and has no manual trigger. For a
build-only smoke test that publishes nothing, run the `build-images` workflow
by hand.

## Verify the publication

Check out the tag, so the SDK smoke test compiles against that version's API.
Then verify the Release, the archives, the crates, and each image:

```console
git switch --detach v0.28.0
gh release view v0.28.0
gh release verify v0.28.0

gh release download v0.28.0 \
  --pattern 'zinder-0.28.0-*-unknown-linux-gnu.tar.gz' \
  --pattern 'zinder-0.28.0-*-unknown-linux-gnu.spdx.json' \
  --pattern SHA256SUMS \
  --pattern SHA256SUMS.sigstore.json
sha256sum --ignore-missing --check SHA256SUMS
cosign verify-blob \
  --bundle SHA256SUMS.sigstore.json \
  --certificate-identity \
    'https://github.com/ZcashFoundation/zinder/.github/workflows/release.yml@refs/tags/v0.28.0' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  SHA256SUMS

scripts/verify-published-sdk.sh 0.28.0

for image in $(jq -r '.[]' deploy/release-images.json); do
  docker buildx imagetools inspect "ghcr.io/zcashfoundation/${image}:v0.28.0"
done
```

Each image must list amd64 and arm64 manifests. Each binary archive contains
`bin/zinder`, `BUILD-INFO.json`, `LICENSE`, `README.md`, and its own `SHA256SUMS`.

When a release publishes an image name for the first time, GHCR creates the
package as private. An organization admin must make it public once. An
anonymous `docker pull` of the new image confirms it.

Deployments should pin a verified image digest or archive checksum, not
`latest`.

### Inspect an image

The distroless `zinder` image has no shell, so `docker run ... sh` fails. Inspect
the image from outside, and invoke `zinder` commands directly:

```console
image=ghcr.io/zcashfoundation/zinder:v0.28.0
docker pull "$image"
docker image inspect "$image" --format '{{.Config.User}}'
docker image inspect "$image" --format '{{json .Config.Labels}}' | jq
docker run --rm "$image" --version
```

The image runs as `10001:10001`, and `--version` prints `zinder 0.28.0`.

Every image carries these OCI labels:

| Label | Value |
| --- | --- |
| `org.opencontainers.image.title` | The image name (`zinder`) |
| `org.opencontainers.image.description` | One line on what the image runs |
| `org.opencontainers.image.version` | The release tag |
| `org.opencontainers.image.revision` | The tag-target commit |
| `org.opencontainers.image.created` | The tag-target commit time in UTC, so a rebuilt image reports the same time |
| `org.opencontainers.image.source`, `org.opencontainers.image.url` | The GitHub repository |
| `org.opencontainers.image.documentation` | The deployment runbook for the image at the release tag |
| `org.opencontainers.image.vendor` | `Zcash Foundation` |
| `org.opencontainers.image.licenses` | `MIT` |

`scripts/package-release-images.sh` sets the description and runbook per
image. Packaging fails for a catalog image that has no entry there, so add one
when you add an image.

## Recover a failed release

Nothing is published before approval. A tag whose binaries do not build,
reproduce, or package therefore publishes nothing. After approval, every job
can be rerun: crate publication skips versions already published from the
same commit, and the workflow replaces draft assets and recreates exact image
tags from the same commit.

| Failure | Action |
| --- | --- |
| A transient failure, such as a lost runner | Rerun the failed jobs. A rerun after approval does not ask again. |
| A build, reproduction, or packaging failure that is not transient | Merge the fix and release it as the next version. To reproduce a packaging failure locally, build an archive with `scripts/build-release-binary-archive.sh` and run `scripts/package-release-images.sh check` against it. |
| `publish public SDK crates` times out on registry propagation | Rerun the job. It reobserves crates.io and skips verified versions. |
| A crate version exists but was published from another commit | Stop and investigate. Never yank or replace a version to let the release continue. |
| Exact and commit image tags do not converge in GHCR | Inspect both tags in GHCR before you retry. Do not bypass the topology check. |
| Image SBOM evidence reports an unexpected runtime package | Regenerate the SBOM from the exact child digest and compare it with `deploy/release-images.json`. The distroless image must report only `bin/zinder` copied from the archive. Do not widen the allowed package set. |
| A prerelease published crates or images but no GitHub Release | Leave the tag and artifacts as they are. Merge the fix and publish the next release candidate. |
| Only `promote stable image tags` failed | Rerun that job. It checks the verified manifests before moving `latest`. |

Never move a release tag or rebuild it from another commit. A published
GitHub Release is immutable, and the workflow refuses to change it.

To roll a deployment back, follow
[Deploying on a VM](deploying-on-a-vm.md). Start an older binary only against
a store whose layout it supports.
