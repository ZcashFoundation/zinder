# Release note fragments

Every pull request must either add a fragment under `.changes/unreleased/` or
check the exact `No release note required` declaration in the pull request
template. Add a fragment for any change that affects operators, API consumers,
deployment behavior, compatibility, security, or supported workflows.

Zinder uses Changie v1.26.0 to keep each pull request's note independent until
release preparation. Install that version, then create a fragment from the
repository root:

```console
go install github.com/miniscruff/changie@v1.26.0
changie new
```

Choose the category (`kind`), the SemVer impact (`Bump`), and the upgrade class
(`Upgrade`) independently. Write the body for a Zinder user or operator: state
the behavior that changed and omit implementation details that do not affect
them. The pull request number links the final changelog entry back to its review
context.

## Upgrade classes

Every pending fragment must set `custom.Upgrade` to one of four values, ordered
from least to most disruptive:

| Class | Meaning and operator impact | Rollback contract |
| --- | --- | --- |
| `compatible` | Old and new releases read and write the same stores and schemas. | Redeploy the previous release against the same store and schema. |
| `additive-schema` | Only additive schema changes occur; old and new releases still read and write the same store and each other's rows. | Redeploy the previous release against the same store and schema. |
| `projection-layout` | A projection store layout changes; that projection must be rebuilt from scratch in a clean path. | Name the affected projection (`wallet`, `explorer`, or `materialized-view`) and its rebuild or rollback steps in `body`. |
| `canonical-layout` | The canonical store layout changes; the canonical store and dependent projections must be rebuilt from scratch in clean paths. | State the layout change and rebuild or rollback steps in `body`. |

Authoring and policy rules:

- A change the previous release cannot read is never `compatible` or
  `additive-schema`. Use `projection-layout` or `canonical-layout`.
- Any fragment with `Upgrade: projection-layout` must name at least one affected
  projection (`wallet`, `explorer`, or `materialized-view`) and state rebuild or
  rollback guidance in `body`. `scripts/validate-changelog.sh` enforces this in
  `fragments` and `pr` modes.
- On single-volume `rocksdb` deployments, keeping the previous store path side by
  side with the new store path is optional (requiring roughly double disk space
  until the old path is removed); fallback rollback otherwise restores a
  matching backup or snapshot or rebuilds from empty in a clean path. Any
  7-day superseded-schema retention guidance applies strictly to future hosted
  `postgres` (planned M3, not yet implemented).
- **Waiver rule:** Checking `- [x] No release note required` on a pull request
  counts as `compatible`. Never use the waiver for a change to any persisted
  layout (canonical store, wallet projection, or Explorer materialized views) or
  one-way store revision.
- **Release derivation:** Release preparation derives one release-level upgrade
  class as the highest-precedence class among pending fragments
  (`compatible < additive-schema < projection-layout < canonical-layout`).

Run the local policy checks before opening the pull request:

```console
scripts/validate-changelog.sh fragments
scripts/test-changelog.sh
```

Release preparation consumes all pending fragments into one dated version
section. Do not edit or delete another pull request's fragment to resolve a
merge conflict; fragment filenames are isolated by category and pull request
number. Follow the [release runbook](../docs/runbooks/releasing.md) to prepare a
version.
