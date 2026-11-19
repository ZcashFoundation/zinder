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

Choose the category that describes the change, and choose the SemVer impact
independently. Write the body for a Zinder user or operator: state the behavior
that changed and omit implementation details that do not affect them. The pull
request number links the final changelog entry back to its review context.

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
