# Releasing b2sdk

b2sdk is versioned from git tags (`hatch-vcs`): a release is a `release X.Y.Z` commit on `master` and a
`vX.Y.Z` tag on that commit. Pushing the tag runs [`cd.yml`](.github/workflows/cd.yml), which builds the
distribution, creates the GitHub release from the changelog section and uploads to PyPI.

## What a release commit contains

Every recent release (for example 2.13.2, `17a6be76`) changed exactly these, in one commit made by
`nox -s make_release_commit`:

| File | Change | Done by |
| --- | --- | --- |
| `CHANGELOG.md` | New `## [X.Y.Z](...) - YYYY-MM-DD` section under the towncrier marker | `towncrier build` |
| `changelog.d/*` | Every released fragment deleted (`.gitkeep` stays) | `towncrier build` |

No version string lives in the code; `b2sdk/version.py` reads it from the installed package.

## Versioning

Semantic versioning. The apiver layers (`b2sdk.v0`..`b2sdk.v3`) let a minor release add a new interface
without breaking the old ones, so a breaking change normally ships as a new apiver in a minor release,
not as a major bump. Pick the version from the fragments: any `added`, `changed`, `deprecated` or
`removed` → minor; only `fixed`, `doc` or `infrastructure` → patch.

## Steps

1. Make sure `master` is green (including `cli-compat`) and every PR meant for the release has merged.
   Check the [Security tab](https://github.com/Backblaze/b2-sdk-python/security) for open Dependabot,
   code scanning or secret scanning alerts that have a fix, and fix them first.
2. Preview the changelog: `uv run --group release towncrier build --draft --version X.Y.Z`.
3. Create the release commit from an up-to-date, clean `master`:

   ```bash
   nox -s make_release_commit -- X.Y.Z
   ```

4. Check the diff touches only `CHANGELOG.md` and `changelog.d/`, then follow the commands the session
   prints: commit as `release X.Y.Z` and push it.
5. Wait for the CI workflow on that commit to pass. Only then tag and push the tag:

   ```bash
   git tag vX.Y.Z
   git push upstream vX.Y.Z
   ```

6. Wait for the CD workflow to pass. Check the [GitHub release](https://github.com/Backblaze/b2-sdk-python/releases)
   and [PyPI](https://pypi.org/project/b2sdk/) show X.Y.Z.
7. If the release fixes something the B2 CLI needs, open a PR in
   [`B2_Command_Line_Tool`](https://github.com/Backblaze/B2_Command_Line_Tool) bumping its `b2sdk` bound.

## Never

- Tag a commit whose CI has not passed.
- Move or delete a published tag, or re-upload a version to PyPI. Release a new patch version instead.
- Release from a branch other than `master` without a reason recorded in the release PR or commit.
- Hand-edit a released `CHANGELOG.md` section without a reason recorded in the PR.
