# Release Process

Releasing **b2sdk** is part automated and part manual. A `nox` session prepares the changelog, a
maintainer commits and tags by hand, and the *Continuous Delivery* workflow
([`.github/workflows/cd.yml`](.github/workflows/cd.yml)) does everything from the tag onwards.

Nothing in the release is triggered by merging to `master`. Publishing begins only when a `v*` tag is
pushed.

## Prerequisites

* **A clean worktree.** `make_release_commit` aborts if `git diff --stat` reports anything. Note that
  it inspects tracked files only, so untracked files will not stop it.
* **Release from `master`.** The session only *warns* when you are on another branch, it does not stop
  you. Releasing from a feature branch is almost never what you want.
* **A version of the form `X.Y.Z`**, with integers and no leading `v` and no suffix. Anything else is
  rejected.
* **An `upstream` remote** pointing at the release repository. `noxfile.py` defines the expected URL as
  `UPSTREAM_REPO_URL`; the session prints a command to add it if it is missing.
* **Changelog fragments** for everything going out. Every PR should already have added one to
  `changelog.d`; review that directory before starting, because whatever is there becomes the release
  notes.

## Steps

### 1. Build the changelog

```
nox -s make_release_commit -- X.Y.Z
```

Despite the session's name, **this does not commit and does not tag.** It runs `towncrier build`, which
folds the `changelog.d` fragments into `CHANGELOG.md` and deletes the fragment files, then prints the
commands for the remaining steps. Review the resulting `CHANGELOG.md` diff before going further — this
is the last convenient moment to fix wording.

### 2. Commit and push

```
git commit -m "release X.Y.Z"
git push upstream master
```

### 3. Wait for CI

Let the *Continuous Integration* workflow finish on the pushed commit. Tagging a commit whose CI is red
means publishing a broken release, and a version number cannot be reused on PyPI.

### 4. Tag and push the tag

```
git tag vX.Y.Z
git push upstream vX.Y.Z
```

Pushing the tag is the point of no return.

## What the CD workflow does

Triggered by any pushed tag matching `v*`:

1. Decides whether the release is a prerelease, based only on whether the tag's last character is a
   digit. `v1.2.3` is a normal release.
2. Builds the distribution with `nox -s build`, which also asserts the built package imports from
   `site-packages` rather than the checkout. The version is derived from the tag, not from any file.
3. Reads the notes for that version out of `CHANGELOG.md`. If the tag and the changelog heading do not
   match, there will be no release body.
4. Creates a GitHub release and attaches the distribution.
5. Uploads to PyPI — **only** when the `B2_PYPI_PASSWORD` secret is present *and* the release is not a
   prerelease. A repository without that secret still gets a GitHub release and silently publishes
   nothing to PyPI, so do not treat a green CD run as proof that PyPI was updated.

## After the release

* Confirm the GitHub release exists, has release notes, and has the distribution attached.
* If a PyPI publish was expected, confirm the new version is actually on PyPI.
* Confirm `pip install b2sdk==X.Y.Z` resolves.

## If something goes wrong

### Before the tag is pushed

Everything is still local and reversible. Remember that `towncrier build` **deleted** the changelog
fragments, so recovering means restoring them:

* Fragments deleted but not yet committed: `git checkout -- changelog.d CHANGELOG.md`.
* Already committed: `git reset --hard HEAD~1` (only if the commit has not been pushed), or revert it.

Then fix the problem and start again from step 1.

### After the tag is pushed, before PyPI published

Delete the tag locally and upstream, fix the problem, and re-tag:

```
git push upstream :refs/tags/vX.Y.Z
git tag -d vX.Y.Z
```

Delete the GitHub release too, if one was created. Re-using the version number is safe only while
nothing has reached PyPI.

### After PyPI published

The version is spent. PyPI does not allow re-uploading a version, even after deleting it. Do not try to
reuse the number — fix the problem and release the next patch version instead. If the published release
is actively harmful, yank it on PyPI (which hides it from new resolutions without breaking pins that
already reference it) and follow up with a fixed release.
