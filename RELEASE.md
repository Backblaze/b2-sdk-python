# Releasing b2sdk

A release is a reviewed preparation PR merged into `master`, followed by a
maintainer's tag on that PR's merge commit. Hatch-vcs derives the package version
from Git; do not add a static version to `pyproject.toml` or `b2sdk/version.py`.
Go and Changie manage the release notes, following Backblaze/blazer's setup.

## One-time setup

- Install Git, GitHub CLI, Go, uv and Python 3.14. Signed GPG tags also need GnuPG;
  SSH-signed tags need Git 2.34+ and OpenSSH. Authenticate `gh` with this repository.
- Configure a PyPI **Trusted Publisher** for project `b2sdk`, owner `Backblaze`,
  repository `b2-sdk-python`, workflow filename `cd.yml`, environment `pypi`.
  A PyPI project owner must do this at
  <https://pypi.org/manage/project/b2sdk/settings/publishing/>. Protect the GitHub
  `pypi` environment with maintainer approval and tag deployment rules. OIDC replaces
  the old publishing credential; remove that credential after the migration is verified.
- The publish job has `id-token: write` and explicitly enables PEP 740 attestations.
  It downloads built distributions and does not build code or install build dependencies.
- Ensure the release gate can read Code scanning alerts, Dependabot alerts and
  Secret scanning alerts. If `GITHUB_TOKEN` cannot read all three, configure
  `RELEASE_SECURITY_TOKEN` with those read permissions (prefer an installation
  token managed by the organization). An unreadable or malformed source **holds**
  the release; disabled scanners are not silently treated as clean.
- Select and review the intended maintainer accounts in `.github/signing-maintainers`:
  `goanpeca`, `ffumero2003`, `sophiecarreras`. They are GitHub usernames, not fixture keys.

### Enroll signing keys before enforcement

There is no enrolled-key baseline yet. Each maintainer must create a personal GPG
or SSH signing key, register its public key with their GitHub account and configure
Git to sign tags. Review fingerprints with the maintainer through an independent
channel. The workflow resolves public keys from `https://github.com/<username>.gpg`
and `.keys` at check time into a temporary keyring/allowed-signers file, and runs
`git verify-tag`. Do not commit a private key or test key to the maintainer list.
An SSH signing key must be present in the public `.keys` response used by the verifier.

`ENFORCE_SIGNED_TAGS` is a repository variable. Unset or `false` means **warn-only**:
an unsigned, unknown-key or unverifiable tag is reported but does not hold the
release. Once every intended release signer is enrolled and a signed-tag check has
been verified, a repository administrator enables enforcement:

```sh
gh variable set ENFORCE_SIGNED_TAGS --repo Backblaze/b2-sdk-python --body true
```

With enforcement enabled, signature failures and public-key lookup failures hold
the release. GitHub account key enrollment is the trust source; there are no
committed fingerprints. Key rotation and account security therefore require review.

## Security gate and manual review

Run the same gate used by delivery before planning a release:

```sh
uv run --group nox python scripts/release_security.py Backblaze/b2-sdk-python
```

It holds for medium/high/critical CodeQL security alerts, medium/high/critical
Dependabot alerts with an available fix, and every open secret-scanning alert
(that API has no severity field). Low alerts and dependencies without a patched
version do not hold this gate. The output contains source counts only. Resolve or
have an authorized owner dismiss findings in their source system with a reason;
the gate has no finding waiver flag. An API error is `UNVERIFIED` and exits nonzero.

The public workflow cannot query the organization's private Snyk tracking mirror.
Before approving the release, a maintainer must manually review unresolved
`severity:`-labelled findings in `Backblaze/b2-sdk-python_private`, following the
Confluence page **Security considerations for Backblaze Labs projects**. Coordinate
any medium/high/critical findings with the security owner. Keep private finding
details out of public issues, PRs, release notes and workflow logs.

## Plan and prepare the release PR

1. Assign intended public work to a release milestone, review the merged changes
   since the last tag, and agree the semantic version. Breaking changes require a
   major, compatible features a minor, fixes/tooling a patch; choose deliberately
   when the release has mixed kinds. Changie's `auto` is a preview, not approval.
2. Start from clean, current `master`; this is a hard preparation check:

   ```sh
   git fetch origin --tags
   git switch master
   git pull --ff-only origin master
   git status --short
   gh run list --repo Backblaze/b2-sdk-python --branch master --workflow ci.yml --limit 1
   git log --oneline vLAST..origin/master
   uv run --group nox python scripts/release_security.py Backblaze/b2-sdk-python
   ```

   Require successful CI for the actual current master commit. A pending, absent
   or failed run is a hold; do not release while main is red.
3. Review `.changes/unreleased/` and prepare an explicit version:

   ```sh
   go run github.com/miniscruff/changie@v1.24.0 batch patch --dry-run
   uv run --locked --group nox nox -vs make_release_commit -- X.Y.Z
   git switch -c release/vX.Y.Z
   ```

   The Nox command requires a clean master worktree, including untracked files;
   rejects an existing tag; runs `changie batch vX.Y.Z` then `changie merge`; and
   stops. It never commits, tags or publishes. It creates `.changes/vX.Y.Z.md`,
   consumes the unreleased fragments and updates `CHANGELOG.md`. Historical notes
   remain in `.changes/v2.13.4.md`; do not rewrite that archive. Review the new
   `## [X.Y.Z]` section and regenerate with `changie merge` after editing its batch.
   For a prerelease, pass the full SemVer version instead, for example
   `uv run --locked --group nox nox -vs make_release_commit -- 2.14.0-rc.1`.
   Keep that exact suffix in the branch, Changie batch and tag (`v2.14.0-rc.1`);
   preparing `2.14.0` does not prepare the notes for `2.14.0-rc.1`.
4. Run local verification with no production credentials in the environment:

   ```sh
   uv run --locked --group nox nox -vs lint unit build doc changelog_check
   ```

   `unit` covers v0–v3 on the configured Python interpreters. CI additionally
   exercises the OS/Python matrix, CLI compatibility and credentialed integration
   tests when secrets are available. A local skip is not evidence for a live test.
5. Run the local customer health check against a fresh loopback simulator. From
   the central harness checkout (see `.sdkharness/README.md` for manual simulator
   startup and the fixed test credentials):

   ```sh
   B2SIM_SOURCE=pinned sdkharness/bin/run-health-checks.sh --target simulator --versions head b2-sdk-python
   ```

   This requires the harness to have a clean SDK clone at the release candidate.
   Verify the printed revision; a PASS for another checkout does not verify this
   candidate. The repository-owned health executable is `.sdkharness/tests/health-golden-path`.
6. Review the diff, commit and open the release PR:

   ```sh
   git add CHANGELOG.md .changes
   git commit -m 'chore(release): X.Y.Z'
   git push -u origin release/vX.Y.Z
   gh pr create --base master --title 'chore(release): X.Y.Z' --assignee @me
   ```

   Include the changelog, exact commit scope and verification evidence; request a
   human reviewer. If the scope is tooling-only, say so. Wait for approval and
   green PR CI. The maintainer merges; release preparation does not authorize a tag.

## Non-publishing workflow dry run

After the dispatch-enabled `cd.yml` is on the default branch:

```sh
gh workflow run cd.yml --repo Backblaze/b2-sdk-python --ref release/vX.Y.Z -f dry_run=true
```

The dry run checks current master CI and the live security sources, assembles a
disposable Changie batch, extracts its notes, builds an untagged development
distribution, smoke-tests the installed v0–v3 modules and uploads CI artifacts.
Neither the PyPI publisher nor `gh release create/edit/upload` runs on dispatch;
`dry_run=false` is rejected. No tag is created. This does not prove PyPI's OIDC
configuration or attestation upload; those are only exercised by an actual approved
publish. TestPyPI is not configured by this workflow and dispatch does not publish there.
GitHub may reject dispatch before the workflow trigger exists on master; record
that as a verification limit rather than bypassing the gate.

## Tag and publish (maintainer only)

After the preparation PR merges, fetch its exact merge commit and verify its CI.
Tag that commit, which must be reachable from current `origin/master`:

```sh
git fetch origin --tags
git switch master
git pull --ff-only origin master
sha=$(gh pr view RELEASE_PR --repo Backblaze/b2-sdk-python --json mergeCommit --jq .mergeCommit.oid)
git log -1 --oneline "$sha"
git merge-base --is-ancestor "$sha" origin/master
git tag -s vX.Y.Z "$sha" -m 'vX.Y.Z'
uv run --group nox python scripts/verify_release_tag.py vX.Y.Z
git push origin refs/tags/vX.Y.Z
```

Configure `gpg.format=ssh` and `user.signingkey` for SSH, or the GPG signing key for
GPG, before `git tag -s`. The tag workflow requires successful master and tagged
commit CI, master ancestry, the configured signature gate, a clean security gate,
matching Changie version/notes and no remaining fragments before publishing.
Stable tags publish to PyPI through OIDC with PEP 740 attestations. Prerelease tags
create a GitHub prerelease without a PyPI upload, matching the prior stable-only
publication policy. The publishing job requires the protected `pypi` environment.

After successful PyPI upload, the workflow creates the GitHub release with the
exact changelog section. Re-running the GitHub-release job edits existing notes
and replaces assets via `gh release upload --clobber`. There are no long-lived
PyPI credentials in the workflow. Confirm the release, package version, provenance
and a fresh install before declaring the release complete.

## Recovery

- Before a tag is pushed: fix the PR, repeat checks and obtain approval.
- A security, CI, ancestry, signature or unreadable-source hold requires resolving
  the underlying condition and re-running checks; never disable a gate to ship.
- After a tag is public: do not move or delete it. Use a new version for a new build.
- If publication fails, inspect registry state and attestations before retrying.
  PyPI duplicates fail rather than silently accepting a different artifact. If
  PyPI already has the correct files but GitHub release creation failed, complete
  the GitHub release with the verified artifacts and extracted notes; do not
  rebuild and overwrite a published package. Record the recovery in the release PR.
