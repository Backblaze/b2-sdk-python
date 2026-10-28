# b2sdk

Python SDK for Backblaze B2 Cloud Storage (`b2sdk` on PyPI, `Backblaze/b2-sdk-python`). The B2 CLI is
its largest user. This file is the map for coding agents and new contributors; `CLAUDE.md` points here.
Keep it under about 150 lines and link out for detail.

## Commands

Tooling is `nox` + `uv` (`pip install nox uv`). Sessions are defined in `noxfile.py`.

- Local CI, run before every push and paste the tail in the PR:
  `nox -s lint`, `nox -s towncrier_check`, `nox -s unit-3.14`, `nox -s build`, `nox -s doc`.
- Format: `nox -s format` (ruff). Lint: `nox -s lint` (ruff, `test/static`, liccheck).
- Unit tests run once per apiver: `nox -s unit-3.14` runs `pytest --api=v3`, then `v2`, `v1`, `v0`.
  By keyword: `nox -s unit-3.14 -- -k keyword`. One apiver: `uv run --group test pytest --api=v2 test/unit`.
- Integration tests hit live B2 and need `B2_TEST_APPLICATION_KEY_ID` and `B2_TEST_APPLICATION_KEY`:
  `nox -s integration-3.14`. Leftover test buckets: `nox -s cleanup_old_buckets`.
- Docs: `nox -s doc` (needs `graphviz` and `plantuml`) also runs `doc_cover`, which fails on undocumented API.
- Lock file: `uv lock --check`. Dependency changes go through `uv add`/`uv lock`, never by hand.
- Changelog fragment: `uv run --group release towncrier create -c 'Text.' +slug.fixed.md`.
- Release: [RELEASE.md](RELEASE.md) (`nox -s make_release_commit -- X.Y.Z`).

## Architecture

| Path | Role |
| --- | --- |
| `b2sdk/_internal/` | All implementation. Private: users never import it. |
| `b2sdk/v3/`, `v2/`, `v1/`, `v0/` | Public apiver layers. `v3` re-exports `_internal`; older layers wrap or subclass to keep their old signatures and behavior. |
| `_internal/api.py`, `bucket.py` | High-level `B2Api` and `Bucket`, the surface most callers use. |
| `_internal/session.py`, `b2http.py` | Auth, reauth, retries, `Retry-After`, exception translation. |
| `_internal/raw_api.py` | One method per B2 Native API call (`AbstractRawApi`, `B2RawHTTPApi`). |
| `_internal/raw_simulator.py` | In-memory B2 used by unit tests and by users; must match `raw_api.py`. |
| `_internal/transfer/`, `large_file/` | Upload, download, copy, emerger and large-file managers (thread pools). |
| `_internal/sync/`, `scan/` | Folder scan and sync (used by `b2 sync`). |
| `_internal/account_info/` | Credential and cache stores, including `SqliteAccountInfo`. |
| `test/unit/` | Unit tests; `test/unit/vN/apiver` and `v_all` hold apiver-specific tests, selected by `--api`. |
| `test/integration/` | Live-B2 tests, using `_internal/testing/helpers/bucket_manager.py`. |
| `.sdkharness/` | Contract checks the central SDK harness runs against the B2 simulator on loopback. |
| `changelog.d/` | towncrier fragments; `CHANGELOG.md` is built from them at release. |
| `intent/` | Per-change `intent.md`, `spec.md`, `plan.md` (see Workflow). |

## Conventions

- Python 3.10+ (`requires-python`); CI runs 3.10-3.14 and PyPy 3.10 on Linux, macOS and Windows.
- ruff, 100-column lines, single quotes. Every file keeps the Backblaze license header (`test/static` checks it).
- Semver on each apiver. A breaking change goes into a new apiver (or the newest, unreleased one), never
  into an older layer. Deprecate with a warning before removing.
- New B2 calls follow the B2 docs and the
  [Integration Checklist](https://www.backblaze.com/docs/cloud-storage-integration-checklist).
- Comments are short and explain non-obvious decisions. Public API gets docstrings that Sphinx renders.
- Detail: past review problems in [docs/review-lessons.md](docs/review-lessons.md), contributor setup in
  [CONTRIBUTING.md](CONTRIBUTING.md), the harness in [.sdkharness/README.md](.sdkharness/README.md).

## Things agents get wrong here

Each line was raised by a reviewer, or fixed by a follow-up PR, on at least two PRs.

1. **Missing or wrong changelog fragment.** Every PR adds `changelog.d/<issue>.<type>.md` or
   `+slug.<type>.md` (types in `pyproject.toml`). Write it for SDK users, about the fix, not the test.
2. **Tests that do not pin the fix.** Revert the change and the new test must fail. Exercise the mechanism
   (the real sleep in `_translate_and_retry`, a new upload URL fetched after a 408), not an attribute.
3. **Retrying uploads in the HTTP layer.** Uploads go out with `try_count=1` so the error reaches
   `UploadManager`, which gets a fresh upload URL and token. Do not retry them in `b2http.py`.
4. **Retry parity.** Treat every retryable 5xx alike, and parse `Retry-After` through
   `_parse_retry_after_seconds` so a non-numeric value (an HTTP date) never raises.
5. **Missing apiver exports.** A new public name is exported from `b2sdk/v3` (`__init__.py`,
   `exception.py`, `testing/__init__.py` as relevant); older layers must not pick up v3 behavior or
   rely on v3-only abstractions. Run the unit tests on every `--api`.
6. **`raw_simulator.py` drift.** Any change to `AbstractRawApi` or a B2 call's behavior lands in
   `RawSimulator` in the same PR.
7. **Accidental public interface.** Do not call another class's `_private` members; keep tunables as
   module-level `_CONSTANTS`, not new parameters; use SDK-style names and thread new options up to `Bucket`.
8. **Hidden failures.** No catch-all `except`, no `pytest.fail` that drops the real error, no async test
   that is never awaited. A skipped check is NOT RUN, never PASS.
9. **Portability.** Windows and new Pythons: do not reopen a `NamedTemporaryFile` by name; handle
   permissions and symlinks; check `scan/` on Windows paths.
10. **Integration tests without the helpers.** Use `bucket_manager.py` (seeded names, retries, cleanup) and
    a temporary account-info dir; never leave buckets or keys behind.
11. **sdkharness safety.** Checks target only `http://127.0.0.1:<port>` with `test-key-id`/`test-key`;
    never read or print ambient `B2_*` credentials.
12. **Duplicate and split PRs.** Search open PRs first; one change per PR, against `master`.
13. **Claiming green over a red CI.** The live-B2 stage has known failures (#614). Reproduce on `master`
    and cite the issue; never call a failure "unrelated" without showing it.

## Workflow (AI-native SDLC)

- Non-trivial changes start in `intent/<slug>/`: `intent.md` (problem) → `spec.md` (numbered requirements
  R1..) → `plan.md` (steps cite R numbers). A human approves each before the next. Templates in
  [`intent/README.md`](intent/README.md). Bug fixes with a linked issue may skip straight to a failing test.
- Bug fixes: commit the failing regression test first, then the fix, without editing the test.
- Skills in `.claude/skills/` (`ai-native-sdlc` routes to the right one). A `PostToolUse` hook runs ruff on
  every Python file Claude edits.
- Before opening a PR, review your own diff in a fresh context (`/code-review` and `/security-review`, or a
  subagent that did not write the code) and fix or answer what it finds. Say in the PR what it reported.
- Review: [REVIEW.md](REVIEW.md). Every finding gets fixed or resolved with a reason. Agents never approve
  or merge their own PRs; a maintainer approves.
- The person directing an agent owns its PR: they count as one of its reviewers and fix what it breaks.
- Stacked PRs: state the base PR and merge order in the description; retarget to `master` after it merges.
- Releases: [RELEASE.md](RELEASE.md). Security reports: [SECURITY.md](SECURITY.md).

## Verifying your work

Run the local CI commands above and paste the tail of their output. If a test fails, fix the code, not
the test. Say which apivers and Python versions you ran, and list any integration or `.sdkharness` checks
you ran and any you could not.
