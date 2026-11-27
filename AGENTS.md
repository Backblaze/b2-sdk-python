# b2sdk

Python SDK for Backblaze B2 Cloud Storage (`b2sdk` on PyPI, `Backblaze/b2-sdk-python`). The B2 CLI is
its largest user. This file is the map for coding agents and new contributors; `CLAUDE.md` points here.
Keep it under about 150 lines and link out for detail. [`llms.txt`](llms.txt) lists the user documentation.

## Commands

Tooling is `nox` + `uv` (`pip install nox uv`). Sessions are defined in `noxfile.py`.

- Local CI, run before every push and paste the tail in the PR:
  `nox -s lint`, `nox -s towncrier_check`, `nox -s unit-3.14`, `nox -s build`, `nox --non-interactive -s doc`.
- Format: `nox -s format` (ruff). Lint: `nox -s lint` (ruff, `test/static`, liccheck).
- Unit tests run once per apiver: `nox -s unit-3.14` runs `pytest --api=v3`, then `v2`, `v1`, `v0`.
  By keyword: `nox -s unit-3.14 -- -k keyword`. One apiver: `uv run --group test pytest --api=v2 test/unit`.
- Integration tests hit live B2 and need `B2_TEST_APPLICATION_KEY_ID` and `B2_TEST_APPLICATION_KEY`:
  `nox -s integration-3.14`. Leftover test buckets: `nox -s cleanup_old_buckets`.
- Docs: `nox --non-interactive -s doc` (needs `graphviz` and `plantuml`) also runs `doc_cover`, which fails on
  undocumented API. Without `--non-interactive` in a terminal it starts a live-reload server instead.
- Lock file: `uv lock --check`. Dependency changes go through `uv add`/`uv lock`, never by hand.
- Changelog fragment: `uv run --group release towncrier create -c 'Text.' +slug.fixed.md`.
- Release: [README.release.md](README.release.md) (`nox -s make_release_commit -- X.Y.Z`).

External tools:

- **[b2-mcp](https://github.com/backblaze-labs/b2-mcp)** — a Model Context Protocol server for B2.

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
| `doc/source/` | Sphinx user documentation. `docs/` holds the testing and writing style guides. |
| `changelog.d/` | towncrier fragments; `CHANGELOG.md` is built from them at release. |

## Conventions

- Python 3.10+ (`requires-python`); CI runs 3.10-3.14 and PyPy 3.10 on Linux, macOS and Windows.
- ruff, 100-column lines, single quotes. Every file keeps the Backblaze license header (`test/static` checks it).
- Semver on each apiver. A breaking change goes into a new apiver (or the newest, unreleased one), never
  into an older layer. Deprecate with a warning before removing.
- New B2 calls follow the B2 docs and the
  [Integration Checklist](https://www.backblaze.com/docs/cloud-storage-integration-checklist).
- Comments are short and explain non-obvious decisions. Public API gets docstrings that Sphinx renders.
- Write and edit docs and docstrings as described in [docs/writing-style.md](docs/writing-style.md).
- Detail: test levels and how to run them in [docs/testing.md](docs/testing.md), contributor setup in
  [CONTRIBUTING.md](CONTRIBUTING.md), the harness in [.sdkharness/README.md](.sdkharness/README.md).

## Things agents get wrong here

Each line comes from recurring review feedback on this repository.

1. **Missing or wrong changelog fragment.** Every PR adds `changelog.d/<issue>.<type>.md` or
   `+slug.<type>.md` (types in `pyproject.toml`). Write it for SDK users, about the fix, not the test.
2. **Tests that do not pin the fix.** Revert the change and the new test must fail. Exercise the mechanism
   (the real sleep in `_translate_and_retry`, a new upload URL fetched after a 408), not an attribute.
   Server-format fixes get a simulator unit test and an integration test.
3. **Reordered or inserted positional parameters.** New optional parameters go last (or keyword-only);
   never reorder. Callers pass many positionals (`Bucket.upload_bytes`), and a shift is silent.
4. **apiver layering.** Change only the unreleased layer; a released `b2sdk/vN` keeps its behavior through
   a wrapper in `b2sdk/vN/` that forwards `*args, **kwargs`. Export new public names from `b2sdk/v3`
   (`__init__.py`, `exception.py`, `testing/__init__.py`). Compat shims kept for the CLI get existence and
   signature tests. Run the unit tests on every `--api`.
5. **Raw API drift.** `AbstractRawApi`, `B2RawHTTPApi` and `RawSimulator` change together with matching
   signatures and argument names. Check live B2 before changing simulator behavior; keep it deterministic
   and bounded in memory. No `requests` types in the raw API interface.
6. **Retries in the wrong place.** Uploads go out with `try_count=1` so `UploadManager` retries with a
   fresh upload URL and token; never retry them in `b2http.py`. Treat every retryable 5xx alike and parse
   `Retry-After` through `_parse_retry_after_seconds`.
7. **Accidental public interface.** Do not call another class's `_private` members. Tunables are named
   constants: module-level, or a class attribute (`DEFAULT_*_CLASS`, `B2Http.TIMEOUT`) when a subclass or
   apiver layer may override it. Logic lives in `B2Api` and `Bucket` delegates; use `_by_id`/`_by_name`.
8. **Hidden failures.** No bare `Exception` (subclass in `exception.py`), no catch-all `except`, no
   `pytest.fail` that drops the real error, no unawaited async test. A skipped check is NOT RUN, not PASS.
9. **Undocumented public API.** Every new public class gets a docstring and every new parameter a `:param`,
   with units and whether ranges are inclusive. `doc_cover` fails on the first, reviewers on the second.
10. **Sync and scan safety.** Reject `..` traversal, decide absolute paths per platform, allow names legal on
    the OS, and never silently drop user files. Windows: do not reopen a `NamedTemporaryFile` by name.
11. **Large files and transfers.** A new upload option flows through emerger, executor and the
    unfinished-file matcher, and a mismatch must not resume. Cover zero length, the 10,000-part limit and
    >5 GB. Thread pools: lazy init and `get` share a lock; one `max_workers` default in one place.
12. **Test and integration hygiene.** `parametrize`, not loops of asserts; no nested functions (use
    `_method`); `@pytest.mark.apiver` only where behavior differs. Integration tests use
    `bucket_manager.py` and never touch real account info or leave buckets behind.
13. **sdkharness safety.** Checks target only `http://127.0.0.1:<port>` with `test-key-id`/`test-key`;
    never read or print ambient `B2_*` credentials.
14. **PR scope.** Search open PRs first; one change per PR, against `master`; no unrelated reformatting;
    file an issue for out-of-scope bugs. The live-B2 stage has known failures (#614): reproduce on `master`
    before calling a failure unrelated.

## Workflow

- Bug fixes: commit the failing regression test first, then the fix, without editing the test.
- Before opening a PR, review your own diff in a fresh context (a second agent, or a person who did not
  write it) and fix or answer what it finds. Say in the PR what it reported.
- Every review finding gets fixed or answered with a reason. Agents never approve or merge their own PRs;
  a maintainer approves.
- The person directing an agent owns its PR: they count as one of its reviewers and fix what it breaks.
  Keep PRs small so they are easy to review and revert.
- When reviewers flag the same thing twice, fix it at the source in the same PR: this file, a lint rule
  or a test.
- Stacked PRs: state the base PR and merge order in the description; retarget to `master` after it merges.
- Security reports go through [SECURITY.md](SECURITY.md), never a public issue.

## Verifying your work

Run the local CI commands above and paste the tail of their output. If a test fails, fix the code, not
the test. Say which apivers and Python versions you ran, and list any integration or `.sdkharness` checks
you ran and any you could not.
The `verify` skill in [.claude/skills/verify/SKILL.md](.claude/skills/verify/SKILL.md) wraps these commands.
