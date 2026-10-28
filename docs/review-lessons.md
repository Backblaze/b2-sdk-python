# Review lessons

What reviewers of b2-sdk-python have asked for, grouped by theme, with the PRs where it came up. Use it
as a checklist before asking for review, and add a line when a review finds something that will matter
again. The rules that apply to every change are in [AGENTS.md](../AGENTS.md); the review process is in
[REVIEW.md](../REVIEW.md).

## Where this comes from

All 370 merged PRs (#1 to #632) were read, with every inline comment, review body and conversation comment
from people other than the author (528 comments on 120 PRs; bots excluded; Copilot had not reviewed any
merged PR). About 90% of that review content is on PRs before #514. Lessons marked *(follow-up)* come
instead from later PRs that had to fix or redo an earlier one, or from closed duplicates, not from a
review comment. Rules that no longer apply (Python 2, yapf, Travis, absolute-imports-only) are left out.

## apiver layers and public interface

- Breaking changes go only into the unreleased apiver. A released `b2sdk/vN` keeps its old signature
  through a wrapper in `b2sdk/vN/`. (#229, #297, #456)
- Subclass the next layer explicitly (`class B2Api(v1.B2Api)`); wrappers forward `*args, **kwargs` so new
  arguments reach every layer. (#43, #397)
- Swap internal classes in wrappers through `DEFAULT_*_CLASS` class attributes, never `mock.patch`.
  Helpers that exist only for an old layer live in `b2sdk/vN/`, not `_internal`. (#297)
- Compat methods kept for the B2 CLI (for example v2 `set_thread_pool_size`) get existence and signature
  tests and must not call private upstream members. (#297)
- Export new names, including exceptions, from `b2sdk/v3`. (#618; follow-up #547, #557, #569)
- Older layers must not depend on v3-only abstractions. *(follow-up #538, #541, #542, #559, #581)*
- Docs and docstrings reference `b2sdk.vN.Name`, never `_internal` paths; document differences between
  layers (for example the v1 `list_keys` cap). (#152, #156, #270)
- Decide deliberately whether a new helper is public. (#398, #447)

## Backward compatibility and deprecation

- New optional parameters go last, or keyword-only; never reorder, including constructors of public value
  classes like `FileVersionInfo`. (#233, #297, #383)
- Changing a positional signature is breaking: new apiver plus a wrapper in the old one. (#456)
- Deprecate with the `version_utils` decorators before removing. (#26, #28, #172)
- A behavior change that can drop user data (for example a symlink depth limit in scan) is Important,
  never silent. (#390)
- A new default (for example SSE-B2) or a newly rejected option is a behavior change with its own
  changelog entry. *(follow-up #604)*

## Changelog

- One fragment per PR, with the right type, named for the issue when there is one. (#120, #127, #132,
  #163, #213, #293, #303, #459, #514)
- Describe the user-visible change, not the test. (#560)
- Dependency pins added for a security report still need a fragment. (#429)

## Tests

- Cover every new branch and edge case. (#120, #128, #141, #383, #388)
- Server-format fixes get a simulator unit test and an integration test. (#299, #301, #303)
- Drive the real mechanism: the sleep taken, the new upload URL fetched. Revert the fix and the test must
  fail. (#618, #619)
- `@pytest.mark.apiver` only on tests whose behavior differs by apiver; update `v0`/`v1` tests or move
  the test to `v_all`. (#90, #172, #253, #361)
- `parametrize` instead of loops of asserts; no redundant asserts, duplicated fixtures or debug leftovers;
  check the test calls the function its name says. (#310, #315, #352, #383, #388, #390)
- Tests never touch the developer's real account info or environment. (#314, #447)
- Do not hide failures: no catch-all `except`, no generic `pytest.fail`, no unawaited async tests.
  *(follow-up #550, #566, #623)*
- Review new test dependencies as supply-chain risk. (#512)

## Raw API and RawSimulator

- `AbstractRawApi`, `B2RawHTTPApi` and `RawSimulator` keep matching signatures and argument names
  (`bucket_id` vs `bucket_name`). (#89, #172)
- Fix simulator bugs in the PR that needs them, after checking what live B2 does. (#120)
- The simulator stays deterministic (seeded) and bounded in memory. (#89, #141)
- No `requests` types in the raw API interface; transport details stay in `b2http`. (#172, #279)
- Raw-layer knowledge (default token types) stays in the raw layer. (#84)

## Retries, timeouts and upload tokens

- Timeouts are named constants on `B2Http`, not exactly the server's timeout. (#33, #329)
- Uploads retry in `UploadManager` with a fresh URL and token, never in `b2http`; one token serves one
  upload at a time. (#89, #618)
- Parse `Retry-After` safely (it may be an HTTP date) and treat all retryable 5xx alike. (#619)
- Do not build a new `random.Random` per call. (#329)
- Key-change detection must keep clearing caches between master and non-master keys. (#184)
- Verify the downloaded length against the expected size. (#32)

## Threading and transfer managers

- Lazy pool creation and `get` take the same lock; `super()` chains cooperatively. (#297)
- Each default worker count is set once, named `max_workers`; think through `workers=1` and how sync and
  download pools add up. (#297, #352)
- Validate worker and stream counts; keep per-thread memory O(1). (#352, #359)

## Large files, copy and encryption

- Document whether range ends are inclusive. (#55)
- Zero-length files and copies; copy (class C transaction) vs re-upload. (#165)
- Respect `PART_COUNT_LIMIT` (10,000) and replan; files over 5 GB with one worker; use 200 vs 206 status,
  not `Content-Range`. (#32, #247, #297, #309)
- A new upload option flows through emerger, executor and the unfinished-file matcher; a mismatch must
  not resume that file. (#383, #388)
- Expose the verified flag next to a checksum. (#153, #299)
- Bucket lookups never confuse ids with names. (#229)

## Sync, scan and paths

- Reject `..` traversal; decide which absolute paths are allowed per platform; allow names legal on the
  current OS (backslashes on Linux). (#125)
- Scan policies receive a `FileVersion`, not a raw mtime; comment why mtime is rounded. (#128, #132)
- Document that bucket-to-bucket sync copies only the newest version. (#165)
- Use `pathlib` in new code; iterate directories lazily. (#235, #390)
- Windows: do not reopen a `NamedTemporaryFile` by name; handle permissions and symlinks.
  *(follow-up #525, #604, #607)*

## Account info

- Follow `platformdirs` semantics for XDG paths; keep helpers private. (#235, #447)
- `SqliteAccountInfo` creates its file owner-only before writing credentials.

## Exceptions, docstrings and typing

- Never raise bare `Exception`; subclass in `exception.py` and override `__str__`; never swallow errors.
  (#89, #125, #172, #270, #550)
- Every new class gets a docstring and every new parameter a `:param` with units; prefer type hints over a
  duplicate `:rtype:`; drop docstrings that add nothing. (#52, #55, #89, #165, #172, #270, #297, #361, #383)
- A "why" comment on non-obvious branches. (#132, #141, #234, #270)
- Type touched signatures; `Optional` when the default is `None`; abstract collection types; no return
  annotation on `__init__`. (#172, #297, #329, #390, #475)

## Naming and structure

- Precise names (`parameter_name`, not `field_name`); one meaning per parameter name. (#52, #329, #618)
- `_by_id`/`_by_name` pairs; logic in `B2Api`, `Bucket` delegates in one line. (#172)
- Class constants and factories (`self.BUCKET_FACTORY`) so subclasses can override; module-level
  constants otherwise. (#33, #270, #297, #299, #390)
- No nested functions; use private methods. Compile regexes at module level; module-level loggers.
  (#125, #172, #297, #390)
- Parse opaque server strings into typed objects; duck typing in `__eq__`. (#90, #220)

## Integration tests and live B2

- Use `bucket_manager.py` and a temporary account-info dir; clean up buckets and keys.
  *(follow-up #547, #554, #557, #564)*
- Do not depend on account-gated features (custom upload timestamps); check values round-trip. (#383)
- Integration tests cannot run from forks (no secrets); say what you ran. (#172)
- The live-B2 stage has known failures (#614); reproduce on `master` before calling one unrelated.

## sdkharness

- Checks target only loopback simulators with fixed test credentials, refuse ambient `B2_*` values,
  report FAIL (not SKIP) on real errors, and refuse a used simulator. *(follow-up #620, #623, #624, #628;
  no merged sdkharness PR had a review)*

## CI, nox and packaging

- Shell steps keep the exit status. (#35)
- Ship only what users need in the sdist; run the import test against the build. (#154, #337)
- Environment markers on backports; check what nox `reuse_venv` does before changing it. (#277, #561)
- Keep the CD release steps working when changing actions. (#392)
- A Sphinx or theme upgrade is checked on Read the Docs, not only locally. *(follow-up #567, #577, #582)*

## PR process

- Self-review before asking for review. (#383, #390)
- Split PRs that are too large to review (one at 2,000 lines was "barely reviewable"). (#184, #221)
- No unrelated reformatting; file an issue for out-of-scope bugs; squash noisy histories.
  (#125, #165, #297, #412, #512)
- Search for duplicates first; one change per PR, against `master`. *(follow-up #576/#578/#579,
  #608/#615/#618, #609/#616/#617/#619)*

## Candidates for automation

Checks reviewers keep making that a tool could make instead (see `intent/review-process/intent.md`):

- A per-apiver signature snapshot test: catches reordered parameters, broken wrappers and missing exports.
- A raw API parity test across `AbstractRawApi`, `B2RawHTTPApi` and `RawSimulator`.
- ruff rules beyond `E`, `F`, `I`, `UP`: `BLE001`, `TRY002`, `B`, `PT006`/`PT007`, `T20`, `D417`.
- A changelog check scoped to `b2sdk/` changes.
