# Review lessons

Problems that earlier reviews of b2-sdk-python found (or that follow-up PRs had to fix), grouped by theme,
with the PRs where they came up. Use it as a checklist before asking for review, and add a line when a
review finds something that will matter again. Rules that apply to every change are in
[AGENTS.md](../AGENTS.md).

## Retries, reauth and upload tokens

- Uploads are sent with `try_count=1`; `UploadManager` retries with a new upload URL and token. Retrying
  inside `b2http` reuses a stale URL. (#522, #548, #556, #563, #618)
- A change to `post_content_return_json` affects every POST, not only the call you meant to fix. (#548)
- `Retry-After` may be seconds or an HTTP date; `int()` on the date form raises. Parse it through
  `_parse_retry_after_seconds`, and treat all retryable 5xx alike. (#550, #574, #583, #619)

## Tests

- Revert the fix and the test must fail; show it in the PR. Good examples: #607, #620, #633.
  (#535, #548, #576, #618, #619)
- Assert the mechanism (the sleep actually taken, the new upload URL fetched), not an exception attribute.
  (#548, #618)
- Do not mask failures: no catch-all `except`, no generic `pytest.fail` hiding the cause, no unawaited
  async tests. (#550, #566, #574, #623)

## apiver layers and public interface

- Export new public names from `b2sdk/v3` (`__init__.py`, `exception.py`, `testing/__init__.py`).
  (#547, #557, #569, #618)
- Older layers must keep their own behavior and must not rely on v3-only abstractions. (#538, #541, #542,
  #559, #581)
- Do not call `_private` members across classes; keep tunables as module-level constants; name parameters
  the SDK way (`content_disposition`) and thread them up to `Bucket`. (#576, #581)
- Avoid names that collide in meaning with existing ones (`RequestTimeout` vs `B2RequestTimeout`). (#618)

## Simulator parity

- `RawSimulator` changes in the same PR as `AbstractRawApi` and `B2RawHTTPApi`. (#543, #548, #576, #604)

## Portability

- Windows: do not reopen a `NamedTemporaryFile` by name; handle permission and symlink cases; prefer
  `stat.S_ISDIR` on `stat` results in `scan/folder.py`. (#525, #604, #607)
- New Python versions surface warnings and API changes first; run the newest one locally. (#515, #560)

## Integration tests and live B2

- Use `bucket_manager.py` and a temporary account-info dir; clean up buckets and keys. (#547, #554, #557,
  #564, #574)
- The live-B2 stage has known failures tracked in #614; reproduce on `master` before calling a failure
  unrelated. (#611, #613, #618, #619, #620)

## sdkharness

- Checks target only loopback simulators with the fixed test credentials, refuse ambient `B2_*` values,
  report FAIL (not SKIP) on real errors, and refuse a simulator that already served requests.
  (#620, #623, #624, #628)

## Changelog and PR hygiene

- Every PR adds a towncrier fragment about the user-facing change. (#514, #522, #560, #576, #583)
- Search for open PRs first; one change per PR, against `master`. Duplicates and splits: #516/#517,
  #527/#530, #551/#552, #576/#578/#579, #608/#615/#618, #609/#616/#617/#619.
- Behavior changes hidden in a fix (a new SSE default, rejecting an option) need their own fragment and
  a reviewer's attention. (#604)
- Docs: a Sphinx or theme upgrade must be checked on Read the Docs, not only locally. (#567, #577, #582)
