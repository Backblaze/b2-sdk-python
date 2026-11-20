# Review instructions

For human reviewers and review agents. Approval always comes from a maintainer; an agent's findings never
approve or block on their own, and the agent that wrote a change never reviews it.

## What failure looks like here

b2sdk fails by sending the wrong request to B2 (a dropped, extra or mis-shaped field), by breaking a caller
of an older apiver layer (`b2sdk.v0`..`b2sdk.v2`) or the B2 CLI, by retrying what it should not (or not
retrying what it should), by leaving resources behind (unfinished large files, keys, buckets in
integration tests), by hanging a transfer thread, or by leaking a credential into a log or exception. A
test that passes either way is also a failure: it claims coverage it does not give.

## Before the passes

1. Read the linked issue and, if present, `intent/<slug>/spec.md` and `plan.md`. Ask: does the change
   solve its problem, fit the system, and leave everything else working? On a bug fix, find the root
   cause and check the fix is there, not at a symptom.
2. Check out the PR on current `master` and run the local CI commands from `AGENTS.md`. Say what you ran.
3. For each behavior the PR claims to fix or add, revert that line and confirm a test fails. Name any
   behavior no test pins.

## Passes

Tag each finding with its pass.

- **Bugs**: logic errors, off-by-one in ranges and part sizes, error paths that leak resources or
  unfinished large files, threads or executors that are never shut down, shared state touched without the
  lock that guards it, a new upload option the unfinished-file matcher ignores, scan or sync silently
  dropping user files.
- **Wire**: the request matches the B2 docs for the API version used; omitted vs `None` vs `[]` is
  deliberate; `raw_simulator.py` models the same behavior as `raw_api.py`; retry, reauth and `Retry-After`
  behave like sibling calls in `b2http.py`/`session.py`; the B2 transaction class (and cost) of new calls
  is deliberate (copy is class C).
- **Security**: secrets (application keys, auth tokens, SSE-C keys) never logged, put in `repr` or in an
  exception message or request body logs; capability checks fail closed; `SqliteAccountInfo` files are
  not world-readable; sync rejects `..` traversal; new dependencies (including test and dev ones) are
  vetted.
- **Compatibility**: every apiver layer keeps its signatures and return shapes; new optional parameters
  are last or keyword-only, never inserted or reordered; compat shims have signature tests; new public names are
  exported only from the layers that should see them; `b2sdk._internal` stays private; deprecations warn
  before removal; behavior the B2 CLI depends on (the `cli-compat` CI job) still holds. A breaking change
  belongs only in a new apiver and needs a `changed`/`removed` fragment that says what callers must do.
- **Compliance**: the change matches the issue, `spec.md` and `plan.md`; the PR description matches the
  code; a `changelog.d/` fragment exists, reads well for users and uses the right type; docstrings and
  `doc/source` cover new public API (`doc_cover` passes); `AGENTS.md` is updated when the change makes it
  stale or breaks a convention in it.

## Effort by risk

Match the depth of review to what a missed bug would cost:

- **Quick**: docs, `changelog.d/`, `intent/`, test-only and `.claude/` changes. Check claims and links.
- **Standard**: ordinary changes in `b2sdk/_internal/`. All passes on the whole diff.
- **Deep**: `b2http.py`, `session.py`, `raw_api.py`, `account_info/`, `encryption/`, `transfer/`,
  `sync/`, any `b2sdk/vN/` change, `pyproject.toml`, `uv.lock`, `.github/workflows/`. All passes, run the
  tests on every `--api`, and say what the checks would miss. These always need a maintainer.

## Skip

Do not review generated or vendored content: `uv.lock` hunks (review only the `pyproject.toml` change
that caused them), `CHANGELOG.md` release sections, `.claude/skills/` copied from the playbook, and
`doc/source` output. Do not report what CI already enforces (ruff, codespell, liccheck, license headers,
towncrier naming, `doc_cover`).

## Severity

- **Important**: breaks existing callers, sends a wrong request to B2, loses or leaks data or
  credentials, hangs, or leaves resources behind. A claimed behavior with no test pinning it is Important.
- **Nit**: style, naming, comments. At most five per review; summarize the rest as a count.
- **Pre-existing**: a real bug this PR did not introduce. Report it so it is not lost, but it does not
  block this PR; suggest an issue.

## Evidence

Every finding cites `file:line` and says what goes wrong, with the input or sequence that triggers it.
Do not infer behavior from a function's name; read it. If you could not verify something (no live B2, no
simulator), say so.

## Converging

- Round 1 reviews the whole diff. From round 3 on, review only the commits since your last review.
- Name the class of bug, not one instance, so the author fixes every occurrence in one round.
- Never repeat a finding already posted on the PR, resolved or not.
- After three rounds with Important findings still open, stop and ask the author to reconsider the
  approach instead of posting more line comments.

## Answering findings

Every finding gets an answer before merge: fix it, or resolve the thread with a one-line reason
(false positive, intentional, out of scope with an issue link). Do not resolve a thread silently. Start
the reply with one of `Fixed`, `Wrong`, `Intentional` or `Won't fix`, so reviewer noise can be measured
(see `intent/review-process/intent.md`).

When the same finding appears on a second PR, add it to `docs/review-lessons.md` and, if it is a rule,
to `AGENTS.md`, or turn it into a lint rule or test.

## Format

Start with a count (`2 Important, 1 Nit, 0 Pre-existing`, or `No issues found`), then what you checked
and against what (issue number, live B2 or simulator, commands run). Then a table sorted by severity:
`Severity | File:Line | Pass: finding`. When there is nothing to fix, say so plainly.
