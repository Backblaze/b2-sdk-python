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

1. Read the linked issue and, if present, `intent/<slug>/spec.md` and `plan.md`.
2. Check out the PR on current `master` and run the local CI commands from `AGENTS.md`. Say what you ran.
3. For each behavior the PR claims to fix or add, revert that line and confirm a test fails. Name any
   behavior no test pins.

## Passes

Tag each finding with its pass.

- **Bugs**: logic errors, off-by-one in ranges and part sizes, error paths that leak resources or
  unfinished large files, threads or executors that are never shut down, shared state touched without the
  lock that guards it.
- **Wire**: the request matches the B2 docs for the API version used; omitted vs `None` vs `[]` is
  deliberate; `raw_simulator.py` models the same behavior as `raw_api.py`; retry, reauth and `Retry-After`
  behave like sibling calls in `b2http.py`/`session.py`.
- **Security**: secrets (application keys, auth tokens, SSE-C keys) never logged, put in `repr` or in an
  exception message; capability checks fail closed; `SqliteAccountInfo` files are not world-readable.
- **Compatibility**: every apiver layer keeps its signatures and return shapes; new public names are
  exported only from the layers that should see them; `b2sdk._internal` stays private; deprecations warn
  before removal; behavior the B2 CLI depends on (the `cli-compat` CI job) still holds. A breaking change
  belongs only in a new apiver and needs a `changed`/`removed` fragment that says what callers must do.
- **Compliance**: the change matches the issue, `spec.md` and `plan.md`; the PR description matches the
  code; a `changelog.d/` fragment exists, reads well for users and uses the right type; docstrings and
  `doc/source` cover new public API (`doc_cover` passes); `AGENTS.md` is updated when the change makes it
  stale or breaks a convention in it.

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
(false positive, intentional, out of scope with an issue link). Do not resolve a thread silently.

## Format

Start with a count (`2 Important, 1 Nit, 0 Pre-existing`, or `No issues found`), then what you checked
and against what (issue number, live B2 or simulator, commands run). Then a table sorted by severity:
`Severity | File:Line | Pass: finding`. Do not report generated files or what CI already enforces
(ruff, codespell, liccheck, towncrier naming).
