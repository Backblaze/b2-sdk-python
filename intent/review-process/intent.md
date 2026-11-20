# Intent: review and merge process scaled by risk
Author: Gonzalo Peña-Castellanos (with Claude). Issue: #634. Status: draft, for the b2-sdk-python maintainers.
Source: [How the Claude Code team does code review](https://claude.dev/blog/solving-code-review-at-anthropic/)
(claude.dev, 2026-10-15), applied to b2-sdk-python.

## Problem

More b2-sdk-python PRs are written with agents, and review has not scaled with them. Across the last 80
PRs (#514 to #633):

- 27 of 61 merged PRs got no review or comment from anyone but the author, including the whole
  sdkharness series (#611 to #628). Most approvals carry no text, so they do not show what was checked.
- The same findings recur (see `docs/review-lessons.md`): missing changelog fragments, tests that do not
  fail when the fix is reverted, retry logic in the wrong layer, missing apiver exports, simulator drift.
- Work is duplicated or split: #608/#615/#618 and #609/#616/#617/#619 fixed the same problems, and
  #615 and #617 merged unreviewed into `development` before being redone against `master`.
- The live-B2 CI stage has known failures (#614), so every PR argues its red checks are unrelated, and
  `nox -s lint` runs `ruff format` in write mode rather than `--check`, so lint never fails on formatting.
- Every PR gets the same attention whatever its risk; a docs fix and a change to `b2http.py` retries are
  treated alike.
- Nothing measures whether a reviewer change (REVIEW.md, AGENTS.md, model) catches more or fewer bugs.

PR #635 covers the parts that need no settings change: AGENTS.md and docs/review-lessons.md built from
the review history of all 370 merged PRs, REVIEW.md (failure modes, passes, effort by risk, skip list,
severities, evidence, convergence, reply codes), Risk and How-to-revert in the PR template, a
fresh-context self-review before a PR opens, a ruff hook for agents, a `claude-review.yml` workflow (AI
review and security review, inert until an `ANTHROPIC_API_KEY` secret exists), a seed eval set
([evals.md](evals.md)) and `contrib/review_metrics.py` for the metrics below.

## Proposed outcome

Human review time goes where the risk is, with no rise in reverts or post-release fixes:

1. **Risk tiers set by the change, not the author.** A first proposal to accept or change:

   | Tier | Paths | Human approvals |
   | --- | --- | --- |
   | Low | `*.md`, `doc/`, `changelog.d/`, `intent/`, `test/` only, `.sdkharness/` docs | None once CI, AI review and security review are green, for trusted authors; otherwise one |
   | Medium | `b2sdk/_internal/` (except below), `.sdkharness/tests/` | One maintainer |
   | High | `b2http.py`, `session.py`, `raw_api.py`, `account_info/`, `encryption/`, any `b2sdk/vN/` export, `.github/workflows/`, `pyproject.toml`, `uv.lock`, release commits | Two maintainers, or one plus a green `cli-compat` run reviewed by hand |

   Authors outside the maintainer list always need a person. A bot's approval never fills a human slot.
2. **Approvals cover one commit.** New commits pushed after an approval need re-approval for Medium and
   High tiers.
3. **Every review thread resolved before merge** (GitHub "require conversation resolution").
4. **Automated AI review and security review on every PR**, following REVIEW.md, with open Important
   findings blocking merge after a trial period. `claude-review.yml` is in place; it needs the
   `ANTHROPIC_API_KEY` secret, and then "require conversation resolution" makes open findings block.
   Optionally a nightly review run over existing code to find old bugs.
5. **Release commits through a PR** instead of a direct push to `master`, so CI and a second person see
   them before tagging. Risky releases could go out first as a pre-release tried by the B2 CLI. Today `cd.yml`
   treats a tag as a pre-release only when it ends in a non-digit, so `vX.Y.Zrc1` would be uploaded to
   PyPI as a normal release; fix that check before relying on it. Rollback is a new patch release, never a moved tag.
6. **Merge queue**, so checks rerun on `master` plus the change and two PRs that pass alone cannot
   combine into a broken `master`.
7. **A green baseline**: fix or quarantine the known live-B2 failures (#614) and make `nox -s lint` check
   formatting, so a red check means something.
8. **Turn repeated review comments into checks** (see docs/review-lessons.md, "Candidates for
   automation"): a per-apiver signature snapshot test, a raw API parity test, and more ruff rules.
9. **CODEOWNERS** so the Deep paths in REVIEW.md always need a maintainer, and branch protection that
   dismisses stale approvals on new commits.
10. **Past bugs become reviewer evals.** Build a set from bugs reviewers caught or that shipped, for
   example #548 (retry change that would break every POST), #550 (TypeError on a date `Retry-After`),
   #541 and #542 (v2 relying on v3 abstractions), #543 (simulator bucket-name mapping), #620 (harness
   check that could reach production). Score the reviewer against it whenever REVIEW.md, AGENTS.md or the
   model changes.
   The seed set is [evals.md](evals.md).
11. **Measure it.** `python contrib/review_metrics.py --days 30` reports human approvals per PR (median,
    p95), human comments, review rounds, hours to first human review, PRs merged with no human review, and
    reverts as the guardrail. Over the last 120 days: 16 PRs merged, 14 (88%) with no human review, 0
    reverts. Reviewer noise is measured from the reply codes in REVIEW.md (`Fixed`, `Wrong`,
    `Intentional`, `Won't fix`).

## Affected users and systems

- Contributors and agents opening PRs; the maintainers who review them.
- Repo settings: branch protection or rulesets on `master`, conversation resolution, required checks.
- Workflows: `ci.yml` (lint formatting check), a new review workflow, possibly a risk-tier check.
- The B2 CLI, which depends on this SDK's behavior (`cli-compat`).
- Library users only indirectly: fewer regressions and faster fixes.

## Constraints

- Settings changes need a repo admin; this intent proposes them, it does not apply them.
- Separation of duties: the agent that wrote a change never approves it, and the person directing it
  counts as one reviewer.
- Workflow changes keep the Dependabot guard (`github.actor` and `pull_request.user.login`).
- Start strict and loosen with evidence; any tier can be tightened if reverts rise.
- Cost of running an AI reviewer per push in CI.

## Open questions

- Who are the maintainers for approval purposes, and should a `CODEOWNERS` file name them per path?
- Do the tier paths above match where the risk is?
- Should the changelog validation step become a required check on `master`?
- Who owns fixing the #614 baseline, and should the `development` branch be retired?
- Where should the reviewer eval set live and run: this repo, or the central SDK harness?
- Who adds the `ANTHROPIC_API_KEY` secret for `claude-review.yml`, and what per-PR cost is acceptable?
- Who confirms the candidate cases in `evals.md`?
