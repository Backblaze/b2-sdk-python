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

PR for #634 covers the parts that need no settings change: AGENTS.md, REVIEW.md severities, evidence and
convergence rules, Risk and How-to-revert in the PR template, and a fresh-context self-review before a
PR opens.

## Proposed outcome

Human review time goes where the risk is, with no rise in reverts or post-release fixes:

1. **Risk tiers set by the change, not the author.** A first proposal to accept or change:

   | Tier | Paths | Human approvals |
   | --- | --- | --- |
   | Low | `*.md`, `doc/`, `changelog.d/`, `intent/`, `test/` only, `.sdkharness/` docs | One maintainer, light review |
   | Medium | `b2sdk/_internal/` (except below), `.sdkharness/tests/` | One maintainer |
   | High | `b2http.py`, `session.py`, `raw_api.py`, `account_info/`, `encryption/`, any `b2sdk/vN/` export, `.github/workflows/`, `pyproject.toml`, `uv.lock`, release commits | Two maintainers, or one plus a green `cli-compat` run reviewed by hand |

   Authors outside the maintainer list always need a person. A bot's approval never fills a human slot.
2. **Approvals cover one commit.** New commits pushed after an approval need re-approval for Medium and
   High tiers.
3. **Every review thread resolved before merge** (GitHub "require conversation resolution").
4. **Automated AI review and security review on every PR**, following REVIEW.md, with open Important
   findings blocking merge after a trial period.
5. **Release commits through a PR** instead of a direct push to `master`, so CI and a second person see
   them before tagging.
6. **A green baseline**: fix or quarantine the known live-B2 failures (#614) and make `nox -s lint` check
   formatting, so a red check means something.
7. **Past bugs become reviewer evals.** Build a set from bugs reviewers caught or that shipped, for
   example #548 (retry change that would break every POST), #550 (TypeError on a date `Retry-After`),
   #541 and #542 (v2 relying on v3 abstractions), #543 (simulator bucket-name mapping), #620 (harness
   check that could reach production). Score the reviewer against it whenever REVIEW.md, AGENTS.md or the
   model changes.
8. **Measure it.** Share of PRs merged with a substantive review, review rounds to pass, time a PR waits
   on a person; reverts and post-release fix PRs as the guardrail.

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
