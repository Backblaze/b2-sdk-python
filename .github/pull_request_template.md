Summary:

## Motivation

Why this change is needed. Link the issue (`Fixes #123`) and, if one exists, `intent/<slug>/`.

## Changes

What changed, and which apiver layers (`b2sdk.v0`..`b2sdk.v3`) see it. If this PR is stacked on another, name it and the merge order.

## Testing

Paste the tail of the local CI commands from `AGENTS.md`. For each behavior this PR adds or fixes, name the test that fails when the change is reverted. List any live-B2 integration tests or `.sdkharness` checks you ran, and any you could not.

## Risk

What could break for existing callers of each apiver layer, and whether the request sent to B2 changes. Name the riskiest part of the diff.

## How to revert

Whether reverting this commit is enough, or whether callers (including the B2 CLI) would need to pin the previous release.

## Checklist

- [ ] `nox -s lint`, `nox -s unit`, `nox -s build` and `nox -s towncrier_check` pass locally.
- [ ] A fresh-context review (`/code-review`, `/security-review` or a subagent) ran on the diff; findings are fixed or answered below.
- [ ] A `changelog.d/` fragment exists, or the PR is labelled `-changelog`.
- [ ] New or changed behavior is pinned by a test that fails when the change is reverted, on every apiver it affects.
- [ ] Docstrings, `doc/source`, README and `AGENTS.md` are updated if needed.
- [ ] The description matches the final code.
