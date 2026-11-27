---
name: verify
description: Run the local verification gate before you push, open a PR, or say work is done.
---

# Verify your work

Run these stages from the repository root, in this order. Read the first failing stage and its output.
Fix the cause and rerun the stage. Paste the tail of each stage into the PR.
If a test fails, fix the code, not the test.

## Stages

1. `nox -s lint`: ruff reported an issue, a `test/static` test failed (for example the license header or the
   `llms.txt` link test), or liccheck rejected a dependency license.
2. `nox -s towncrier_check`: a `changelog.d/` fragment has a wrong name or an unknown type.
3. `nox -s unit-3.14`: runs every apiver. One failing apiver means a layer broke. To run one apiver:
   `uv run --group test pytest --api=v2 test/unit`.
4. `nox -s build`: the distribution did not build.
5. `nox --non-interactive -s doc`: Sphinx runs with `-W`, so a warning is a failure. It also runs `doc_cover`,
   which fails on undocumented public API.
6. `uv lock --check`: `uv.lock` does not match `pyproject.toml`.

Without `--non-interactive`, `nox -s doc` starts a live-reload server in a terminal. Always pass the flag.

## Separate checks

Integration tests (`nox -s integration-3.14`) and the `.sdkharness` checks are not part of this gate.
They need live B2 credentials or a simulator. See [docs/testing.md](../../../docs/testing.md).
Report any stage you did not run as NOT RUN, never as passed.
