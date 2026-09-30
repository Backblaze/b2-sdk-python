# Contributing to B2 Python SDK

We encourage outside contributors to perform changes on our codebase. Many such changes have been merged
already.

**The full contributors guide is published at
[b2-sdk-python.readthedocs.io/en/master/contributing.html](https://b2-sdk-python.readthedocs.io/en/master/contributing.html)**
(source: [`doc/source/contributing.rst`](doc/source/contributing.rst)). It is the canonical reference for
environment setup, the available `nox` sessions, dependency management, testing, and building the
documentation. This file is a short entry point only — please keep detailed guidance in the published
guide rather than duplicating it here.

## Getting started

You'll need [nox](https://github.com/theacodes/nox) and [uv](https://docs.astral.sh/uv/):

    pip install nox uv

Then, to run the linters and the test suite:

    nox -s lint
    nox -s test

Integration tests need real B2 credentials; see the published guide for details.

## Before you open a pull request

* **Add a changelog entry.** Every PR needs at least one news fragment in `changelog.d`, or CI will
  fail. Name it `{issue_number}.{type}.md` (e.g. `157.fixed.md`) when the PR closes an issue, or
  `+{unique_string}.{type}.md` (e.g. `+foobar.fixed.md`) otherwise. Valid types are `fixed`, `changed`,
  `added`, `deprecated`, `removed`, `infrastructure` and `doc`. The description must stand on its own —
  a change like `fixed #157` will not be accepted. [towncrier](https://towncrier.readthedocs.io/)
  compiles these into [CHANGELOG.md](CHANGELOG.md).
* **Don't bump the version.** Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
  and are established by reading git tags, so no code or manifest changes are required in a PR.
* **Update `uv.lock` if you touched dependencies.** Use `uv add` or `uv lock`, and verify with
  `uv lock --check`.

Releases are cut by maintainers; see [README.release.md](README.release.md).
