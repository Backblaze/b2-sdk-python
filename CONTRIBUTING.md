# Contributing to B2 Python SDK

We encourage outside contributors to perform changes on our codebase. Many such changes have been merged already.
In order to make it easier to contribute, core developers of this project:

* provide guidance (through the issue reporting system)
* provide tool assisted code review (through the Pull Request system)
* maintain a set of unit tests
* maintain a set of integration tests (run with a production cloud)
* maintain development automation tools using [nox](https://github.com/theacodes/nox) that can easily:
   * format the code using [ruff](https://github.com/astral-sh/ruff)
   * runs linters to find subtle/potential issues with maintainability
   * run the test suite on multiple Python versions using [pytest](https://github.com/pytest-dev/pytest)
* maintain Continuous Integration (by using GitHub Actions) that:
   * runs all sorts of linters
   * checks if the Python distribution can be built
   * runs all tests on a matrix of 8 versions of Python (including pypy) and 3 operating systems
     (Linux, Mac OS X and Windows)
   * checks if the documentation can be built properly
* maintain other Continuous Integration tools (coverage tracker)

## Before you start

Read [AGENTS.md](AGENTS.md): it maps the codebase, lists the local CI commands, and collects the problems
reviewers keep finding. Larger changes start from an intent in [`intent/`](intent/README.md) (problem →
spec → plan, each approved by a maintainer). Bug fixes with a linked issue can start from a failing test.

Reviews follow [REVIEW.md](REVIEW.md). Releases follow [RELEASE.md](RELEASE.md). Report vulnerabilities
privately as described in [SECURITY.md](SECURITY.md). Everyone taking part follows the
[Code of Conduct](CODE_OF_CONDUCT.md).

## Coding agents

PRs written with coding agents are welcome on the same terms as any other PR. The person directing the
agent owns the PR: they read the diff, answer review findings, and fix what it breaks. Before opening the
PR, have a fresh context (`/code-review`, `/security-review` or a subagent that did not write the code)
review the diff, and say in the PR what it found. Agents never approve or merge PRs.

If you doubt a change should be made at all, say so early on the issue or a draft PR, not as a blocking
comment on a finished one. Whoever lands a change owns fixing it if it breaks something; keep changes
small so they are easy to review and revert. `python contrib/review_metrics.py` reports how much human
review merged PRs got.

## Versioning

This package's versions adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and the versions are
established by reading git tags, i.e. no code or manifest file changes are required when working on PRs.

## Changelog

Each PR needs to have at least one changelog (aka news) item added. This is done by creating files in `changelog.d`.
`towncrier` is used for compiling these files into [CHANGELOG.md](CHANGELOG.md). There are several types of changes 
(news):

1. fixed
2. changed
3. added
4. deprecated
5. removed
6. infrastructure
7. doc


The `changelog.d` file name convention is:

1. If the PR closes a github issue: `{issue_number}.{type}.md` e.g. `157.fixed.md`. Note that the
   change description still has to be complete, linking an issue is just there for convenience, a change like 
   `fixed #157` will not be accepted.
2. If the PR is not related to a github issue: `+{unique_string}.{type}.md` e.g. `+foobar.fixed.md`. 

These files can either be created manually, or using `towncrier` e.g.

    towncrier create -c 'write your description here' 157.fixed.md

`towncrier create` also takes care of duplicates automatically (if there is more than 1 news fragment of one type 
for a given github issue).

Each entry is one or more full sentences, starting with a capital letter and ending with a full stop, written
for SDK users. `nox -s towncrier_check` validates names and content locally. PRs that need no entry (for example
a lockfile-only change) are labelled `-changelog` by a maintainer.

## Developer Info

You'll need to have [nox](https://github.com/theacodes/nox) and [uv](https://docs.astral.sh/uv/) installed:

* `pip install nox uv`

With `nox`, you can run different sessions (default are `lint` and `test`):

* `format` -> Format the code.
* `lint` -> Run linters.
* `test` (`test-3.10`, `test-3.11`, `test-3.12`, `test-3.13`, `test-3.14`, `test-pypy3.10`) -> Run test suite.
* `cover` -> Perform coverage analysis.
* `build` -> Build the distribution.
* `doc` -> Build the documentation.
* `doc_cover` -> Perform coverage analysis for the documentation.

For example:

    $ nox -s format
    nox > Running session format
    nox > Creating virtual environment (virtualenv) using python3.14 in .nox/format
    ...

    $ nox -s format
    nox > Running session format
    nox > Re-using existing virtual environment at .nox/format.
    ...

    $ nox --no-venv -s format
    nox > Running session format
    ...

Sessions `test` ,`unit`, and `integration` can run on many Python versions, 3.10-3.14 (+ pypy3.10) by default.

Sessions other than `test` use the last CPython version from `NOX_PYTHONS`, 3.14 by default.

You can change it:

    export NOX_PYTHONS=3.12,3.14

With the above setting, session `test` will run on Python 3.12 and 3.14, and all other sessions on Python 3.14.

Given Python interpreters should be installed in the operating system or via [pyenv](https://github.com/pyenv/pyenv).

## Managing dependencies

We use [uv](https://docs.astral.sh/uv/) for managing dependencies and developing locally.
If you want to change any of the project requirements (or requirement bounds) in `pyproject.toml`,
make sure that `uv.lock` file reflects those changes by using `uv add`, `uv lock` or other
commands - see [documentation](https://docs.astral.sh/uv/). You can verify that the lock file
is up to date by running `uv lock --check`.

## Local CI

Run these before every push; they mirror the `lint`, `build` and `test` jobs of
[ci.yml](.github/workflows/ci.yml) on one Python version:

    nox -s lint
    nox -s towncrier_check
    nox -s unit-3.14
    nox -s build
    nox --non-interactive -s doc

`doc` needs `graphviz` and `plantuml` installed. CI also runs the unit tests on every supported Python and
OS, the integration tests with B2 credentials, and the B2 CLI test suite against this checkout
(`cli-compat`).

## Linting

To run all available linters:

    nox -s lint

## Testing

To run all tests on every available Python version:

    nox -s test

To run all tests on a specific version:

    nox -s test-3.10

To run just unit tests:

    nox -s unit-3.10

To run just integration tests:

    export B2_TEST_APPLICATION_KEY=your_app_key
    export B2_TEST_APPLICATION_KEY_ID=your_app_key_id
    nox -s integration-3.10

To run tests by keyword expressions:
    
    nox -s unit-3.10 -- -k keyword

## Documentation

To build the documentation and watch for changes (including the source code):

    nox -s doc

To just build the documentation:

    nox --non-interactive -s doc
