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

## Versioning

This package's versions adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and the versions are
established by reading git tags, i.e. no code or manifest file changes are required when working on PRs.

## Changelog

Add a [Changie](https://changie.dev) fragment to `.changes/unreleased/` for each
user-visible, documentation or infrastructure change. Go is required to run the
pinned tool, following the same setup as Backblaze/blazer:

```sh
go run github.com/miniscruff/changie@v1.24.0 new
uv run --group nox nox -s changelog_check
```

Choose Added, Changed, Deprecated, Removed, Fixed, Doc, Infrastructure or Security.
Write a complete sentence that explains the resulting behavior. Include a public
issue link in the body when applicable; keep private findings out of fragments.
Use `--kind Fixed --body 'Correct the documented behavior.'` for noninteractive
creation. CI checks the fragment fields and reproduces the merged changelog.
A `-changelog` label can waive adding a fragment; validation still runs. Lockfile-only
updates and release PRs that batch fragments have their own checks.

Release managers use `changie batch` and `changie merge` through the Nox release
session. See [RELEASE.md](RELEASE.md) for the complete process. The `.changes/v2.13.4.md`
archive preserves all earlier release notes; do not rewrite that history.

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
    nox > Reusing existing virtual environment at .nox/format.
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
