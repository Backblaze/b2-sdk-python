# Contributing to B2 Python SDK

We encourage outside contributors to perform changes on our codebase. Many such changes have been merged already. In order to make it easier to contribute, core developers of this project:

* provide guidance (through the issue reporting system)
* provide tool assisted code review (through the Pull Request system)
* maintain a set of unit tests
* maintain a set of integration tests (run with a production cloud)
* maintain development automation tools using [nox](https://github.com/theacodes/nox) that can easily:
   * format the code using [yapf](https://github.com/google/yapf)
   * runs linters to find subtle/potential issues with maintainability
   * run the test suite on multiple Python versions using [pytest](https://github.com/pytest-dev/pytest)
* maintain Continuous Integration (by using GitHub Actions) that:
   * runs all sorts of linters
   * checks if the Python distribution can be built
   * runs all tests on a matrix of 6 versions of Python (including pypy) and 3 operating systems (Linux, Mac OS X and Windows)
   * checks if the documentation can be built properly
* maintain other Continuous Integration tools (coverage tracker)

See also the "Pull-Request Workflow" section below.

## Project Overview

* The project uses [nox](https://github.com/theacodes/nox) as a command runner so that verifications can be run across multiple python versions
* The project is built on pull requests via [github actions](./.github/workflows/ci.yml)
* Tests can be found in the [`test`](./b2sdk/test) directory and are split into unit/integration/static
    * `unit` is divided into tests for core areas, and tests for the various sdk versions. `v_all` is the recommended place to add API unit tests, as each method can be decorated with its applicable versions
    * `integration` will require a Backblaze account, but will not use capabilities beyond the free "Class A" transactions
    * `static` is licensing etc.
* The SDK promises a clear version policy (see [main README](./README.md)). SOME NOTE ABOUT VERSIONING STRATEGY AND HOW v0, v1, v2 SHOULD BE DONE ON THE TECHNICAL SIDE - ITS NOT QUITE CLEAR TO ME
* Changes for each version are found in the [CHANGELOG](./CHANGELOG.md). **It is expected this is updated with each PR introducing functional changes**. CI will enforce this.
* The project is released via [github worklows](./.github/workflows/cd.yml). See the [readme](./README.release.md)

## System Setup

1. You must have at least one (preferably more) current available version of python on the system. [`pyenv`](https://github.com/pyenv/pyenv#basic-github-checkout) is recommended as a version manager.  
<PERSONALLY I WOULD PROVIDE A RECOMMENDED COMMAND HERE TO INSTALL ALL PYTHON VERSIONS e.g. `for v in $(cat .python-version); do pyenv install $v; done` or whatever>
1. You must have `nox` installed in your environment to run test commands

## Developer Info

With `nox`, you can run different sessions (default are `lint` and `test`):

* `format` -> Format the code.
* `lint` -> Run linters.
* `test` (`test-3.5`, `test-3.6`, `test-3.7`, `test-3.8`, `test-3.9`) -> Run test suite.
* `cover` -> Perform coverage analysis.
* `build` -> Build the distribution.
* `deploy` -> Deploy the distribution to the PyPi.
* `doc` -> Build the documentation.
* `doc_cover` -> Perform coverage analysis for the documentation.

For example:

    $ nox -s format
    nox > Running session format
    nox > Creating virtual environment (virtualenv) using python3.9 in .nox/format
    ...

    $ nox -s format
    nox > Running session format
    nox > Re-using existing virtual environment at .nox/format.
    ...

    $ nox --no-venv -s format
    nox > Running session format
    ...

Sessions `test` ,`unit`, and `integration` can run on many Python versions, 3.5-3.9 by default.

Sessions other than `test` use the last given Python version, 3.9 by default.

You can change it:

    export NOX_PYTHONS=3.6,3.7

With the above setting, session `test` will run on Python 3.6 and 3.7, and all other sessions on Python 3.7.

Given Python interpreters should be installed in the operating system or via [pyenv](https://github.com/pyenv/pyenv).

## Linting

To run all available linters:

    nox -s lint

## Testing

To run all tests on every available Python version:

    nox -s test

To run all tests on a specific version:

    nox -s test-3.9

To run just unit tests:

    nox -s unit-3.9

To run just integration tests:

    export B2_TEST_APPLICATION_KEY=your_app_key
    export B2_TEST_APPLICATION_KEY_ID=your_app_key_id
    nox -s integration-3.9

## Documentation

To build the documentation and watch for changes (including the source code):

    nox -s doc

To just build the documentation:

    nox --non-interactive -s doc

## Pull-Request Workflow

Each knows best how to work for themselves. But we recommend the following:

1. Fetch the code. Identify the areas which will be changed. Identify how your changes fall into the SDK versioning strategy as explained above.
1. Identify which tests are applicable to your changes and where they fit within the test strategy.
1. Write your code + tests
1. Run the test/linting workflows on at least two major python versions. If your changes are significant, run the integration tests.
1. Submit your PR and ensure the build is passing.  
Feel free to post an earlier version of your work with a failing build to ask questions so long as you identify it as a work-in-progress.