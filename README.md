# B2 Command Line Tool&nbsp;[![Travis CI](https://img.shields.io/travis/Backblaze/b2-sdk-python/master.svg?label=Travis%20CI)](https://travis-ci.org/Backblaze/b2-sdk-python)&nbsp;[![License](https://img.shields.io/pypi/l/b2sdk.svg?label=License)](https://pypi.python.org/pypi/b2)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/b2sdk.svg?label=python%20versions)](https://pypi.python.org/pypi/b2sdk)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/b2sdk.svg?label=PyPI%20version)](https://pypi.python.org/pypi/b2sdk)

The command-line tool that gives easy access to all of the capabilities of B2 Cloud Storage.

This program provides command-line access to the B2 service.

Version 1.0.0

# Installation

This tool can be installed with:

    pip install b2sdk

If you see a message saying that the `six` library cannot be installed, which
happens if you're installing with the system python on OS X El Capitan, try
this:

    pip install --ignore-installed b2sdk

# Release History

## 1.0.0 (February ????, 2019)

Initial official release of SDK as a separate package (until now it was a part of B2 CLI)


# Developer Info

We encourage outside contributors to perform changes on our codebase. Many such changes have been merged already. In order to make it easier to contribute, core developers of this project:

* provide guidance (through the issue reporting system)
* provide tool assisted code review (through the Pull Request system)
* maintain a set of integration tests (run with a production cloud)
* maintain a set of (well over a hundred) unit tests
* automatically run unit tests on 14 versions of python (including osx, Jython and pypy)
* format the code automatically using [yapf](https://github.com/google/yapf)
* use static code analysis to find subtle/potential issues with maintainability
* maintain other Continous Integration tools (coverage tracker)

You'll need to some Python packages installed.  To get all the latest things:

* `pip install --upgrade --upgrade-strategy eager -r requirements.txt -r requirements-test.txt -r requirements-setup.txt`

There is a `Makefile` with a rule to run the unit tests using the currently active Python:

    make setup
    make test

will install the required packages, then run the unit tests.

To test in multiple python virtual environments, set the enviroment variable `PYTHON_VIRTUAL_ENVS`
to be a space-separated list of their root directories.  When set, the makefile will run the
unit tests in each of the environments.

Before checking in, use the `pre-commit.sh` script to check code formatting, run
unit tests, run integration tests etc.

The integration tests need a file in your home directory called `.b2_auth`
that contains two lines with nothing on them but your account ID and application key:

     accountId
     applicationKey

We marked the places in the code which are significantly less intuitive than others in a special way. To find them occurrences, use `git grep '*magic*'`.
