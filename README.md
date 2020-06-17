# B2 python SDK
&nbsp;[![Travis CI](https://img.shields.io/travis/Backblaze/b2-sdk-python/master.svg?label=Travis%20CI)](https://travis-ci.org/Backblaze/b2-sdk-python)&nbsp;[![License](https://img.shields.io/pypi/l/b2sdk.svg?label=License)](https://pypi.python.org/pypi/b2)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/b2sdk.svg?label=python%20versions)](https://pypi.python.org/pypi/b2sdk)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/b2sdk.svg?label=PyPI%20version)](https://pypi.python.org/pypi/b2sdk)&nbsp;[![Docs](https://readthedocs.org/projects/b2-sdk-python/badge/?version=master)](https://b2-sdk-python.readthedocs.io/en/master/)

This repository contains a client library and a few handy utilities for easy access to all of the capabilities of B2 Cloud Storage.

[B2 command-line tool](https://github.com/Backblaze/B2_Command_Line_Tool) is an example of how it can be used to provide command-line access to the B2 service, but there are many possible applications (including FUSE filesystems, storage backend drivers for backup applications etc).

# Documentation

The latest documentation is available on [Read the Docs](https://b2-sdk-python.readthedocs.io).

# Installation

The sdk can be installed with:

    pip install b2sdk

If you see a message saying that the `six` library cannot be installed, which
happens if you're installing with the system python on OS X El Capitan, try
this:

    pip install --ignore-installed b2sdk

# Version policy

b2sdk follows [Semantic Versioning](https://semver.org/) policy, so in essence the version number is MAJOR.MINOR.PATCH (for example 1.2.3) and:
- we increase MAJOR version when we make incompatible API changes
- we increase MINOR version when we add functionality in a backwards-compatible manner, and
- we increase PATCH version when we make backwards-compatible bug fixes (unless someone relies on the undocumented behavior of a fixed bug)

Therefore when setting up b2sdk as a dependency, please make sure to match the version appropriately, for example you could put this in your `requirements.txt` to make sure your code is compatible with the `b2sdk` version your user will get from pypi:

```
b2sdk>=0.0.0,<1.0.0
```


# Release History

## 1.1.0 (not released yet)

* Make sync treat hidden files as deleted
* Remove arrow warnings caused by https://github.com/crsmithdev/arrow/issues/612
* Add `list_file_versions` method to buckets.
* Add server-side copy support for large files
* Add ability to synthesize objects from local and remote sources
* Add AuthInfoCache, InMemoryCache and AbstractCache to public interface
* Ignore urllib3 "connection pool is full" warning
* Add ability to filter in ScanPoliciesManager based on modification time
* Add ScanPoliciesManager and SyncReport to public interface
* Add md5 checksum to FileVersionInfo
* Add more keys to dicts returned by as_dict() methods
* Fix handling of modification time of files

## 1.0.2 (2019-10-15)

Changes:

* Remove upper version limit for arrow dependency

## 1.0.0 (2019-10-03)

Changes:

* Minor bug fix.

## 1.0.0-rc1 (2019-07-09)

Changes:

* Deprecate some transitional method names to v0 in preparation for v1.0.0.

## 0.1.10 (2019-07-09)

Changes:

* Remove a parameter (which did nothing, really) from `b2sdk.v1.Bucket.copy_file` signature


## 0.1.8 (2019-06-28)

Changes:

* Add support for b2_copy_file
* Add support for `prefix` parameter on ls-like calls


## 0.1.6 (2019-04-24)

Changes:

* Fix transferer crashing on empty file download attempt
* Rename account ID for authentication to application key ID.
Account ID is still backwards compatible, only the terminology
has changed.


## 0.1.4 (2019-04-04)

Initial official release of SDK as a separate package (until now it was a part of B2 CLI)


# Developer Info

We encourage outside contributors to perform changes on our codebase. Many such changes have been merged already. In order to make it easier to contribute, core developers of this project:

* provide guidance (through the issue reporting system)
* provide tool assisted code review (through the Pull Request system)
* maintain a set of integration tests (run with a production cloud)
* maintain a set of (well over a hundred) unit tests
* automatically run unit tests on 13 versions of python (including osx and pypy)
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
that contains two lines with nothing on them but your application key ID and application key:

     applicationKeyId
     applicationKey

We marked the places in the code which are significantly less intuitive than others in a special way. To find them occurrences, use `git grep '*magic*'`.
