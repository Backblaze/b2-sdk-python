# B2 Command Line Tool&nbsp;[![Travis CI](https://img.shields.io/travis/Backblaze/B2_Command_Line_Tool/master.svg?label=Travis%20CI)](https://travis-ci.org/Backblaze/B2_Command_Line_Tool)&nbsp;[![License](https://img.shields.io/pypi/l/b2.svg?label=License)](https://pypi.python.org/pypi/b2)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/b2.svg?label=python%20versions)](https://pypi.python.org/pypi/b2)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/b2.svg?label=PyPI%20version)](https://pypi.python.org/pypi/b2)

The command-line tool that gives easy access to all of the capabilities of B2 Cloud Storage.

This program provides command-line access to the B2 service.

Version 1.3.9

# Installation

This tool can be installed with:

    pip install b2-sdk

If you see a message saying that the `six` library cannot be installed, which
happens if you're installing with the system python on OS X El Capitan, try
this:

    pip install --ignore-installed b2-sdk

# Release History

## 1.3.8 (December 6, 2018)

New features:

* New `--excludeAllSymlinks` option for `sync`.
* Faster downloading of large files using multiple threads and bigger buffers. 

Bug fixes:

* Fixed doc for cancel-all-unfinished-large-files

## 1.3.6 (August 21, 2018)

Bug fixes:

* Fix auto-reauthorize for application keys.
* Fix problem with bash auto-completion module.
* Fix (hopefully) markdown display in PyPI.

## 1.3.4 (August 10, 2018)

Bug fixes:

* Better documentation for authorize-account command.
* Fix error reporting when using application keys
* Fix auth issues with bucket-restricted application keys. 

## 1.3.2 (July 28, 2018)

Bug fixes:

* Tests fixed for Python 3.7
* Add documentation about what capabilites are required for different commands.
* Better error messages for authorization problems with application keys.

## 1.3.0 (July 20, 2018)

New features:

* Support for [application keys](https://www.backblaze.com/b2/docs/application_keys.html).
* Support for Python 3.6
* Drop support for Python 3.3 (`setuptools` no longer supports 3.3)

Bug fixes:

* Fix content type so markdown displays properly in PyPI
* The testing package is called `test`, not `tests`

Internal upgrades:

* Faster and more complete integration tests

## 1.2.0 (July 6, 2018)

New features:

* New `--recursive` option for ls
* New `--showSize` option for get-bucket
* New `--excludeDirRegex` option for sync

And some bug fixes:

* Include LICENSE file in the source tarball. Fixes #433
* Test suite now runs as root (fixes #427)
* Validate file names before trying to upload
* Fix scaling problems when syncing large numbers of files
* Prefix Windows paths during sync to handle long paths (fixes #265)
* Check if file to be synced is still accessible before syncing (fixes #397)

## 1.1.0 (November 30, 2017)

Just one change in this release:

* Add support for CORS rules in `create-bucket` and `update-bucket`.  `get-bucket` will display CORS rules.

## 1.0.0 (November 9, 2017)

This is the same code as 0.7.4, with one incompatible change:

* Require `--allowEmptySource` to sync from empty directory, to help avoid accidental deletion of all files.

## 0.7.4 (November 9, 2017)

New features:

* More efficient uploads by sending SHA1 checksum at the end.

Includes a number of bug fixes:

* File modification times are set correctly when downloading.
* Fix an off-by-one issue when downloading a range of a file (affects library, but not CLI).
* Better handling of some errors from the B2 service.

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
