.. _contributors_guide:

#########################################
Contributors Guide
#########################################

We encourage outside contributors to perform changes on our codebase. Many such changes have been merged already. In order to make it easier to contribute, core developers of this project:

* provide guidance (through the issue reporting system)
* provide tool assisted code review (through the Pull Request system)
* maintain a set of unit tests
* maintain a set of integration tests (run with a production cloud)
* maintain development automation tools using `nox <https://github.com/theacodes/nox>`_ that can easily:

   * format the code using `ruff <https://github.com/astral-sh/ruff>`_
   * runs linters to find subtle/potential issues with maintainability
   * run the test suite on multiple Python versions using `pytest <https://github.com/pytest-dev/pytest>`_

* maintain Continuous Integration (by using GitHub Actions) that:

   * runs all sorts of linters
   * checks if the Python distribution can be built
   * runs all tests on a matrix of every supported Python version (CPython and PyPy, see ``PYTHON_VERSIONS`` in ``noxfile.py``) across Linux, macOS and Windows
   * checks if the documentation can be built properly

* maintain other Continuous Integration tools (coverage tracker)

Versioning
#############

This package's versions adhere to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_, and the
versions are established by reading git tags, i.e. no code or manifest file changes are required when
working on PRs.

Changelog
#############

Each PR needs to have at least one changelog (aka news) item added. This is done by creating files in
``changelog.d``. `towncrier <https://towncrier.readthedocs.io/>`_ is used for compiling these files into
``CHANGELOG.md``. There are several types of changes (news):

#. ``fixed``
#. ``changed``
#. ``added``
#. ``deprecated``
#. ``removed``
#. ``infrastructure``
#. ``doc``

The ``changelog.d`` file name convention is:

#. If the PR closes a GitHub issue: ``{issue_number}.{type}.md``, e.g. ``157.fixed.md``. Note that the
   change description still has to be complete; linking an issue is just there for convenience. A change
   like ``fixed #157`` will not be accepted.
#. If the PR is not related to a GitHub issue: ``+{unique_string}.{type}.md``, e.g. ``+foobar.fixed.md``.

These files can either be created manually, or using ``towncrier``, e.g.::

    $ towncrier create -c 'write your description here' 157.fixed.md

``towncrier create`` also takes care of duplicates automatically (if there is more than one news fragment
of one type for a given GitHub issue).

Developer info
##############

You'll need to have `nox <https://github.com/theacodes/nox>`_ and `uv <https://docs.astral.sh/uv/>`_ installed:

* ``pip install nox uv``

With ``nox``, you can run different sessions (default are ``lint`` and ``test``):

* ``format`` -> Format the code.
* ``lint`` -> Run linters.
* ``test`` (``test-3.10``, ``test-3.11``, ``test-3.12``, ``test-3.13``, ``test-3.14``, ``test-pypy3.10``) -> Run test suite.
* ``cover`` -> Perform coverage analysis.
* ``build`` -> Build the distribution.
* ``doc`` -> Build the documentation.
* ``doc_cover`` -> Perform coverage analysis for the documentation.

Releases are not published from a ``nox`` session. The *Continuous Delivery* GitHub Actions
workflow (``.github/workflows/cd.yml``) builds the distribution when a version tag is pushed,
creates a GitHub release, and uploads it to PyPI for non-prerelease versions. The release
procedure itself is described in ``README.release.md``.

For example::

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

Sessions ``test``, ``unit``, and ``integration`` can run on many Python versions, 3.10-3.14 (+ pypy3.10) by default.

Sessions other than ``test`` use the last CPython version from ``NOX_PYTHONS``, 3.14 by default.

You can change it::

    export NOX_PYTHONS=3.12,3.14

With the above setting, session ``test`` will run on Python 3.12 and 3.14, and all other sessions on Python 3.14.

Given Python interpreters should be installed in the operating system or via `pyenv <https://github.com/pyenv/pyenv>`_.

Managing dependencies
#####################

We use `uv <https://docs.astral.sh/uv/>`_ for managing dependencies and developing locally. If you want
to change any of the project requirements (or requirement bounds) in ``pyproject.toml``, make sure that
the ``uv.lock`` file reflects those changes by using ``uv add``, ``uv lock`` or other commands - see the
`uv documentation <https://docs.astral.sh/uv/>`_. You can verify that the lock file is up to date by
running::

    $ uv lock --check

Linting
#############

To run all available linters::

    $ nox -s lint


Testing
#############

To run all tests on every available Python version::

    $ nox -s test

To run all tests on a specific version::

    $ nox -s test-3.10

To run just unit tests::

    $ nox -s unit-3.10

To run just integration tests::

    $ export B2_TEST_APPLICATION_KEY=your_app_key
    $ export B2_TEST_APPLICATION_KEY_ID=your_app_key_id
    $ nox -s integration-3.10

To run tests matching a keyword expression::

    $ nox -s unit-3.10 -- -k keyword

Where to put a new test
=======================

**b2sdk** keeps several numbered interfaces (``b2sdk.v0`` through ``b2sdk.v3``) working at the same
time, and the unit suite is what holds that promise. ``nox -s unit`` therefore runs ``pytest`` once per
interface version - ``--api=v3``, then ``v2``, ``v1`` and ``v0`` - so a single shared test is executed
four times, once against each interface.

Two mechanisms make that work:

* ``pytest`` gains an ``--api`` option, defaulting to the newest version. It puts
  ``test/unit/<apiver>/apiver`` on ``sys.path``, so a shared test can write
  ``from apiver_deps import B2Api`` and receive whichever interface is under test. The ``apiver`` and
  ``apiver_int`` fixtures expose that version as ``"v2"`` or ``2`` respectively.
* Tests living under a *different* version's directory are not collected. Running with ``--api=v3``
  skips ``test/unit/v0``, ``v1`` and ``v2`` entirely.

Choose a location accordingly:

.. list-table::
   :header-rows: 1
   :widths: 28 22 50

   * - Location
     - Collected for
     - Use it for
   * - ``test/unit/<topic>/``, e.g. ``bucket``, ``api``, ``sync``
     - every interface version
     - The default home. Behaviour shared by all interfaces - in practice, anything implemented in
       ``b2sdk/_internal``. Import through ``apiver_deps``.
   * - ``test/unit/v0`` … ``test/unit/v3``
     - only its own version
     - Behaviour that exists in exactly one interface, such as a legacy constructor argument or an
       older return type preserved by a compatibility adapter.
   * - ``test/unit/v_all/``
     - every interface version
     - Assertions about the interfaces themselves - that a name is still exported, or that two
       versions agree.
   * - ``test/integration/``
     - not run by ``nox -s unit``
     - End-to-end behaviour against real B2. Requires credentials, so it cannot gate every PR.

If a shared test applies to only some versions, keep it in the shared directory and mark it rather than
copying it into each version directory::

    @pytest.mark.apiver(1)          # only v1
    @pytest.mark.apiver(1, 3)       # only v1 and v3
    @pytest.mark.apiver(from_ver=2) # v2 and newer
    @pytest.mark.apiver(to_ver=2)   # v2 and older

``from_ver`` and ``to_ver`` are inclusive, and a single mark may not mix positional and keyword
arguments. Marks also work on individual ``pytest.param`` entries, which is the usual way to assert that
an older interface raises a different exception than a newer one. When a module-level ``pytestmark`` and
a test-level mark disagree, the test is skipped unless *both* conditions allow it. See
``test/unit/conftest.py`` for the full behaviour.

When you change something in ``b2sdk/_internal``, check that:

#. the regression test sits in a shared directory, so it runs for every interface, and not in a single
   version directory where it would silently cover only one;
#. it imports from ``apiver_deps`` rather than a fixed ``b2sdk.vN``;
#. any interface that deliberately keeps the old behaviour has a test asserting that, placed in that
   version's directory or marked with ``apiver``;
#. ``nox -s unit`` passes - a suite that passes only for the newest interface is not enough.

Documentation
#############

To build the documentation and watch for changes (including the source code)::

    $ nox -s doc

To just build the documentation::

    $ nox --non-interactive -s doc
