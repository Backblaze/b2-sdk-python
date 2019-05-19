.. _contributors_guide:

#########################################
Contributors Guide
#########################################

We encourage outside contributors to perform changes on our codebase. Many such changes have been merged already. In order to make it easier to contribute, core developers of this project:

* provide guidance (through the issue reporting system)
* provide tool assisted code review (through the Pull Request system)
* maintain a set of integration tests (run with a production cloud)
* maintain a set of (well over a hundred) unit tests
* automatically run unit tests on 14 versions of python (including ``osx``, ``Jython`` and ``pypy``)
* format the code automatically using `yapf <https://github.com/google/yapf>`_
* use static code analysis to find subtle/potential issues with maintainability
* maintain other Continous Integration tools (coverage tracker)

We marked the places in the code which are significantly less intuitive than others in a special way. To find them occurrences, use ``git grep '*magic*'``.

To install a development environment, please follow :ref:`this link <install_contributors>`.

To test in multiple python virtual environments, set the enviroment variable ``PYTHON_VIRTUAL_ENVS``
to be a space-separated list of their root directories.  When set, the makefile will run the
unit tests in each of the environments.

Before checking in, use the ``pre-commit.sh`` script to check code formatting, run
unit tests, run integration tests etc.

The integration tests need a file in your home directory called ``.b2_auth``
that contains two lines with nothing on them but your account ID and application key::

 accountId
 applicationKey

