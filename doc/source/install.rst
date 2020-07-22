########################
Installation Guide
########################

Installing as a dependency
==========================

**b2sdk** can simply be added to ``requirements.txt`` (or equivalent such as ``setup.py``, ``.pipfile`` etc).
In order to properly set a dependency, see :ref:`versioning chapter <semantic_versioning>` for details.

.. note::
  The stability of your application depends on correct :ref:`pinning of versions <semantic_versioning>`.


Installing a development version
================================

To install **b2sdk**, checkout the repository and run::

 pip install b2sdk

in your python environment.

.. _install_contributors:

Installing for contributors
===================================

You'll need to some Python packages installed.  To get all the latest things::

 pip install --upgrade --upgrade-strategy eager -r requirements.txt -r requirements-test.txt -r requirements-setup.txt

There is a `Makefile` with a rule to run the unit tests using the currently active Python::

 make setup
 make test

will install the required packages, then run the unit tests.
