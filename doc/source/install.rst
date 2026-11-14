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

To work on **b2sdk** itself, check out the repository and install your local checkout
in your python environment::

 pip install -e .

The ``-e`` flag installs the package in editable mode, so changes you make to the source
are picked up without reinstalling. Running ``pip install b2sdk`` instead would download
the published release from PyPI and ignore your local changes.

The project uses `nox <https://github.com/theacodes/nox>`_ and
`uv <https://docs.astral.sh/uv/>`_ to manage test and documentation environments. See
:ref:`contributors_guide` for the full development setup.
