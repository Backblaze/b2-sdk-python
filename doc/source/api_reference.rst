.. hint:: Use :doc:`quick_start` to quickly jump into examples

########################
API Reference
########################

Interface types
=======================

**b2sdk** API is divided into two parts, :ref:`public <api_public>` and :ref:`internal <api_internal>`. Please pay attention to which interface type you use and :ref:`pin the version range accordingly <semantic_versioning>`.


.. tip::
   Proper :ref:`pinning of versions <semantic_versioning>` ensures the stability of your application.


.. _api_public:

Public API
========================

.. todo::
   Public API doc list

.. toctree::
   account_info
   b2sdk/v1/api
   b2sdk/v1/exception
   b2sdk/v1/bucket
   b2sdk/v1/data_classes
   b2sdk/cache
   b2sdk/progress
   source_dest
   sync

.. _api_internal:

Internal API
========================

.. note:: See :ref:`Internal interface <internal_interface>` chapter to learn when and how to safely use the Internal API

.. todo::
   Private API doc list

.. toctree::
   b2sdk/session
   b2sdk/raw_api
   b2sdk/b2http
   b2sdk/utils
   b2sdk/transferer/abstract
   b2sdk/transferer/file_metadata
   b2sdk/transferer/parallel
   b2sdk/transferer/range
   b2sdk/transferer/simple
   b2sdk/transferer/transferer
