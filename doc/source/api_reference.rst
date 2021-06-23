.. hint:: Use :doc:`quick_start` to quickly jump to examples

########################
API Reference
########################

Interface types
=======================

**b2sdk** API is divided into two parts, :ref:`public <api_public>` and :ref:`internal <api_internal>`. Please pay attention to which interface type you use.


.. tip::
   :ref:`Pinning versions <semantic_versioning>` properly ensures the stability of your application.


.. _api_public:

Public API
========================

.. toctree::
   api/application_key
   api/account_info
   api/cache
   api/api
   api/exception
   api/bucket
   api/file_lock
   api/data_classes
   api/enums
   api/progress
   api/sync
   api/utils
   api/transfer/emerge/write_intent
   api/transfer/outbound/outbound_source
   api/download_dest
   api/encryption/setting
   api/encryption/types

.. _api_internal:

Internal API
========================

.. note:: See :ref:`Internal interface <internal_interface>` chapter to learn when and how to safely use the Internal API

.. toctree::
   api/internal/session
   api/internal/raw_api
   api/internal/b2http
   api/internal/utils
   api/internal/cache
   api/internal/stream/chained
   api/internal/stream/hashing
   api/internal/stream/progress
   api/internal/stream/range
   api/internal/stream/wrapper
   api/internal/sync/action
   api/internal/sync/exception
   api/internal/sync/folder
   api/internal/sync/folder_parser
   api/internal/sync/path
   api/internal/sync/policy
   api/internal/sync/policy_manager
   api/internal/sync/scan_policies
   api/internal/sync/sync
   api/internal/transfer/inbound/downloader/abstract
   api/internal/transfer/inbound/downloader/parallel
   api/internal/transfer/inbound/downloader/simple
   api/internal/transfer/inbound/download_manager
   api/internal/transfer/inbound/file_metadata
   api/internal/transfer/outbound/upload_source
   api/internal/raw_simulator
