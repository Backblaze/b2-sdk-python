.. _AccountInfo:

########################
AccountInfo
########################

AccountInfo stores basic information about the account, such as *Application Key ID* and *Application Key*,
in order to let :py:class:`b2sdk.v1.B2Api` perform authenticated requests.

There are two usable implementations provided by **b2sdk**:

 * :py:class:`b2sdk.v1.InMemoryAccountInfo` - basic implementation with no persistence
 * :py:class:`b2sdk.v1.SqliteAccountInfo` - for console and GUI applications

both provide the :ref:`AccountInfo interface <account_info_interface>`.

***************************
AccountInfo implementations
***************************

InMemoryAccountInfo
===================

AccountInfo with no persistence.

.. autoclass:: b2sdk.v1.InMemoryAccountInfo()
   :no-members:

   .. method:: __init__()

      The constructor takes no parameters.


SqliteAccountInfo
=================

.. autoclass:: b2sdk.v1.SqliteAccountInfo()
   :no-members:
   :special-members: __init__

   Uses a `SQLite database <https://www.sqlite.org/index.html>`_ for persistence
   and access synchronization between multiple processes. Not suitable for usage over NFS.

   Underlying database has the following schema:

   .. graphviz:: /dot/sqlite_account_info_schema.dot


.. _my_account_info:

*********************
Implementing your own
*********************

When building a web service, you might want to implement your own ``AccountInfo`` class backed by a database. In such case, you should inherit from :py:class:`b2sdk.v1.UrlPoolAccountInfo` - it has groundwork for url pool functionality).

.. code-block:: python

    >>> from b2sdk.v1 import UrlPoolAccountInfo
    >>> class MyAccountInfo(UrlPoolAccountInfo):
            ...


:py:class:`b2sdk.v1.AbstractAccountInfo` describes the interface. Below it, you can find :py:class:`b2sdk.v1.UrlPoolAccountInfo` and :py:class:`b2sdk.v1.UploadUrlPool`, which together implement a part of the interface for in-memory upload token management..


.. _account_info_interface:

AccountInfo interface
=====================

.. autoclass:: b2sdk.v1.AbstractAccountInfo()
   :private-members:
   :exclude-members: _abc_cache, _abc_negative_cache, _abc_negative_cache_version, _abc_registry


AccountInfo helper classes
==========================

.. autoclass:: b2sdk.v1.UrlPoolAccountInfo()
   :no-members:
   :members: BUCKET_UPLOAD_POOL_CLASS, LARGE_FILE_UPLOAD_POOL_CLASS

   .. caution::
      This class is not part of the public interface. To find out how to safely use it, read :ref:`this <semantic_versioning>`.

.. autoclass:: b2sdk.account_info.upload_url_pool.UploadUrlPool()
   :private-members:

   .. caution::
      This class is not part of the public interface. To find out how to safely use it, read :ref:`this <semantic_versioning>`.
