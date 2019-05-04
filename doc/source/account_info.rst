.. _AccountInfo:

########################
AccountInfo
########################

AccountInfo stores basic information about the account, such as *Application Key ID* and *Application Key*,
in order to let :py:class:`b2sdk.v1.B2Api` perform authenticated requests.

There are implementations provided by **b2sdk**: 

 * :py:class:`b2sdk.v1.InMemoryAccountInfo` - basic implementation with no persistence
 * :py:class:`b2sdk.v1.SqliteAccountInfo` - for console and GUI applications

both provide the :ref:`AccountInfo interface <_account_info_interface>`.

.. tip::
   If you need to implement your own *AccountInfo*, consider inheriting from :py:class:`b2sdk.v1.UrlPoolAccountInfo`


When building a web service, you might want to implement your own ``AccountInfo`` class backed by a database. In such case, you should inherit from :py:class:`b2sdk.v1.AbstractAccountInfo` or its subclass (we recommend :py:class:`b2sdk.v1.UrlPoolAccountInfo` - it has groundwork for url pool functionality)

.. code-block:: python

    >>> from b2sdk.v1 import AbstractAccountInfo
    >>> class MyAccountInfo(AbstractAccountInfo):
            ...

or

.. code-block:: python

    >>> from b2sdk.v1 import UrlPoolAccountInfo
    >>> class MyAccountInfo(UrlPoolAccountInfo):
            ...



InMemoryAccountInfo - AccountInfo with no persistence
=========================================================

.. autoclass:: b2sdk.v1.InMemoryAccountInfo()
   :no-members:


AccountInfo backed by sqlite3
=================================

.. autoclass:: b2sdk.v1.SqliteAccountInfo()
   :no-members:
   :special-members: __init__

   Uses a `SQLite database <https://www.sqlite.org/index.html>`_ for persistence
   and access synchronization between multiple processes. Not suitable for usage over NFS.

   Underlying database has the following schema:

   .. graphviz:: /dot/sqlite_account_info_schema.dot


.. _account_info_interface:


AccountInfo interface
=====================

.. autoclass:: b2sdk.v1.AbstractAccountInfo()


UploadUrlPool
=====================

.. autoclass:: b2sdk.v1.UrlPoolAccountInfo()
   :no-members:
   :members: BUCKET_UPLOAD_POOL_CLASS, LARGE_FILE_UPLOAD_POOL_CLASS

   .. caution::
      This class is not part of the public interface.

.. autoclass:: b2sdk.account_info.upload_url_pool.UploadUrlPool()

   .. caution::
      This class is not part of the public interface.
