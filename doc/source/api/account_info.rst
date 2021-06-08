.. _AccountInfo:

########################
AccountInfo
########################

*AccountInfo* stores basic information about the account, such as *Application Key ID* and *Application Key*,
in order to let :py:class:`b2sdk.v1.B2Api` perform authenticated requests.

There are two usable implementations provided by **b2sdk**:

 * :py:class:`b2sdk.v1.InMemoryAccountInfo` - a basic implementation with no persistence
 * :py:class:`b2sdk.v1.SqliteAccountInfo` - for console and GUI applications

They both provide the full :ref:`AccountInfo interface <account_info_interface>`.

.. note::
   Backup applications and many server-side applications should :ref:`implement their own <my_account_info>` *AccountInfo*, backed by the metadata/configuration database of the application.


***************************
AccountInfo implementations
***************************

InMemoryAccountInfo
===================

*AccountInfo* with no persistence.

.. autoclass:: b2sdk.v1.InMemoryAccountInfo()
   :no-members:

   Implements all methods of :ref:`AccountInfo interface <account_info_interface>`.

   .. hint::

      Usage of this class is appropriate for secure Web applications which do not wish to persist any user data.

   Using this class for applications such as CLI, GUI or backup is discouraged, as ``InMemoryAccountInfo`` does not write down the authorization token persistently. That would be slow, as it would force the application to retrieve a new one on every command/click/backup start. Furthermore - an important property of *AccountInfo* is caching the ``bucket_name:bucket_id`` mapping; in case of ``InMemoryAccountInfo`` the cache will be flushed between executions of the program.

   .. method:: __init__()

      The constructor takes no parameters.


SqliteAccountInfo
=================

.. autoclass:: b2sdk.v1.SqliteAccountInfo()
   :inherited-members:
   :no-members:
   :special-members: __init__

   Implements all methods of :ref:`AccountInfo interface <account_info_interface>`.

   Uses a `SQLite database <https://www.sqlite.org/index.html>`_ for persistence
   and access synchronization between multiple processes. Not suitable for usage over NFS.

   Underlying database has the following schema:

   .. graphviz:: /dot/sqlite_account_info_schema.dot

   .. hint::

      Usage of this class is appropriate for interactive applications installed on a user's machine (i.e.: CLI and GUI applications).

      Usage of this class **might** be appropriate for non-interactive applications installed on the user's machine, such as backup applications. An alternative approach that should be considered is to store the *AccountInfo* data alongside the configuration of the rest of the application.


.. _my_account_info:

*********************
Implementing your own
*********************

When building a server-side application or a web service, you might want to implement your own *AccountInfo* class backed by a database. In such case, you should inherit from :py:class:`b2sdk.v1.UrlPoolAccountInfo`, which has groundwork for url pool functionality). If you cannot use it, inherit directly from :py:class:`b2sdk.v1.AbstractAccountInfo`.

.. code-block:: python

    >>> from b2sdk.v1 import UrlPoolAccountInfo
    >>> class MyAccountInfo(UrlPoolAccountInfo):
            ...


:py:class:`b2sdk.v1.AbstractAccountInfo` describes the interface, while :py:class:`b2sdk.v1.UrlPoolAccountInfo` and :py:class:`b2sdk.v1.UploadUrlPool` implement a part of the interface for in-memory upload token management.


.. _account_info_interface:

AccountInfo interface
=====================

.. autoclass:: b2sdk.v1.AbstractAccountInfo()
   :inherited-members:
   :private-members:
   :exclude-members: _abc_cache, _abc_negative_cache, _abc_negative_cache_version, _abc_registry


AccountInfo helper classes
==========================

.. autoclass:: b2sdk.v1.UrlPoolAccountInfo()
   :inherited-members:
   :no-members:
   :members: BUCKET_UPLOAD_POOL_CLASS, LARGE_FILE_UPLOAD_POOL_CLASS

   .. caution::
      This class is not part of the public interface. To find out how to safely use it, read :ref:`this <semantic_versioning>`.

.. autoclass:: b2sdk.account_info.upload_url_pool.UploadUrlPool()
   :inherited-members:
   :private-members:

   .. caution::
      This class is not part of the public interface. To find out how to safely use it, read :ref:`this <semantic_versioning>`.
