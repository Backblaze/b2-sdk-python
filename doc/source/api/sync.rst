.. _sync:

########################
Synchronizer
########################

Synchronizer is a powerful utility with functionality of a basic backup application.
It is able to copy entire folders into the cloud and back to a local drive
or even between two cloud buckets, providing retention policies and many other options.

The **high performance** of sync is credited to parallelization of:

* listing local directory contents
* listing bucket contents
* uploads
* downloads

Synchronizer spawns threads to perform the operations listed above in parallel to shorten
the backup window to a minimum.

Sync Options
============

Following are the important optional arguments that can be provided while initializing `Synchronizer` class.


* ``compare_version_mode``: When comparing the source and destination files for finding whether to replace them or not, `compare_version_mode` can be passed to specify the mode of comparison. For possible values see :class:`b2sdk.v1.CompareVersionMode`. Default value is :py:attr:`b2sdk.v1.CompareVersionMode.MODTIME`
* ``compare_threshold``: It's the minimum size (in bytes)/modification time (in seconds) difference between source and destination files before we assume that it is new and replace.
* ``newer_file_mode``: To identify whether to skip or replace if source is older. For possible values see :class:`b2sdk.v1.NewerFileSyncMode`. If you don't specify this the sync will raise :class:`b2sdk.v1.exception.DestFileNewer` in case any of the source file is older than destination.
* ``keep_days_or_delete``: specify policy to keep or delete older files. For possible values see :class:`b2sdk.v1.KeepOrDeleteMode`. Default is `DO_NOTHING`.
* ``keep_days``: if `keep_days_or_delete` is :py:attr:`b2sdk.v1.CompareVersionMode.KEEP_BEFORE_DELETE` then this specify for how many days should we keep.

.. code-block:: python

    >>> from b2sdk.v1 import ScanPoliciesManager
    >>> from b2sdk.v1 import parse_sync_folder
    >>> from b2sdk.v1 import Synchronizer
    >>> from b2sdk.v1 import KeepOrDeleteMode, CompareVersionMode, NewerFileSyncMode
    >>> import time
    >>> import sys

    >>> source = '/home/user1/b2_example'
    >>> destination = 'b2://example-mybucket-b2'

    >>> source = parse_sync_folder(source, b2_api)
    >>> destination = parse_sync_folder(destination, b2_api)

    >>> policies_manager = ScanPoliciesManager(exclude_all_symlinks=True)

    >>> synchronizer = Synchronizer(
            max_workers=10,
            policies_manager=policies_manager,
            dry_run=False,
            allow_empty_source=True,
            compare_version_mode=CompareVersionMode.SIZE,
            compare_threshold=10,
            newer_file_mode=NewerFileSyncMode.REPLACE,
            keep_days_or_delete=KeepOrDeleteMode.KEEP_BEFORE_DELETE,
            keep_days=10,
        )

We have a file (hello.txt) which is present in destination but not on source (my local),
so it will be deleted and since our mode is to keep the delete file,
it will be hidden for 10 days in bucket.

.. code-block:: python

    >>> no_progress = False
    >>> with SyncReport(sys.stdout, no_progress) as reporter:
            synchronizer.sync_folders(
                source_folder=source,
                dest_folder=destination,
                now_millis=int(round(time.time() * 1000)),
                reporter=reporter,
            )
    upload f1.txt
    delete hello.txt (old version)
    hide   hello.txt

We changed f1.txt and added 1 byte. Since our compare_threshold is 10, it will not do anything.

.. code-block:: python

    >>> with SyncReport(sys.stdout, no_progress) as reporter:
            synchronizer.sync_folders(
                source_folder=source,
                dest_folder=destination,
                now_millis=int(round(time.time() * 1000)),
                reporter=reporter,
            )

We changed f1.txt and added more than 10 bytes.
Since our compare_threshold is 10, it will replace the file at destination folder.

.. code-block:: python

    >>> with SyncReport(sys.stdout, no_progress) as reporter:
            synchronizer.sync_folders(
                source_folder=source,
                dest_folder=destination,
                now_millis=int(round(time.time() * 1000)),
                reporter=reporter,
            )
    upload f1.txt

Let's just delete the file and not keep - keep_days_or_delete = DELETE
You can avoid passing keep_days argument in this case because it will be ignored anyways

.. code-block:: python

    >>> synchronizer = Synchronizer(
            max_workers=10,
            policies_manager=policies_manager,
            dry_run=False,
            allow_empty_source=True,
            compare_version_mode=CompareVersionMode.SIZE,
            compare_threshold=10,  # in bytes
            newer_file_mode=NewerFileSyncMode.REPLACE,
            keep_days_or_delete=KeepOrDeleteMode.DELETE,
        )

    >>> with SyncReport(sys.stdout, no_progress) as reporter:
        synchronizer.sync_folders(
            source_folder=source,
            dest_folder=destination,
            now_millis=int(round(time.time() * 1000)),
            reporter=reporter,
        )
    delete f1.txt
    delete f1.txt (old version)
    delete hello.txt (old version)
    upload f2.txt
    delete hello.txt (hide marker)

As you can see, it deleted f1.txt and it's older versions (no hide this time)
and deleted hello.txt also because now we don't want the file anymore.
also, we added another file f2.txt which gets uploaded.

Now we changed newer_file_mode to SKIP and compare_version_mode to MODTIME.
also uploaded a new version of f2.txt to bucket using B2 web.

.. code-block:: python

    >>> synchronizer = Synchronizer(
            max_workers=10,
            policies_manager=policies_manager,
            dry_run=False,
            allow_empty_source=True,
            compare_version_mode=CompareVersionMode.MODTIME,
            compare_threshold=10,  # in seconds
            newer_file_mode=NewerFileSyncMode.SKIP,
            keep_days_or_delete=KeepOrDeleteMode.DELETE,
        )
    >>> with SyncReport(sys.stdout, no_progress) as reporter:
        synchronizer.sync_folders(
            source_folder=source,
            dest_folder=destination,
            now_millis=int(round(time.time() * 1000)),
            reporter=reporter,
        )

As expected, nothing happened, it found a file that was older at source
but did not do anything because we skipped.

Now we changed newer_file_mode again to REPLACE and
also uploaded a new version of f2.txt to bucket using B2 web.

.. code-block:: python

    >>> synchronizer = Synchronizer(
            max_workers=10,
            policies_manager=policies_manager,
            dry_run=False,
            allow_empty_source=True,
            compare_version_mode=CompareVersionMode.MODTIME,
            compare_threshold=10,
            newer_file_mode=NewerFileSyncMode.REPLACE,
            keep_days_or_delete=KeepOrDeleteMode.DELETE,
        )
    >>> with SyncReport(sys.stdout, no_progress) as reporter:
        synchronizer.sync_folders(
            source_folder=source,
            dest_folder=destination,
            now_millis=int(round(time.time() * 1000)),
            reporter=reporter,
        )
    delete f2.txt (old version)
    upload f2.txt


Handling encryption
-------------------
The `Synchronizer` object may need `EncryptionSetting` instances to perform downloads and copies. For this reason, the
`sync_folder` method accepts an `EncryptionSettingsProvider`, see :ref:`server_side_encryption` for further explanation
and :ref:`encryption_provider` for public API.


Public API classes
==================

.. autoclass:: b2sdk.v1.ScanPoliciesManager()
   :inherited-members:
   :special-members: __init__
   :members:

.. autoclass:: b2sdk.v1.Synchronizer()
   :inherited-members:
   :special-members: __init__
   :members:

.. autoclass:: b2sdk.v1.SyncReport()
   :inherited-members:
   :special-members: __init__
   :members:


.. _encryption_provider:

Sync Encryption Settings Providers
==================================


.. autoclass:: b2sdk.v1.AbstractSyncEncryptionSettingsProvider()
   :inherited-members:
   :members:


.. autoclass:: b2sdk.v1.ServerDefaultSyncEncryptionSettingsProvider()
   :no-members:


.. autoclass:: b2sdk.v1.BasicSyncEncryptionSettingsProvider()
   :special-members: __init__
   :no-members:
