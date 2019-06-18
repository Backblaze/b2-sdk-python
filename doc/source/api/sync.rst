.. _sync:

########################
Synchronizer
########################

Synchronizer is a powerful utility with functionality of a basic backup application.
It is able to copy entire folders into the cloud and back to a local drive, providing
retention policies and many other options.

The **high performance** of sync is credited to parallelization of:

* listing local directory contents
* listing bucket contents
* uploads
* downloads

Synchronizer spawns threads to perform the operations listed above in parallel to shorten
the backup window to a minimum.


.. todo::
   practically the whole sync documentation is missing

Sync Options
============

Following are the important optional arguments that can be provided while initializing `Synchronizer` class.


* ``compare_version_mode``: When comparing the source and destination files for finding whether to replace them or not, `compare_version_mode` can be passed to specify the mode of comparision. For possible values see :class:`b2sdk.v1.CompareVersionMode`. Default value is :meth:`b2sdk.v1.CompareVersionMode.MODTIME`
* ``compare_threshold``: It's the minimum size/modification time difference between source and destination files before we assume that it is new and replace.
* ``newer_file_mode``: To identify whether to skip or replace if source is older. For possible values see :class:`b2sdk.v1.NewerFileSyncMode`. If you don't specify this the sync will raise :class:`b2sdk.v1.exception.DestFileNewer` in case any of the source file is older than destination.
* ``keep_days_or_delete``: specify policy to keep or delete older files. For possible values see :class:`b2sdk.v1.KeepOrDeleteMode`. Default is `DO_NOTHING`.
* ``keep_days``: if `keep_days_or_delete` is KEEP_BEFORE_DELETE then this specify for how many days should we keep.

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

    >>> no_progress = False
    >>> with SyncReport(sys.stdout, no_progress) as reporter:
            synchronizer.sync_folders(
                source_folder=source,
                dest_folder=destination,
                now_millis=int(round(time.time() * 1000)),
                reporter=reporter,
            )
    upload some.pdf
    upload som2.pdf


.. autoclass:: b2sdk.v1.Synchronizer()
   :special-members: __init__
   :members:
