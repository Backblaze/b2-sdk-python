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

.. toctree::

   b2sdk/sync/action
   b2sdk/sync/exception
   b2sdk/sync/file
   b2sdk/sync/folder_parser
   b2sdk/sync/folder
   b2sdk/sync/policy_manager
   b2sdk/sync/policy
   b2sdk/sync/scan_policies
   b2sdk/sync/sync
