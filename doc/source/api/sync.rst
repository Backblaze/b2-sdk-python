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
