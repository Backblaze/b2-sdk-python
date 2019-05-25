.. _quick_start:

########################
Quick Start Guide
########################

*********************
Initialize API
*********************

.. code-block:: python

    >>> from b2sdk.account_info.in_memory import InMemoryAccountInfo
    >>> from b2sdk.account_info.sqlite_account_info import SqliteAccountInfo
    >>> from b2sdk.api import B2Api

    >>> info = InMemoryAccountInfo()  # to store credentials, tokens and cache in memory OR
    >>> info = SqliteAccountInfo()  # to store credentials, tokens and cache in ~/.b2_account_info
    >>> b2_api = B2Api(info)

To find out more about API object initialization, see :meth:`b2sdk.v1.B2Api.__init__`.

.. hint:: See :doc:`account_info` section to find out which *AccountInfo* implementation is best for your application.


*********************
Account authorization
*********************

.. code-block:: python

    >>> application_key_id = '4a5b6c7d8e9f'  # get credentials from from B2 website
    >>> application_key = '001b8e23c26ff6efb941e237deb182b9599a84bef7'
    >>> b2_api.authorize_account("production", application_key_id, application_key)

To find out more about account authorization, see :meth:`b2sdk.v1.B2Api.authorize_account`


***************
Synchronization
***************

.. code-block:: python

    >>> from b2sdk.sync.scan_policies import ScanPoliciesManager
    >>> from b2sdk.sync import parse_sync_folder, sync_folders
    >>> import time
    >>> import sys

    >>> source = '/home/user1/b2_example'
    >>> destination = 'b2://example-mybucket-b2'

    >>> source = parse_sync_folder(source, b2_api)
    >>> destination = parse_sync_folder(destination, b2_api)

    >>> policies_manager = ScanPoliciesManager(exclude_all_symlinks=True)

    >>> sync_folders(
            source_folder=source,
            dest_folder=destination,
            args=args,
            now_millis=int(round(time.time() * 1000)),
            stdout=sys.stdout,
            no_progress=False,
            max_workers=10,
            policies_manager=policies_manager,
            dry_run=False,
            allow_empty_source=True,
        )
    upload some.pdf
    upload som2.pdf


.. tip:: Sync is the preferred way of getting data into and out of B2 cloud, because it can achieve *highest performance* due to parallelization of scanning and data transfer operations.

    To learn more about sync, see `Sync <sync.html>`_.


**************
Bucket actions
**************

List buckets
============

.. code-block:: python

    >>> b2_api.list_buckets()
    [Bucket<346501784642eb3e60980d10,example-mybucket-b2-1,allPublic>]
    >>> for b in b2_api.list_buckets():
            print('%s  %-10s  %s' % (b.id_, b.type_, b.name))
    346501784642eb3e60980d10  allPublic   example-mybucket-b2-1

Create a bucket
===============

.. code-block:: python

    >>> bucket_name = 'example-mybucket-b2-1'  # must be unique in B2 (across all accounts!)
    >>> bucket_type = 'allPublic'  # or 'allPrivate'

    >>> b2_api.create_bucket(bucket_name, bucket_type)
    Bucket<346501784642eb3e60980d10,example-mybucket-b2-1,allPublic>

You can optionally store bucket info, CORS rules and lifecycle rules with the bucket. See :meth:`b2sdk.v1.B2Api.create_bucket`.


Remove a bucket
===============

.. code-block:: python

    >>> bucket_name = 'example-mybucket-b2-to-delete'
    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> b2_api.delete_bucket(bucket)
    {'accountId': '451862be08d0',
     'bucketId': '346501784642eb3e60980d10',
     'bucketInfo': {},
     'bucketName': 'example-mybucket-b2-to-delete',
     'bucketType': 'allPublic',
     'corsRules': [],
     'lifecycleRules': [],
     'revision': 3}


Update bucket info
==================

.. code-block:: python

    >>> new_bucket_type = 'allPrivate'
    >>> bucket_name = 'example-mybucket-b2'

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> bucket.update(bucket_type=new_bucket_type)
    {'accountId': '451862be08d0',
     'bucketId': '5485a1682662eb3e60980d10',
     'bucketInfo': {},
     'bucketName': 'example-mybucket-b2',
     'bucketType': 'allPrivate',
     'corsRules': [],
     'lifecycleRules': [],
     'revision': 3}

For more information see :meth:`b2sdk.v1.Bucket.update`.


************
File actions
************

.. tip:: Sync is the preferred way of getting files into and out of B2 cloud, because it can achieve *highest performance* due to parallelization of scanning and data transfer operations.

    To learn more about sync, see `Sync <sync.html>`_.

    Use the functions described below only if you *really* need to transfer a single file.


Upload file
===========

.. code-block:: python

    >>> from b2sdk.progress import make_progress_listener

    >>> local_file_path = '/home/user1/b2_example/new.pdf'
    >>> b2_file_name = 'dummy_new.pdf'
    >>> file_info = {'how': 'good-file'}

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> bucket.upload_local_file(
            local_file=local_file_path,
            file_name=b2_file_name,
            file_infos=file_info,
        )
    <b2sdk.file_version.FileVersionInfo at 0x7fc8cd560550>

This will work regardless of the size of the file - ``upload_local_file`` automatically uses large file upload API when necessary.

For more information see :meth:`b2sdk.v1.Bucket.upload_local_file`.

Download file
=============

By id
-----

.. code-block:: python

    >>> from b2sdk.progress import make_progress_listener
    >>> from b2sdk.download_dest import DownloadDestLocalFile

    >>> local_file_path = '/home/user1/b2_example/new2.pdf'
    >>> file_id = '4_z5485a1682662eb3e60980d10_f1195145f42952533_d20190403_m130258_c002_v0001111_t0002'
    >>> progress_listener = make_progress_listener(local_file_path, True)
    >>> download_dest = DownloadDestLocalFile(local_file_path)
    >>> b2_api.download_file_by_id(file_id, download_dest, progress_listener)
    {'fileId': '4_z5485a1682662eb3e60980d10_f1195145f42952533_d20190403_m130258_c002_v0001111_t0002',
     'fileName': 'som2.pdf',
     'contentType': 'application/pdf',
     'contentLength': 1870579,
     'contentSha1': 'd821849a70922e87c2b0786c0be7266b89d87df0',
     'fileInfo': {'src_last_modified_millis': '1550988084299'}}

    >>> print('File name:   ', download_dest.file_name)
    File name:    som2.pdf
    >>> print('File id:     ', download_dest.file_id)
    File id:      4_z5485a1682662eb3e60980d10_f1195145f42952533_d20190403_m130258_c002_v0001111_t0002
    >>> print('File size:   ', download_dest.content_length)
    File size:    1870579
    >>> print('Content type:', download_dest.content_type)
    Content type: application/pdf
    >>> print('Content sha1:', download_dest.content_sha1)
    Content sha1: d821849a70922e87c2b0786c0be7266b89d87df0

By name
-------

.. code-block:: python

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> b2_file_name = 'dummy_new.pdf'
    >>> local_file_name = '/home/user1/b2_example/new3.pdf'
    >>> download_dest = DownloadDestLocalFile(local_file_name)
    >>> progress_listener = make_progress_listener(local_file_path, True)
    >>> bucket.download_file_by_name(b2_file_name, download_dest, progress_listener)
    {'fileId': '4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044',
     'fileName': 'dummy_new.pdf',
     'contentType': 'application/pdf',
     'contentLength': 1870579,
     'contentSha1': 'd821849a70922e87c2b0786c0be7266b89d87df0',
     'fileInfo': {'how': 'good-file'}}


List files
==========

.. code-block:: python

    >>> bucket_name = 'example-mybucket-b2'
    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> max_to_show = 1  # max files to show, default=100, optional parameter
    >>> start_file_name = 'som'  # default is '', optional parameter
    >>> bucket.list_file_names(start_file_name, max_to_show)
    {'files': [{'accountId': '451862be08d0',
       'action': 'upload',
       'bucketId': '5485a1682662eb3e60980d10',
       'contentLength': 1870579,
       'contentSha1': 'd821849a70922e87c2b0786c0be7266b89d87df0',
       'contentType': 'application/pdf',
       'fileId': '4_z5485a1682662eb3e60980d10_f1195145f42952533_d20190403_m130258_c002_v0001111_t0002',
       'fileInfo': {'src_last_modified_millis': '1550988084299'},
       'fileName': 'som2.pdf',
       'uploadTimestamp': 1554296578000}],
     'nextFileName': 'som2.pdf '}

    # list file versions
    >>> bucket.list_file_versions()
    {'files': [{'accountId': '451862be08d0',
       'action': 'upload',
       'bucketId': '5485a1682662eb3e60980d10',
       'contentLength': 1870579,
       'contentSha1': 'd821849a70922e87c2b0786c0be7266b89d87df0',
       'contentType': 'application/pdf',
       'fileId': '4_z5485a1682662eb3e60980d10_f1195145f42952533_d20190403_m130258_c002_v0001111_t0002',
       'fileInfo': {'src_last_modified_millis': '1550988084299'},
       'fileName': 'som2.pdf',
       'uploadTimestamp': 1554296578000}

For more information see :meth:`b2sdk.v1.Bucket.ls`.

.. todo::
   use ls in the examples above, ``list_file_names`` and ``list_file_versions`` are legacy/discouraged, we shouldn't be promoting them


Get file meta information
=========================

.. code-block:: python

    >>> file_id = '4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044'
    >>> b2_api.get_file_info(file_id)
    {'accountId': '451862be08d0',
     'action': 'upload',
     'bucketId': '5485a1682662eb3e60980d10',
     'contentLength': 1870579,
     'contentSha1': 'd821849a70922e87c2b0786c0be7266b89d87df0',
     'contentType': 'application/pdf',
     'fileId': '4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044',
     'fileInfo': {'how': 'good-file'},
     'fileName': 'dummy_new.pdf',
     'uploadTimestamp': 1554361150000}


Delete file
===========

.. code-block:: python

    >>> file_id = '4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044'
    >>> file_info = b2_api.delete_file_version(file_id, 'dummy_new.pdf')


Cancel large file uploads
=========================

.. code-block:: python

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> for file_version in bucket.list_unfinished_large_files():
            bucket.cancel_large_file(file_version.file_id)
