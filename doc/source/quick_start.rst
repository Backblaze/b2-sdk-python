.. _quick_start:

########################
Quick Start Guide
########################

***********************
Prepare b2sdk
***********************

.. code-block:: python

    >>> from b2sdk.v1 import *
    >>> info = InMemoryAccountInfo()
    >>> b2_api = B2Api(info)
    >>> application_key_id = '4a5b6c7d8e9f'
    >>> application_key = '001b8e23c26ff6efb941e237deb182b9599a84bef7'
    >>> b2_api.authorize_account("production", application_key_id, application_key)

.. tip::
   Get credentials from B2 website


***************
Synchronization
***************

.. code-block:: python

    >>> from b2sdk.v1 import ScanPoliciesManager
    >>> from b2sdk.v1 import parse_sync_folder
    >>> from b2sdk.v1 import Synchronizer
    >>> from b2sdk.v1 import SyncReport
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
        )

    >>> no_progress = False
    >>> encryption_settings_provider = BasicSyncEncryptionSettingsProvider({
            'bucket1': EncryptionSettings(mode=EncryptionMode.SSE_B2),
            'bucket2': EncryptionSettings(
                           mode=EncryptionMode.SSE_C,
                           key=EncryptionKey(secret=b'VkYp3s6v9y$B&E)H@McQfTjWmZq4t7w!', id='user-generated-key-id')
                       ),
            'bucket3': None,
        })
    >>> with SyncReport(sys.stdout, no_progress) as reporter:
            synchronizer.sync_folders(
                source_folder=source,
                dest_folder=destination,
                now_millis=int(round(time.time() * 1000)),
                reporter=reporter,
                encryption_settings_provider=encryption_settings_provider,
            )
    upload some.pdf
    upload som2.pdf


.. tip:: Sync is the preferred way of getting data into and out of B2 cloud, because it can achieve *highest performance* due to parallelization of scanning and data transfer operations.

    To learn more about sync, see :ref:`sync`.

Sync uses an encryption provider. In principle, it's a mapping between file metadata (bucket_name, file_info, etc) and
`EncryptionSetting`. The reason for employing such a mapping, rather than a single `EncryptionSetting`, is the fact that
users of Sync do not necessarily know up front what files it's going to upload and download. This approach enables using
unique keys, or key identifiers, across files. This is covered in greater detail in :ref:`server_side_encryption`.

In the example above, Sync will assume `SSE-B2` for all files in `bucket1`, `SSE-C` with the key provided for `bucket2`
and rely on bucket default for `bucket3`. Should developers need to provide keys per file (and not per bucket), they
need to implement their own :class:`b2sdk.v1.AbstractSyncEncryptionSettingsProvider`.

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


Delete a bucket
===============

.. code-block:: python

    >>> bucket_name = 'example-mybucket-b2-to-delete'
    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> b2_api.delete_bucket(bucket)

returns `None` if successful, raises an exception in case of error.

Update bucket info
==================

.. code-block:: python

    >>> new_bucket_type = 'allPrivate'
    >>> bucket_name = 'example-mybucket-b2'

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> bucket.update(bucket_type=new_bucket_type,
                      default_server_side_encryption=EncryptionSetting(mode=EncryptionMode.SSE_B2))
    {'accountId': '451862be08d0',
     'bucketId': '5485a1682662eb3e60980d10',
     'bucketInfo': {},
     'bucketName': 'example-mybucket-b2',
     'bucketType': 'allPrivate',
     'corsRules': [],
     'lifecycleRules': [],
     'revision': 3,
     'defaultServerSideEncryption': {'isClientAuthorizedToRead': True,
                                     'value': {'algorithm': 'AES256', 'mode': 'SSE-B2'}}},
     }

For more information see :meth:`b2sdk.v1.Bucket.update`.


************
File actions
************

.. tip:: Sync is the preferred way of getting files into and out of B2 cloud, because it can achieve *highest performance* due to parallelization of scanning and data transfer operations.

    To learn more about sync, see :ref:`sync`.

    Use the functions described below only if you *really* need to transfer a single file.


Upload file
===========

.. code-block:: python

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

Upload file encrypted with SSE-C
--------------------------------

.. code-block:: python

    >>> local_file_path = '/home/user1/b2_example/new.pdf'
    >>> b2_file_name = 'dummy_new.pdf'
    >>> file_info = {'how': 'good-file'}
    >>> encryption_setting = EncryptionSetting(
            mode=EncryptionMode.SSE_C,
            key=EncryptionKey(secret=b'VkYp3s6v9y$B&E)H@McQfTjWmZq4t7w!', id='user-generated-key-id'),
        )

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> bucket.upload_local_file(
            local_file=local_file_path,
            file_name=b2_file_name,
            file_infos=file_info,
            encryption=encryption_setting,
        )

Download file
=============

By id
-----

.. code-block:: python

    >>> from b2sdk.v1 import DownloadDestLocalFile
    >>> from b2sdk.v1 import DoNothingProgressListener

    >>> local_file_path = '/home/user1/b2_example/new2.pdf'
    >>> file_id = '4_z5485a1682662eb3e60980d10_f1195145f42952533_d20190403_m130258_c002_v0001111_t0002'
    >>> download_dest = DownloadDestLocalFile(local_file_path)
    >>> progress_listener = DoNothingProgressListener()

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
    >>> bucket.download_file_by_name(b2_file_name, download_dest)
    {'fileId': '4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044',
     'fileName': 'dummy_new.pdf',
     'contentType': 'application/pdf',
     'contentLength': 1870579,
     'contentSha1': 'd821849a70922e87c2b0786c0be7266b89d87df0',
     'fileInfo': {'how': 'good-file'}}


Downloading encrypted files
---------------------------

Both methods (`By name`_ and `By id`_) accept an optional `encryption` argument, similarly to `Upload file`_. This
parameter is necessary for downloading files encrypted with `SSE-C`.

List files
==========

.. code-block:: python

    >>> bucket_name = 'example-mybucket-b2'
    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> for file_info, folder_name in bucket.ls(show_versions=False):
    >>>     print(file_info.file_name, file_info.upload_timestamp, folder_name)
    f2.txt 1560927489000 None
    som2.pdf 1554296578000 None
    some.pdf 1554296579000 None
    test-folder/.bzEmpty 1561005295000 test-folder/

    # Recursive
    >>> bucket_name = 'example-mybucket-b2'
    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> for file_info, folder_name in bucket.ls(show_versions=False, recursive=True):
    >>>     print(file_info.file_name, file_info.upload_timestamp, folder_name)
    f2.txt 1560927489000 None
    som2.pdf 1554296578000 None
    some.pdf 1554296579000 None
    test-folder/.bzEmpty 1561005295000 test-folder/
    test-folder/folder_file.txt 1561005349000 None

Note: The files are returned recursively and in order so all files in a folder are printed one after another.
The folder_name is returned only for the first file in the folder.

.. code-block:: python

    # Within folder
    >>> bucket_name = 'example-mybucket-b2'
    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> for file_info, folder_name in bucket.ls(folder_to_list='test-folder', show_versions=False):
    >>>     print(file_info.file_name, file_info.upload_timestamp, folder_name)
    test-folder/.bzEmpty 1561005295000 None
    test-folder/folder_file.txt 1561005349000 None

    # list file versions
    >>> for file_info, folder_name in bucket.ls(show_versions=True):
    >>>     print(file_info.file_name, file_info.upload_timestamp, folder_name)
    f2.txt 1560927489000 None
    f2.txt 1560849524000 None
    som2.pdf 1554296578000 None
    some.pdf 1554296579000 None

For more information see :meth:`b2sdk.v1.Bucket.ls`.


Get file metadata
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
     'fileInfo': {'how': 'good-file', 'sse_c_key_id': 'user-generated-key-id'},
     'fileName': 'dummy_new.pdf',
     'uploadTimestamp': 1554361150000,
     "serverSideEncryption": {"algorithm": "AES256",
                              "mode": "SSE-C"},
     }

Copy file
=========

Please switch to  :meth:`b2sdk.v1.Bucket.copy`.

.. code-block:: python

    >>> file_id = '4_z5485a1682662eb3e60980d10_f118df9ba2c5131e8_d20190619_m065809_c002_v0001126_t0040'
    >>> bucket.copy(file_id, 'f2_copy.txt')
    {'accountId': '451862be08d0',
     'action': 'copy',
     'bucketId': '5485a1682662eb3e60980d10',
     'contentLength': 124,
     'contentSha1': '737637702a0e41dda8b7be79c8db1d369c6eef4a',
     'contentType': 'text/plain',
     'fileId': '4_z5485a1682662eb3e60980d10_f1022e2320daf707f_d20190620_m122848_c002_v0001123_t0020',
     'fileInfo': {'src_last_modified_millis': '1560848707000'},
     'fileName': 'f2_copy.txt',
     'uploadTimestamp': 1561033728000,
     "serverSideEncryption": {"algorithm": "AES256",
                              "mode": "SSE-B2"}}

If the ``content length`` is not provided and the file is larger than 5GB, ``copy`` would not succeed and error would be raised. If length is provided, then the file may be copied as a large file. Maximum copy part size can be set by ``max_copy_part_size`` - if not set, it will default to 5GB. If ``max_copy_part_size`` is lower than :term:`absoluteMinimumPartSize`, file would be copied in single request - this may be used to force copy in single request large file that fits in server small file limit.

Copying files allows for providing encryption settings for both source and destination files - `SSE-C` encrypted source files
cannot be used unless the proper key is provided.

If you want to copy just the part of the file, then you can specify the offset and content length:

.. code-block:: python

    >>> file_id = '4_z5485a1682662eb3e60980d10_f118df9ba2c5131e8_d20190619_m065809_c002_v0001126_t0040'
    >>> bucket.copy(file_id, 'f2_copy.txt', offset=1024, length=2048)

Note that content length is required for offset values other than zero.


For more information see :meth:`b2sdk.v1.Bucket.copy`.


Delete file
===========

.. code-block:: python

    >>> file_id = '4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044'
    >>> file_info = b2_api.delete_file_version(file_id, 'dummy_new.pdf')
    >>> print(file_info)
    {'file_id': '4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044',
     'file_name': 'dummy_new.pdf'}


Cancel large file uploads
=========================

.. code-block:: python

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> for file_version in bucket.list_unfinished_large_files():
            bucket.cancel_large_file(file_version.file_id)


