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
    >>> with SyncReport(sys.stdout, no_progress) as reporter:
            synchronizer.sync_folders(
                source_folder=source,
                dest_folder=destination,
                now_millis=int(round(time.time() * 1000)),
                reporter=reporter,
            )
    upload some.pdf
    upload som2.pdf


.. tip:: Sync is the preferred way of getting data into and out of B2 cloud, because it can achieve *highest performance* due to parallelization of scanning and data transfer operations.

    To learn more about sync, see :ref:`sync`.


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
     'fileInfo': {'how': 'good-file'},
     'fileName': 'dummy_new.pdf',
     'uploadTimestamp': 1554361150000}

Copy file
=========

Note that :meth:`b2sdk.v1.Bucket.copy_file` is deprecated. Please switch to  :meth:`b2sdk.v2.Bucket.copy`.

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
     'uploadTimestamp': 1561033728000}

If the content length is not provided, then if file is larger than server small file limit, copy would not success and error would raise. If length is provided, then file may be splitted on parts and be copied as a large file. Maximum copy part size can be set by ``max_copy_part_size`` - if not set it will default to server maximum part size. If ``max_copy_part_size`` lower than :term:`absoluteMinimumPartSize` file would be copied in single request - this may be used to force copy in single request large file that fits in server small file limit.

If you want to copy just the part of the file, then you can specify the offset and content length:

.. code-block:: python

    >>> file_id = '4_z5485a1682662eb3e60980d10_f118df9ba2c5131e8_d20190619_m065809_c002_v0001126_t0040'
    >>> bucket.copy(file_id, 'f2_copy.txt', offset=1024, length=2048)

Note that content length is required for offset values other than zero.


For more information see :meth:`b2sdk.v1.Bucket.copy_file`.


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


**************
Advanced Usage
**************

Concatenate files
=================

:meth:`b2sdk.v1.Bucket.concatenate` accepts an iterable of upload sources (either local or remote). It can be used to glue remote files together, back-to-back, into a new file. To support automatic continuation, :meth:`b2sdk.v1.Bucket.concatenate` creates plan before it starts to copy/upload anything. :meth:`b2sdk.v1.Bucket.concatenate_stream` can be used to process large input iterator.

Concatenate files of known size
---------------------------------

.. code-block:: python

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> input_sources = [
    ...     RemoteFileUploadSource('4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044', offset=100, length=100),
    ...     LocalUploadSource('my_local_path/to_file.txt'),
    ...     RemoteFileUploadSource('4_z5485a1682662eb3e60980d10_f1022e2320daf707f_d20190620_m122848_c002_v0001123_t0020', length=2123456789),
    ... ]
    >>> file_info = {'how': 'good-file'}
    >>> bucket.concatenate(input_sources, remote_name, file_info)
    <b2sdk.file_version.FileVersionInfo at 0x7fc8cd560551>

If one of remote source has length smaller than :term:`absoluteMinimumPartSize` then it cannot be copied into large file part. Such remote source would be downloaded and concatenated locally with local source or with other downloaded remote source.

Please note that this method only allows checksum verification for local upload sources. Checksum verification for remote sources is available only when local copy is available. In such case :meth:`b2sdk.v1.Bucket.create_file` can be used with overalapping ranges in input.

For more information about ``concatenate`` please see :meth:`b2sdk.v1.Bucket.concatenate` and :class:`b2sdk.v1.RemoteUploadSource`.

Concatenate files of known size (streamed version)
--------------------------------------------------

:meth:`b2sdk.v1.Bucket.concatenate` accepts an iterable of upload sources (either local or remote). The operation would not be planned ahead so it supports very large output objects, but continuation is only possible for local only sources and provided unfinished large file id. See more about continuation in :meth:`b2sdk.v1.Bucket.create_file` paragraph about continuation.

.. code-block:: python

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> input_sources = [
    ...     RemoteFileUploadSource('4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044', offset=100, length=100),
    ...     LocalUploadSource('my_local_path/to_file.txt'),
    ...     RemoteFileUploadSource('4_z5485a1682662eb3e60980d10_f1022e2320daf707f_d20190620_m122848_c002_v0001123_t0020', length=2123456789),
    ... ]
    >>> file_info = {'how': 'good-file'}
    >>> bucket.concatenate_stream(input_sources, remote_name, file_info)
    <b2sdk.file_version.FileVersionInfo at 0x7fc8cd560551>



Concatenate files of unknown size
---------------------------------

Currently it is not supported by **b2sdk**.


Update a file efficiently
====================================

:meth:`b2sdk.v1.Bucket.create_file` accepts an iterable which *can contain overlapping destination ranges*. To support automatic continuation, :meth:`b2sdk.v1.Bucket.create_file` creates plan before it starts to copy/upload anything. :meth:`b2sdk.v1.Bucket.create_file_stream()` can be used to process large input iterator. 

Plase note that all following examples *creates* new file - data in bucket is immutable, but it can create a new file version with the same name as remote source. Old version has to be deleted manually.

Append to the end of a file
---------------------------

The assumption here is that the file has been appended to since it was last uploaded to. This assumption is verified by **b2sdk** when possible by recalculating checksums of the overlapping remote and local ranges. If copied remote part sha does not match with locally available source, file creation process would be interrupted and error raised.

.. code-block:: python

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> input_sources = [
    ...     WriteIntent(
    ...         data=RemoteFileUploadSource(
    ...             '4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044',
    ...             offset=0,
    ...             length=5000000,
    ...         ),
    ...         destination_offset=0,
    ...     ),
    ...     WriteIntent(
    ...         data=LocalFileUploadSource('my_local_path/to_file.txt'),  # of length 60000000
    ...         destination_offset=0,
    ...     ),
    ... ]
    >>> file_info = {'how': 'good-file'}
    >>> bucket.create_file(input_sources, remote_name, file_info)
    <b2sdk.file_version.FileVersionInfo at 0x7fc8cd560552>

`LocalUploadSource` has the size determined automatically in this case. This is more efficient than :meth:`b2sdk.v1.Bucket.concatenate`, as it can use the overlapping ranges when a remote part is smaller than :term:`absoluteMinimumPartSize` to prevent downloading a range (when concatenating, local source would have destination offset at the end of remote source)

For more information see :meth:`b2sdk.v1.Bucket.create_file`.


Change the middle of the remote file
------------------------------------

.. code-block:: python

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> input_sources = [
    ...     WriteIntent(
    ...         RemoteUploadSource('4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044', offset=0, length=4000000),
    ...         destination_offset=0,
    ...     ),
    ...     WriteIntent(
    ...         LocalFileUploadSource('my_local_path/to_file.txt'),  # length 1024 - read from disk
    ...         destination_offset=4000000,
    ...     ),
    ...     WriteIntent(
    ...         RemoteUploadSource('4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044', offset=4001024, length=123456789),
    ...         destination_offset=4001024,
    ...     ),
    ... ]
    >>> file_info = {'how': 'good-file'}
    >>> bucket.create_file(input_sources, remote_name, file_info)
    <b2sdk.file_version.FileVersionInfo at 0x7fc8cd560552>

`LocalUploadSource` has the size determined automatically in this case. This is more efficient than :meth:`b2sdk.v1.Bucket.concatenate`, as it can use the overlapping ranges when a remote part is smaller than :term:`absoluteMinimumPartSize` to prevent downloading a range.

For more information see :meth:`b2sdk.v1.Bucket.create_file`.


Synthetize a file from local and remote parts
=============================================

This is useful for advanced usage patterns such as:
 - *synthetic backup*
 - *reverse synthetic backup*
 - mostly-server-side cutting and gluing uncompressed media files such as `wav` and `avi` with rewriting of file headers
 - various deduplicated backup scenarios

Please note that :meth:`b2sdk.v1.Bucket.create_file_stream` accepts **an ordered iterable** which *can contain overlapping ranges*, so the operation does not need to be planned ahead, but can be streamed, which supports very large output objects.

Scenarios such as below are then possible:

.. code-block::

    A          C       D           G
    |          |       |           |
    | cloud-AC |       | cloud-DG  |
    |          |       |           |
    v          v       v           v
    ############       #############
    ^                              ^
    |                              |
    +---- desired file A-G --------+
    |                              |
    |                              |
    |    ######################### |
    |    ^                       ^ |
    |    |                       | |
    |    |      local file-BF    | |
    |    |                       | |
    A    B     C       D       E F G

.. code-block:: python

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> def generate_input():
    ...     yield WriteIntent(
    ...         RemoteUploadSource('4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044', offset=0, length=lengthC),
    ...         destination_offset=0,
    ...     ),
    ...     yield WriteIntent(
    ...         LocalFileUploadSource('my_local_path/to_file.txt'), # length = offsetF - offsetB
    ...         destination_offset=offsetB,
    ...     ),
    ...     yield WriteIntent(
    ...         RemoteUploadSource('4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044', offset=0, length=offsetG-offsetD),
    ...         destination_offset=offsetD,
    ...     ),
    ...
    >>> file_info = {'how': 'good-file'}
    >>> bucket.create_file(generate_input(), remote_name, file_info)
    <b2sdk.file_version.FileVersionInfo at 0x7fc8cd560552>


In such case, if the sizes allow for it (there would be no parts smaller than :term:`absoluteMinimumPartSize`), the only uploaded part will be `C-D`. Otherwise, more data will be uploaded, but the data transfer will be reduced in most cases. :meth:`b2sdk.v1.Bucket.create_file` does not guarantee that outbound transfer usage would be optimal, it would just use greedy algorithm with as small look aheads as possible.
For more information see :meth:`b2sdk.v1.Bucket.create_file`.


Prioritize remote or local sources
==================================

:meth:`b2sdk.v1.Bucket.create_file` and streamed version :meth:`b2sdk.v1.Bucket.create_file_stream` supports source origin prioritization so planner would know which sources should be used for overlapping ranges. Supported values are: `local`, `remote`, `local_verification` (default).

.. code-block::

    A              D               G
    |              |               |
    | cloud-AD     |               |
    |              |               |
    v              v               |
    ################               |
    ^                              |
    |                              |
    +---- desired file A-G --------+
    |                              |
    |                              |
    |    #######   #################
    |    ^     ^   ^               |
    |    |     |   |               |
    |    |   local file BC and DE  |
    |    |     |   |               |
    A    B     C   D               E

    A=0, B=50M, C=80M, D=100M, E=200

.. code-block:: python

    >>> bucket.create_file(input_sources, remote_name, file_info, prioritize='local')
    # planner parts: cloud[A, B], local[B, C], remote[C, D], local[D, E]

.. code-block:: python

    >>> planner.create_file(input_sources, remote_name, file_info, prioritize='remote')
    # produce parts: cloud[A, D], local[D, E]

.. code-block:: python

    >>> bucket.create_file(input_sources, remote_name, file_info)
    # or
    >>> bucket.create_file(input_sources, remote_name, file_info, prioritize='local_verification')
    # planner parts: cloud[A, B], cloud[B, C], cloud[C, D], local[D, E]

In `local_verification` prioritization remote range is splitted on three parts to allow local checksum verification. Note that this is only a planner setting - remote part is always verifified if local part exists.

TODO: prioritization should accept enum

Continue file update
====================

:meth:`b2sdk.v1.Bucket.create_file` supports automatic continuation or manual continuation. :meth:`b2sdk.v1.Bucket.create_file_stream` supports only manual continuation for local source only inputs. The situation looks the same for :meth:`b2sdk.v1.Bucket.concatenate` and :meth:`b2sdk.v1.Bucket.concatenate_stream` (streamed version supports only manual continuation of local sources). Also :meth:`b2sdk.v1.Bucket.upload` and :meth:`b2sdk.v2.Bucket.copy` supports both auto and manual continuation.

Manual continuation
-------------------

.. code-block:: python

    >>> def large_file_callback(large_file):
    ...     # storage is not part of the interface - here only for demonstration purposes
    ...     storage.store({'name': remote_name, 'large_file_id': large_file.id})
    >>> bucket.create_file(input_sources, remote_name, file_info, large_file_callback=large_file_callback)
    # ...
    >>> large_file_id = storage.query({'name': remote_name})[0]['large_file_id']
    >>> bucket.create_file(input_sources, remote_name, file_info, large_file_id=large_file_id)


Manual continuation (streamed version)
--------------------------------------

.. code-block:: python

    >>> def large_file_callback(large_file):
    ...     # storage is not part of the interface - here only for demonstration purposes
    ...     storage.store({'name': remote_name, 'large_file_id': large_file.id})
    >>> bucket.create_file_stream(input_sources, remote_name, file_info, large_file_callback=large_file_callback)
    # ...
    >>> large_file_id = storage.query({'name': remote_name})[0]['large_file_id']
    >>> bucket.create_file_stream(input_sources, remote_name, file_info, large_file_id=large_file_id)

Streams that contains remote sources cannot be continued with :meth:`b2sdk.v1.Bucket.create_file` - internally :meth:`b2sdk.v1.Bucket.create_file` stores plan information in file info for such inputs, and verifies it before any copy/upload and :meth:`b2sdk.v1.Bucket.create_file_stream` cannot store this information. Local source only inputs can be safely continued with :meth:`b2sdk.v1.Bucket.create_file` in auto continue mode or manual continue mode (because plan information is not stored in file info in such case).

Auto continuation
-----------------

.. code-block:: python

    >>> bucket.create_file(input_sources, remote_name, file_info)

For local source only input, :meth:`b2sdk.v1.Bucket.create_file` would try to find matching unfinished large file. It will verify uploaded parts checksums with local sources - the most completed, having all uploaded parts matched candidate would be automatically selected as file to continue. If there is no matching candidate (even if there are unfinished files for the same file name) new large file would be started.

In other cases plan information would be generated and :meth:`b2sdk.v1.Bucket.create_file` would try to find unfinished large file with matching plan info in its file info. If there is one or more such unfinished large files, :meth:`b2sdk.v1.Bucket.create_file` would verify checksums for all locally available parts and choose any matching candidate. If all candidates fails on uploaded parts checksums verification, process is interrupted and error raises. In such case corrupted unfinished large files should be cancelled manullay and :meth:`b2sdk.v1.Bucket.create_file` should be retried, or auto continuation should be turned off with `auto_continue=False`


No continuation
---------------

.. code-block:: python

    >>> bucket.create_file(input_sources, remote_name, file_info, auto_continue=False)


Note, that this only forces start of a new large file, and it is possible to continue the process with either auto or manual modes.
