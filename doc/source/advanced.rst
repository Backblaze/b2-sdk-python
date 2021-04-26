.. _AdvancedUsagePatterns:

#########################################
Advanced usage patterns
#########################################

B2 server API allows for creation of an object from existing objects. This allows to avoid transferring data from the source machine if the desired outcome can be (at least partially) constructed from what is already on the server.

The way **b2sdk** exposes this functionality is through a few functions that allow the user to express the desired outcome and then the library takes care of planning and executing the work. Please refer to the table below to compare the support of object creation methods for various usage patterns.

*****************
Available methods
*****************

.. |br| raw:: html

  <br/>

.. _advanced_methods_support_table:

+--------------------------------------------+--------+---------------------+--------------------------+------------------------------------+
| Method / supported options                 | Source | Range |br| overlap  | Streaming |br| interface | :ref:`Continuation <continuation>` |
+============================================+========+=====================+==========================+====================================+
| :meth:`b2sdk.v1.Bucket.upload`             | local  | no                  | no                       | automatic                          |
+--------------------------------------------+--------+---------------------+--------------------------+------------------------------------+
| :meth:`b2sdk.v1.Bucket.copy`               | remote | no                  | no                       | automatic                          |
+--------------------------------------------+--------+---------------------+--------------------------+------------------------------------+
| :meth:`b2sdk.v1.Bucket.concatenate`        | any    | no                  | no                       | automatic                          |
+--------------------------------------------+--------+---------------------+--------------------------+------------------------------------+
| :meth:`b2sdk.v1.Bucket.concatenate_stream` | any    | no                  | yes                      | manual                             |
+--------------------------------------------+--------+---------------------+--------------------------+------------------------------------+
| :meth:`b2sdk.v1.Bucket.create_file`        | any    | yes                 | no                       | automatic                          |
+--------------------------------------------+--------+---------------------+--------------------------+------------------------------------+
| :meth:`b2sdk.v1.Bucket.create_file_stream` | any    | yes                 | yes                      | manual                             |
+--------------------------------------------+--------+---------------------+--------------------------+------------------------------------+

Range overlap
=============

Some methods support overlapping ranges between local and remote files. **b2sdk** tries to use the remote ranges as much as possible, but due to limitations of ``b2_copy_part`` (specifically the minimum size of a part) that may not be always possible. A possible solution for such case is to download a (small) range and then upload it along with another one, to meet the ``b2_copy_part`` requirements. This can be improved if the same data is already available locally - in such case **b2sdk** will use the local range rather than downloading it.


Streaming interface
===================

Some object creation methods start writing data before reading the whole input (iterator). This can be used to write objects that do not have fully known contents without writing them first locally, so that they could be copied. Such usage pattern can be relevant to small devices which stream data to B2 from an external NAS, where caching large files such as media files or virtual machine images is not an option.

Please see :ref:`advanced method support table <advanced_methods_support_table>` to see where streaming interface is supported. 

Continuation
============

Please see :ref:`here <continuation>`


*****************
Concatenate files
*****************

:meth:`b2sdk.v1.Bucket.concatenate` accepts an iterable of upload sources (either local or remote). It can be used to glue remote files together, back-to-back, into a new file.

:meth:`b2sdk.v1.Bucket.concatenate_stream` does not create and validate a plan before starting the transfer, so it can be used to process a large input iterator, at a cost of limited automated continuation. 


Concatenate files of known size
=================================

.. code-block:: python

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> input_sources = [
    ...     CopySource('4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044', offset=100, length=100),
    ...     UploadSourceLocalFile('my_local_path/to_file.txt'),
    ...     CopySource('4_z5485a1682662eb3e60980d10_f1022e2320daf707f_d20190620_m122848_c002_v0001123_t0020', length=2123456789),
    ... ]
    >>> file_info = {'how': 'good-file'}
    >>> bucket.concatenate(input_sources, remote_name, file_info)
    <b2sdk.file_version.FileVersionInfo at 0x7fc8cd560551>

If one of remote source has length smaller than :term:`absoluteMinimumPartSize` then it cannot be copied into large file part. Such remote source would be downloaded and concatenated locally with local source or with other downloaded remote source.

Please note that this method only allows checksum verification for local upload sources. Checksum verification for remote sources is available only when local copy is available. In such case :meth:`b2sdk.v1.Bucket.create_file` can be used with overalapping ranges in input.

For more information about ``concatenate`` please see :meth:`b2sdk.v1.Bucket.concatenate` and :class:`b2sdk.v1.CopySource`.


Concatenate files of known size (streamed version)
==================================================

:meth:`b2sdk.v1.Bucket.concatenate` accepts an iterable of upload sources (either local or remote). The operation would not be planned ahead so it supports very large output objects, but continuation is only possible for local only sources and provided unfinished large file id. See more about continuation in :meth:`b2sdk.v1.Bucket.create_file` paragraph about continuation.

.. code-block:: python

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> input_sources = [
    ...     CopySource('4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044', offset=100, length=100),
    ...     UploadSourceLocalFile('my_local_path/to_file.txt'),
    ...     CopySource('4_z5485a1682662eb3e60980d10_f1022e2320daf707f_d20190620_m122848_c002_v0001123_t0020', length=2123456789),
    ... ]
    >>> file_info = {'how': 'good-file'}
    >>> bucket.concatenate_stream(input_sources, remote_name, file_info)
    <b2sdk.file_version.FileVersionInfo at 0x7fc8cd560551>



Concatenate files of unknown size
=================================

While it is supported by B2 server, this pattern is currently not supported by **b2sdk**.


*********************
Synthethize an object
*********************

Using methods described below an object can be created from both local and remote sources while avoiding downloading small ranges when such range is already present on a local drive.

Update a file efficiently
====================================

:meth:`b2sdk.v1.Bucket.create_file` accepts an iterable which *can contain overlapping destination ranges*.

.. note::
  Following examples *create* new file - data in bucket is immutable, but **b2sdk** can create a new file version with the same name and updated content


Append to the end of a file
---------------------------

The assumption here is that the file has been appended to since it was last uploaded to. This assumption is verified by **b2sdk** when possible by recalculating checksums of the overlapping remote and local ranges. If copied remote part sha does not match with locally available source, file creation process would be interrupted and an exception would be raised.

.. code-block:: python

    >>> bucket = b2_api.get_bucket_by_name(bucket_name)
    >>> input_sources = [
    ...     WriteIntent(
    ...         data=CopySource(
    ...             '4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044',
    ...             offset=0,
    ...             length=5000000,
    ...         ),
    ...         destination_offset=0,
    ...     ),
    ...     WriteIntent(
    ...         data=UploadSourceLocalFile('my_local_path/to_file.txt'),  # of length 60000000
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
    ...         CopySource('4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044', offset=0, length=4000000),
    ...         destination_offset=0,
    ...     ),
    ...     WriteIntent(
    ...         UploadSourceLocalFile('my_local_path/to_file.txt'),  # length=1024, here not passed and just checked from local source using seek
    ...         destination_offset=4000000,
    ...     ),
    ...     WriteIntent(
    ...         CopySource('4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044', offset=4001024, length=123456789),
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

This is useful for expert usage patterns such as:
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
    ...         CopySource('4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044', offset=0, length=lengthC),
    ...         destination_offset=0,
    ...     )
    ...     yield WriteIntent(
    ...         UploadSourceLocalFile('my_local_path/to_file.txt'), # length = offsetF - offsetB
    ...         destination_offset=offsetB,
    ...     )
    ...     yield WriteIntent(
    ...         CopySource('4_z5485a1682662eb3e60980d10_f113f963288e711a6_d20190404_m065910_c002_v0001095_t0044', offset=0, length=offsetG-offsetD),
    ...         destination_offset=offsetD,
    ...     )
    ...
    >>> file_info = {'how': 'good-file'}
    >>> bucket.create_file(generate_input(), remote_name, file_info)
    <b2sdk.file_version.FileVersionInfo at 0x7fc8cd560552>


In such case, if the sizes allow for it (there would be no parts smaller than :term:`absoluteMinimumPartSize`), the only uploaded part will be `C-D`. Otherwise, more data will be uploaded, but the data transfer will be reduced in most cases. :meth:`b2sdk.v1.Bucket.create_file` does not guarantee that outbound transfer usage would be optimal, it uses a simple greedy algorithm with as small look-aheads as possible.

For more information see :meth:`b2sdk.v1.Bucket.create_file`.

Encryption
----------

Even if files `A-C` and `D-G` are encrypted using `SSE-C` with different keys, they can still be used in a single :meth:`b2sdk.v1.Bucket.create_file` call, because :class:`b2sdk.v1.CopySource` accepts an optional :class:`b2sdk.v1.EncryptionSetting`.

Prioritize remote or local sources
----------------------------------

:meth:`b2sdk.v1.Bucket.create_file` and :meth:`b2sdk.v1.Bucket.create_file_stream` support source/origin prioritization, so that planner would know which sources should be used for overlapping ranges. Supported values are: `local`, `remote` and `local_verification`.

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

Here the planner has only used a remote source where remote range was not available, minimizing downloads.

.. code-block:: python

    >>> planner.create_file(input_sources, remote_name, file_info, prioritize='remote')
    # planner parts: cloud[A, D], local[D, E]

Here the planner has only used a local source where remote range was not available, minimizing uploads.

.. code-block:: python

    >>> bucket.create_file(input_sources, remote_name, file_info)
    # or
    >>> bucket.create_file(input_sources, remote_name, file_info, prioritize='local_verification')
    # planner parts: cloud[A, B], cloud[B, C], cloud[C, D], local[D, E]

In `local_verification` mode the remote range was artificially split into three parts to allow for checksum verification against matching local ranges.

.. note::
  `prioritize` is just a planner setting - remote parts are always verified if matching local parts exists.

.. TODO::
  prioritization should accept enum, not string


.. _continuation:

************
Continuation
************

Continuation of upload
======================

In order to continue a simple upload session, **b2sdk** checks for any available sessions with of the same ``file name``, ``file_info`` and ``media type``, verifying the size of an object as much as possible.

To support automatic continuation, some advanced methods create a plan before starting copy/upload operations, saving the hash of that plan in ``file_info`` for increased reliability.

If that is not available, ``large_file_id`` can be extracted via callback during the operation start. It can then be passed into the subsequent call to continue the same task, though the responsibility for passing the exact same input is then on the user of the function. Please see :ref:`advanced method support table <advanced_methods_support_table>` to see where automatic continuation is supported. ``large_file_id`` can also be passed if automatic continuation is available in order to avoid issues where multiple matchin upload sessions are matching the transfer.


Continuation of create/concantenate
===================================

:meth:`b2sdk.v1.Bucket.create_file` supports automatic continuation or manual continuation. :meth:`b2sdk.v1.Bucket.create_file_stream` supports only manual continuation for local-only inputs. The situation looks the same for :meth:`b2sdk.v1.Bucket.concatenate` and :meth:`b2sdk.v1.Bucket.concatenate_stream` (streamed version supports only manual continuation of local sources). Also :meth:`b2sdk.v1.Bucket.upload` and :meth:`b2sdk.v2.Bucket.copy` support both automatic and manual continuation.

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


Note, that this only forces start of a new large file - it is still possible to continue the process with either auto or manual modes.
