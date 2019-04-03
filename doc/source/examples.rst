========
Examples
========

Initialize API:

.. code-block:: python

    from b2sdk.account_info.sqlite_account_info import SqliteAccountInfo
    from b2sdk.api import B2Api
    from b2sdk.cache import AuthInfoCache

    info = SqliteAccountInfo()

    b2_api = B2Api(info, AuthInfoCache(info))


Account authorization
=====================

.. code-block:: python

    realm = 'staging'  # a realm to authiroze account in

    b2_api.authorize_account(realm, account_id_or_key_id, application_key)


Bucket actions
==============

Create a bucket
---------------

.. code-block:: python

    # Optionally stores bucket info, CORS rules and lifecycle rules with the bucket.
    b2_api.create_bucket(
        bucketName,  # bucket name (str)
        bucketType,  # a bucket type, could be one of the following values: "allPublic", "allPrivate" (str)
        bucket_info=bucketInfo,  # additional bucket info to store with the bucket (dict)
        cors_rules=corsRules,  # bucket CORS rules to store with the bucket (dict)
        lifecycle_rules=lifecycleRules,  # bucket lifecycle rules to store with the bucket (dict)
    )

Remove a bucket
---------------

.. code-block:: python

    bucket = b2_api.get_bucket_by_name(bucketName)
    b2_api.delete_bucket(bucket)

List buckets
-------------

.. code-block:: python

    for b in b2_api.list_buckets():
        print('%s  %-10s  %s' % (b.id_, b.type_, b.name))


Update bucket info
------------------

.. code-block:: python

    bucket = b2_api.get_bucket_by_name(bucketName)
    bucket.update(
        bucket_type=bucketType,
        bucket_info=bucketInfo,
        cors_rules=corsRules,
        lifecycle_rules=lifecycleRules,
    )

File actions
============

Upload file
-----------

.. code-block:: python

    from b2sdk.progress import make_progress_listener

    bucket = b2_api.get_bucket_by_name(bucketName)
    bucket.upload_local_file(
        local_file=localFilePath,
        file_name=b2FileName,
        content_type=contentType,
        file_infos=file_infos,
        sha1_sum=sha1,
        min_part_size=minPartSize,
        progress_listener=make_progress_listener(localFilePath, noProgress),
    )

Download file
-------------

By Id:

.. code-block:: python

    from b2sdk.progress import make_progress_listener
    from b2sdk.download_dest import DownloadDestLocalFile

    progress_listener = make_progress_listener(localFileName, noProgress)
    download_dest = DownloadDestLocalFile(localFileName)
    b2_api.download_file_by_id(fileId, download_dest, progress_listener)

    print('File name:   ', download_dest.file_name)
    print('File id:     ', download_dest.file_id)
    print('File size:   ', download_dest.content_length)
    print('Content type:', download_dest.content_type)
    print('Content sha1:', download_dest.content_sha1)

By Name:

.. code-block:: python

    bucket = b2_api.get_bucket_by_name(bucketName)
    progress_listener = make_progress_listener(localFileName, noProgress)
    download_dest = DownloadDestLocalFile(localFileName)
    bucket.download_file_by_name(b2FileName, download_dest, progress_listener)

List files
----------

.. code-block:: python

    bucket = b2_api.get_bucket_by_name(bucketName)
    response = bucket.list_file_names(startFileName, maxToShow)

    # list file versions
    response = bucket.list_file_versions(startFileName, startFileId, maxToShow)


Get file meta information
-------------------------

.. code-block:: python

    b2_api.get_file_info(fileId)


Delete file
-----------

.. code-block:: python

    file_info = b2_api.delete_file_version(fileId, file_name)


Cancel file operations
----------------------

.. code-block:: python

    bucket = b2_api.get_bucket_by_name(bucketName)
    for file_version in bucket.list_unfinished_large_files():
        bucket.cancel_large_file(file_version.file_id)


Synchronization
===============

.. code-block:: python

    from b2sdk.sync.scan_policies import ScanPoliciesManager
    from b2sdk.sync import parse_sync_folder, sync_folders

    max_workers = num_threads
    b2_api.set_thread_pool_size(max_workers)
    source = parse_sync_folder(source, b2_api)
    destination = parse_sync_folder(destination, b2_api)

    policies_manager = ScanPoliciesManager(
        exclude_dir_regexes=excludeDirRegex,
        exclude_file_regexes=excludeRegex,
        include_file_regexes=includeRegex,
        exclude_all_symlinks=excludeAllSymlinks,
    )

    sync_folders(
        source_folder=source,
        dest_folder=destination,
        args=args,
        now_millis=current_time_millis(),
        stdout=stdout,
        no_progress=noProgress,
        max_workers=max_workers,
        policies_manager=policies_manager,
        dry_run=dryRun,
        allow_empty_source=allow_empty_source
    )


Account information
===================

.. code-block:: python

    account_info = b2_api.account_info

    # Get Account ID
    accountId = account_info.get_account_id()

    # Allowed Permissions
    allowed = account_info.get_allowed()

    # Get Application Key
    applicationKey = account_info.get_application_key()

    # Get Application Key
    accountAuthToken = account_info.get_account_auth_token()

    # Get Application Key
    apiUrl = account_info.get_api_url()

    # Get Application Key
    downloadUrl = account_info.get_download_url()

