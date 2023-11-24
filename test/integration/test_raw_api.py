######################################################################
#
# File: test/integration/test_raw_api.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import io
import os
import random
import re
import sys
import time
import traceback

import pytest

from b2sdk.b2http import B2Http
from b2sdk.encryption.setting import EncryptionAlgorithm, EncryptionMode, EncryptionSetting
from b2sdk.exception import DisablingFileLockNotSupported, Unauthorized
from b2sdk.file_lock import (
    NO_RETENTION_FILE_SETTING,
    BucketRetentionSetting,
    FileRetentionSetting,
    RetentionMode,
    RetentionPeriod,
)
from b2sdk.raw_api import ALL_CAPABILITIES, REALM_URLS, B2RawHTTPApi
from b2sdk.replication.setting import ReplicationConfiguration, ReplicationRule
from b2sdk.replication.types import ReplicationStatus
from b2sdk.utils import hex_sha1_of_stream


# TODO: rewrite to separate test cases
def test_raw_api(dont_cleanup_old_buckets):
    """
    Exercise the code in B2RawHTTPApi by making each call once, just
    to make sure the parameters are passed in, and the result is
    passed back.

    The goal is to be a complete test of B2RawHTTPApi, so the tests for
    the rest of the code can use the simulator.

    Prints to stdout if things go wrong.

    :return: 0 on success, non-zero on failure
    """
    application_key_id = os.environ.get('B2_TEST_APPLICATION_KEY_ID')
    if application_key_id is None:
        pytest.fail('B2_TEST_APPLICATION_KEY_ID is not set.')

    application_key = os.environ.get('B2_TEST_APPLICATION_KEY')
    if application_key is None:
        pytest.fail('B2_TEST_APPLICATION_KEY is not set.')

    print()

    try:
        raw_api = B2RawHTTPApi(B2Http())
        raw_api_test_helper(raw_api, not dont_cleanup_old_buckets)
    except Exception:
        traceback.print_exc(file=sys.stdout)
        pytest.fail('test_raw_api failed')


def authorize_raw_api(raw_api):
    application_key_id = os.environ.get('B2_TEST_APPLICATION_KEY_ID')
    if application_key_id is None:
        print('B2_TEST_APPLICATION_KEY_ID is not set.', file=sys.stderr)
        sys.exit(1)

    application_key = os.environ.get('B2_TEST_APPLICATION_KEY')
    if application_key is None:
        print('B2_TEST_APPLICATION_KEY is not set.', file=sys.stderr)
        sys.exit(1)

    realm = os.environ.get('B2_TEST_ENVIRONMENT', 'production')
    realm_url = REALM_URLS.get(realm, realm)
    auth_dict = raw_api.authorize_account(realm_url, application_key_id, application_key)
    return auth_dict


def raw_api_test_helper(raw_api, should_cleanup_old_buckets):
    """
    Try each of the calls to the raw api.  Raise an
    exception if anything goes wrong.

    This uses a Backblaze account that is just for this test.
    The account uses the free level of service, which should
    be enough to run this test a reasonable number of times
    each day.  If somebody abuses the account for other things,
    this test will break and we'll have to do something about
    it.
    """
    # b2_authorize_account
    print('b2_authorize_account')
    auth_dict = authorize_raw_api(raw_api)
    missing_capabilities = set(ALL_CAPABILITIES) - {'readBuckets', 'listAllBucketNames'
                                                   } - set(auth_dict['allowed']['capabilities'])
    assert not missing_capabilities, 'it appears that the raw_api integration test is being run with a non-full key. Missing capabilities: {}'.format(
        missing_capabilities,
    )

    account_id = auth_dict['accountId']
    account_auth_token = auth_dict['authorizationToken']
    api_url = auth_dict['apiUrl']
    download_url = auth_dict['downloadUrl']

    # b2_create_key
    print('b2_create_key')
    key_dict = raw_api.create_key(
        api_url,
        account_auth_token,
        account_id,
        ['readFiles'],
        'testKey',
        None,
        None,
        None,
    )

    # b2_list_keys
    print('b2_list_keys')
    raw_api.list_keys(api_url, account_auth_token, account_id, 10)

    # b2_delete_key
    print('b2_delete_key')
    raw_api.delete_key(api_url, account_auth_token, key_dict['applicationKeyId'])

    # b2_create_bucket, with a unique bucket name
    # Include the account ID in the bucket name to be
    # sure it doesn't collide with bucket names from
    # other accounts.
    print('b2_create_bucket')
    bucket_name = 'test-raw-api-%s-%d-%d' % (
        account_id, int(time.time()), random.randint(1000, 9999)
    )

    # very verbose http debug
    #import http.client; http.client.HTTPConnection.debuglevel = 1

    bucket_dict = raw_api.create_bucket(
        api_url,
        account_auth_token,
        account_id,
        bucket_name,
        'allPublic',
        is_file_lock_enabled=True,
    )
    bucket_id = bucket_dict['bucketId']
    first_bucket_revision = bucket_dict['revision']

    #################################
    print('b2 / replication')

    # 1) create source key (read permissions)
    replication_source_key_dict = raw_api.create_key(
        api_url,
        account_auth_token,
        account_id,
        [
            'listBuckets',
            'listFiles',
            'readFiles',
            'writeFiles',  # Pawel @ 2022-06-21: adding this to make tests pass with a weird server validator
        ],
        'testReplicationSourceKey',
        None,
        None,
        None,
    )
    replication_source_key = replication_source_key_dict['applicationKeyId']

    # 2) create source bucket with replication to destination - existing bucket
    try:
        # in order to test replication, we need to create a second bucket
        replication_source_bucket_name = 'test-raw-api-%s-%d-%d' % (
            account_id, int(time.time()), random.randint(1000, 9999)
        )
        replication_source_bucket_dict = raw_api.create_bucket(
            api_url,
            account_auth_token,
            account_id,
            replication_source_bucket_name,
            'allPublic',
            is_file_lock_enabled=True,
            replication=ReplicationConfiguration(
                rules=[
                    ReplicationRule(
                        destination_bucket_id=bucket_id,
                        include_existing_files=True,
                        name='test-rule',
                    ),
                ],
                source_key_id=replication_source_key,
            ),
        )
        assert 'replicationConfiguration' in replication_source_bucket_dict
        assert replication_source_bucket_dict['replicationConfiguration'] == {
            'isClientAuthorizedToRead': True,
            'value':
                {
                    "asReplicationSource":
                        {
                            "replicationRules":
                                [
                                    {
                                        "destinationBucketId": bucket_id,
                                        "fileNamePrefix": "",
                                        "includeExistingFiles": True,
                                        "isEnabled": True,
                                        "priority": 128,
                                        "replicationRuleName": "test-rule"
                                    },
                                ],
                            "sourceApplicationKeyId": replication_source_key,
                        },
                    "asReplicationDestination": None,
                },
        }

        # 3) upload test file and check replication status
        upload_url_dict = raw_api.get_upload_url(
            api_url,
            account_auth_token,
            replication_source_bucket_dict['bucketId'],
        )
        file_contents = b'hello world'
        file_dict = raw_api.upload_file(
            upload_url_dict['uploadUrl'],
            upload_url_dict['authorizationToken'],
            'test.txt',
            len(file_contents),
            'text/plain',
            hex_sha1_of_stream(io.BytesIO(file_contents), len(file_contents)),
            {'color': 'blue'},
            io.BytesIO(file_contents),
        )

        assert ReplicationStatus[file_dict['replicationStatus'].upper()
                                ] == ReplicationStatus.PENDING

    finally:
        raw_api.delete_key(api_url, account_auth_token, replication_source_key)

    # 4) create destination key (write permissions)
    replication_destination_key_dict = raw_api.create_key(
        api_url,
        account_auth_token,
        account_id,
        ['listBuckets', 'listFiles', 'writeFiles'],
        'testReplicationDestinationKey',
        None,
        None,
        None,
    )
    replication_destination_key = replication_destination_key_dict['applicationKeyId']

    # 5) update destination bucket to receive updates
    try:
        bucket_dict = raw_api.update_bucket(
            api_url,
            account_auth_token,
            account_id,
            bucket_id,
            'allPublic',
            replication=ReplicationConfiguration(
                source_to_destination_key_mapping={
                    replication_source_key: replication_destination_key,
                },
            ),
        )
        assert bucket_dict['replicationConfiguration'] == {
            'isClientAuthorizedToRead': True,
            'value':
                {
                    'asReplicationDestination':
                        {
                            'sourceToDestinationKeyMapping':
                                {
                                    replication_source_key: replication_destination_key,
                                },
                        },
                    'asReplicationSource': None,
                },
        }
    finally:
        raw_api.delete_key(
            api_url,
            account_auth_token,
            replication_destination_key_dict['applicationKeyId'],
        )

    # 6) cleanup: disable replication for destination and remove source
    bucket_dict = raw_api.update_bucket(
        api_url,
        account_auth_token,
        account_id,
        bucket_id,
        'allPublic',
        replication=ReplicationConfiguration(),
    )
    assert bucket_dict['replicationConfiguration'] == {
        'isClientAuthorizedToRead': True,
        'value': None,
    }

    _clean_and_delete_bucket(
        raw_api,
        api_url,
        account_auth_token,
        account_id,
        replication_source_bucket_dict['bucketId'],
    )

    #################
    print('b2_update_bucket')
    sse_b2_aes = EncryptionSetting(
        mode=EncryptionMode.SSE_B2,
        algorithm=EncryptionAlgorithm.AES256,
    )
    sse_none = EncryptionSetting(mode=EncryptionMode.NONE)
    for encryption_setting, default_retention in [
        (
            sse_none,
            BucketRetentionSetting(mode=RetentionMode.GOVERNANCE, period=RetentionPeriod(days=1))
        ),
        (sse_b2_aes, None),
        (sse_b2_aes, BucketRetentionSetting(RetentionMode.NONE)),
    ]:
        bucket_dict = raw_api.update_bucket(
            api_url,
            account_auth_token,
            account_id,
            bucket_id,
            'allPublic',
            default_server_side_encryption=encryption_setting,
            default_retention=default_retention,
        )

    # b2_list_buckets
    print('b2_list_buckets')
    bucket_list_dict = raw_api.list_buckets(api_url, account_auth_token, account_id)
    #print(bucket_list_dict)

    # b2_get_upload_url
    print('b2_get_upload_url')
    upload_url_dict = raw_api.get_upload_url(api_url, account_auth_token, bucket_id)
    upload_url = upload_url_dict['uploadUrl']
    upload_auth_token = upload_url_dict['authorizationToken']

    # b2_upload_file
    print('b2_upload_file')
    file_name = 'test.txt'
    file_contents = b'hello world'
    file_sha1 = hex_sha1_of_stream(io.BytesIO(file_contents), len(file_contents))
    file_dict = raw_api.upload_file(
        upload_url,
        upload_auth_token,
        file_name,
        len(file_contents),
        'text/plain',
        file_sha1,
        {
            'color': 'blue',
            'b2-cache-control': 'private, max-age=2222'
        },
        io.BytesIO(file_contents),
        server_side_encryption=sse_b2_aes,
        #custom_upload_timestamp=12345,
        file_retention=FileRetentionSetting(
            RetentionMode.GOVERNANCE,
            int(time.time() + 100) * 1000,
        )
    )

    file_id = file_dict['fileId']

    # b2_list_file_versions
    print('b2_list_file_versions')
    list_versions_dict = raw_api.list_file_versions(api_url, account_auth_token, bucket_id)
    assert [file_name] == [f_dict['fileName'] for f_dict in list_versions_dict['files']]
    assert ['private, max-age=2222'] == [
        f_dict['fileInfo']['b2-cache-control'] for f_dict in list_versions_dict['files']
    ]

    # b2_download_file_by_id with auth
    print('b2_download_file_by_id (auth)')
    url = raw_api.get_download_url_by_id(download_url, file_id)
    with raw_api.download_file_from_url(account_auth_token, url) as response:
        data = next(response.iter_content(chunk_size=len(file_contents)))
        assert data == file_contents, data

    # b2_download_file_by_id no auth
    print('b2_download_file_by_id (no auth)')
    url = raw_api.get_download_url_by_id(download_url, file_id)
    with raw_api.download_file_from_url(None, url) as response:
        data = next(response.iter_content(chunk_size=len(file_contents)))
        assert data == file_contents, data

    # b2_download_file_by_name with auth
    print('b2_download_file_by_name (auth)')
    url = raw_api.get_download_url_by_name(download_url, bucket_name, file_name)
    with raw_api.download_file_from_url(account_auth_token, url) as response:
        data = next(response.iter_content(chunk_size=len(file_contents)))
        assert data == file_contents, data

    # b2_download_file_by_name no auth
    print('b2_download_file_by_name (no auth)')
    url = raw_api.get_download_url_by_name(download_url, bucket_name, file_name)
    with raw_api.download_file_from_url(None, url) as response:
        data = next(response.iter_content(chunk_size=len(file_contents)))
        assert data == file_contents, data

    # b2_get_download_authorization
    print('b2_get_download_authorization')
    download_auth = raw_api.get_download_authorization(
        api_url, account_auth_token, bucket_id, file_name[:-2], 12345
    )
    download_auth_token = download_auth['authorizationToken']

    # b2_download_file_by_name with download auth
    print('b2_download_file_by_name (download auth)')
    url = raw_api.get_download_url_by_name(download_url, bucket_name, file_name)
    with raw_api.download_file_from_url(download_auth_token, url) as response:
        data = next(response.iter_content(chunk_size=len(file_contents)))
        assert data == file_contents, data

    # b2_list_file_names
    print('b2_list_file_names')
    list_names_dict = raw_api.list_file_names(api_url, account_auth_token, bucket_id)
    assert [file_name] == [f_dict['fileName'] for f_dict in list_names_dict['files']]

    # b2_list_file_names (start, count)
    print('b2_list_file_names (start, count)')
    list_names_dict = raw_api.list_file_names(
        api_url, account_auth_token, bucket_id, start_file_name=file_name, max_file_count=5
    )
    assert [file_name] == [f_dict['fileName'] for f_dict in list_names_dict['files']]

    # b2_copy_file
    print('b2_copy_file')
    copy_file_name = 'test_copy.txt'
    raw_api.copy_file(api_url, account_auth_token, file_id, copy_file_name)

    # b2_get_file_info_by_id
    print('b2_get_file_info_by_id')
    file_info_dict = raw_api.get_file_info_by_id(api_url, account_auth_token, file_id)
    assert file_info_dict['fileName'] == file_name

    # b2_get_file_info_by_name
    print('b2_get_file_info_by_name (no auth)')
    info_headers = raw_api.get_file_info_by_name(download_url, None, bucket_name, file_name)
    assert info_headers['x-bz-file-id'] == file_id

    # b2_get_file_info_by_name
    print('b2_get_file_info_by_name (auth)')
    info_headers = raw_api.get_file_info_by_name(
        download_url, account_auth_token, bucket_name, file_name
    )
    assert info_headers['x-bz-file-id'] == file_id

    # b2_get_file_info_by_name
    print('b2_get_file_info_by_name (download auth)')
    info_headers = raw_api.get_file_info_by_name(
        download_url, download_auth_token, bucket_name, file_name
    )
    assert info_headers['x-bz-file-id'] == file_id

    # b2_hide_file
    print('b2_hide_file')
    raw_api.hide_file(api_url, account_auth_token, bucket_id, file_name)

    # b2_start_large_file
    print('b2_start_large_file')
    file_info = {'color': 'red'}
    large_info = raw_api.start_large_file(
        api_url,
        account_auth_token,
        bucket_id,
        file_name,
        'text/plain',
        file_info,
        server_side_encryption=sse_b2_aes,
    )
    large_file_id = large_info['fileId']

    # b2_get_upload_part_url
    print('b2_get_upload_part_url')
    upload_part_dict = raw_api.get_upload_part_url(api_url, account_auth_token, large_file_id)
    upload_part_url = upload_part_dict['uploadUrl']
    upload_path_auth = upload_part_dict['authorizationToken']

    # b2_upload_part
    print('b2_upload_part')
    part_contents = b'hello part'
    part_sha1 = hex_sha1_of_stream(io.BytesIO(part_contents), len(part_contents))
    raw_api.upload_part(
        upload_part_url, upload_path_auth, 1, len(part_contents), part_sha1,
        io.BytesIO(part_contents)
    )

    # b2_copy_part
    print('b2_copy_part')
    raw_api.copy_part(api_url, account_auth_token, file_id, large_file_id, 2, (0, 5))

    # b2_list_parts
    print('b2_list_parts')
    parts_response = raw_api.list_parts(api_url, account_auth_token, large_file_id, 1, 100)
    assert [1, 2] == [part['partNumber'] for part in parts_response['parts']]

    # b2_list_unfinished_large_files
    unfinished_list = raw_api.list_unfinished_large_files(api_url, account_auth_token, bucket_id)
    assert [file_name] == [f_dict['fileName'] for f_dict in unfinished_list['files']]
    assert file_info == unfinished_list['files'][0]['fileInfo']

    # b2_finish_large_file
    print('b2_finish_large_file')
    try:
        raw_api.finish_large_file(api_url, account_auth_token, large_file_id, [part_sha1])
        raise Exception('finish should have failed')
    except Exception as e:
        assert 'large files must have at least 2 parts' in str(e)
    # TODO: make another attempt to finish but this time successfully

    # b2_update_bucket
    print('b2_update_bucket')
    updated_bucket = raw_api.update_bucket(
        api_url,
        account_auth_token,
        account_id,
        bucket_id,
        'allPrivate',
        bucket_info={'color': 'blue'},
        default_retention=BucketRetentionSetting(
            mode=RetentionMode.GOVERNANCE, period=RetentionPeriod(days=1)
        ),
        is_file_lock_enabled=True,
    )
    assert first_bucket_revision < updated_bucket['revision']

    # NOTE: this update_bucket call is only here to be able to find out the error code returned by
    # the server if an attempt is made to disable file lock.  It has to be done here since the CLI
    # by design does not allow disabling file lock at all (i.e. there is no --fileLockEnabled=false
    # option or anything equivalent to that).
    with pytest.raises(DisablingFileLockNotSupported):
        raw_api.update_bucket(
            api_url,
            account_auth_token,
            account_id,
            bucket_id,
            'allPrivate',
            is_file_lock_enabled=False,
        )

    # b2_delete_file_version
    print('b2_delete_file_version')

    with pytest.raises(Unauthorized):
        raw_api.delete_file_version(api_url, account_auth_token, file_id, file_name)
    raw_api.delete_file_version(api_url, account_auth_token, file_id, file_name, True)

    # Clean up this test.
    _clean_and_delete_bucket(raw_api, api_url, account_auth_token, account_id, bucket_id)

    if should_cleanup_old_buckets:
        # Clean up from old tests. Empty and delete any buckets more than an hour old.
        _cleanup_old_buckets(raw_api, auth_dict, bucket_list_dict)


def cleanup_old_buckets():
    raw_api = B2RawHTTPApi(B2Http())
    auth_dict = authorize_raw_api(raw_api)
    bucket_list_dict = raw_api.list_buckets(
        auth_dict['apiUrl'], auth_dict['authorizationToken'], auth_dict['accountId']
    )
    _cleanup_old_buckets(raw_api, auth_dict, bucket_list_dict)


def _cleanup_old_buckets(raw_api, auth_dict, bucket_list_dict):
    for bucket_dict in bucket_list_dict['buckets']:
        bucket_id = bucket_dict['bucketId']
        bucket_name = bucket_dict['bucketName']
        if _should_delete_bucket(bucket_name):
            print('cleaning up old bucket: ' + bucket_name)
            _clean_and_delete_bucket(
                raw_api,
                auth_dict['apiUrl'],
                auth_dict['authorizationToken'],
                auth_dict['accountId'],
                bucket_id,
            )


def _clean_and_delete_bucket(raw_api, api_url, account_auth_token, account_id, bucket_id):
    # Delete the files. This test never creates more than a few files,
    # so one call to list_file_versions should get them all.
    versions_dict = raw_api.list_file_versions(api_url, account_auth_token, bucket_id)
    for version_dict in versions_dict['files']:
        file_id = version_dict['fileId']
        file_name = version_dict['fileName']
        action = version_dict['action']
        if action in ['hide', 'upload']:
            print('b2_delete_file', file_name, action)
            if action == 'upload' and version_dict[
                'fileRetention'] and version_dict['fileRetention']['value']['mode'] is not None:
                raw_api.update_file_retention(
                    api_url,
                    account_auth_token,
                    file_id,
                    file_name,
                    NO_RETENTION_FILE_SETTING,
                    bypass_governance=True
                )
            raw_api.delete_file_version(api_url, account_auth_token, file_id, file_name)
        else:
            print('b2_cancel_large_file', file_name)
            raw_api.cancel_large_file(api_url, account_auth_token, file_id)

    # Delete the bucket
    print('b2_delete_bucket', bucket_id)
    raw_api.delete_bucket(api_url, account_auth_token, account_id, bucket_id)


def _should_delete_bucket(bucket_name):
    # Bucket names for this test look like: c7b22d0b0ad7-1460060364-5670
    # Other buckets should not be deleted.
    match = re.match(r'^test-raw-api-[a-f0-9]+-([0-9]+)-([0-9]+)', bucket_name)
    if match is None:
        return False

    # Is it more than an hour old?
    bucket_time = int(match.group(1))
    now = time.time()
    return bucket_time + 3600 <= now
