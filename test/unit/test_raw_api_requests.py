######################################################################
#
# File: test/unit/test_raw_api_requests.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Request-level contracts, replacing only transport and forbidding real HTTP.

Expected payloads are literal Native API examples, not simulator output:
https://www.backblaze.com/apidocs/b2-copy-file
https://www.backblaze.com/apidocs/b2-start-large-file
https://www.backblaze.com/apidocs/b2-upload-file
https://www.backblaze.com/apidocs/b2-update-file-retention
"""

from __future__ import annotations

import io
from unittest.mock import Mock

import pytest
from apiver_deps import (
    UNKNOWN_KEY_ID,
    B2Http,
    B2RawHTTPApi,
    BucketRetentionSetting,
    EncryptionAlgorithm,
    EncryptionKey,
    EncryptionMode,
    EncryptionSetting,
    FileRetentionSetting,
    LegalHold,
    MetadataDirectiveMode,
    RetentionMode,
    RetentionPeriod,
)
from apiver_deps_exception import (
    AccessDenied,
    InvalidMetadataDirective,
    RetentionWriteError,
    SSECKeyError,
    WrongEncryptionModeForBucketDefault,
    WrongEncryptionSettingForFileWrite,
)

# These raw-transport exceptions have no public interface export.
from b2sdk._internal.exception import FileOrBucketNotFound, ResourceNotFound

API = 'https://api.example.invalid'
TOKEN = 'synthetic-account-token'
UNTIL = 1900000000123
KEY = bytes(range(32))
KEY_B64 = 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8='
KEY_MD5 = 'tP/LI3N87DFaSk0aoqYgzg=='
SSE_JSON = {
    'mode': 'SSE-C',
    'algorithm': 'AES256',
    'customerKey': KEY_B64,
    'customerKeyMd5': KEY_MD5,
}
SSE_HEADERS = {
    'X-Bz-Server-Side-Encryption-Customer-Algorithm': 'AES256',
    'X-Bz-Server-Side-Encryption-Customer-Key': KEY_B64,
    'X-Bz-Server-Side-Encryption-Customer-Key-Md5': KEY_MD5,
}


@pytest.fixture(autouse=True)
def forbid_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('request unit tests must not perform real HTTP')

    monkeypatch.setattr('requests.sessions.Session.request', forbidden)


@pytest.fixture
def raw_api():
    return B2RawHTTPApi(Mock(spec=B2Http))


@pytest.fixture
def api_version(apiver_int):
    # Interface v3 uses Native API v4; the compatibility interfaces use v3.
    return 'v4' if apiver_int == 3 else 'v3'


def customer_setting(key_id='public-key-id', secret=KEY):
    return EncryptionSetting(
        EncryptionMode.SSE_C, EncryptionAlgorithm.AES256, EncryptionKey(secret, key_id)
    )


def assert_json_request(raw_api, api_version, endpoint, payload):
    raw_api.b2_http.post_json_return_json.assert_called_once_with(
        f'{API}/b2api/{api_version}/{endpoint}',
        {'Authorization': TOKEN},
        payload,
    )


# Each row pins an externally named endpoint and wire fields, including pagination.
@pytest.mark.parametrize(
    'method,args,kwargs,endpoint,payload',
    [
        ('cancel_large_file', ('file-id',), {}, 'b2_cancel_large_file', {'fileId': 'file-id'}),
        (
            'delete_bucket',
            ('account-id', 'bucket-id'),
            {},
            'b2_delete_bucket',
            {'accountId': 'account-id', 'bucketId': 'bucket-id'},
        ),
        (
            'delete_file_version',
            ('file-id', 'dir/a.txt'),
            {'bypass_governance': True},
            'b2_delete_file_version',
            {'fileId': 'file-id', 'fileName': 'dir/a.txt', 'bypassGovernance': True},
        ),
        ('delete_key', ('key-id',), {}, 'b2_delete_key', {'applicationKeyId': 'key-id'}),
        (
            'finish_large_file',
            ('file-id', ['hash1', 'hash2']),
            {},
            'b2_finish_large_file',
            {'fileId': 'file-id', 'partSha1Array': ['hash1', 'hash2']},
        ),
        (
            'get_download_authorization',
            ('bucket-id', 'dir/', 60),
            {},
            'b2_get_download_authorization',
            {'bucketId': 'bucket-id', 'fileNamePrefix': 'dir/', 'validDurationInSeconds': 60},
        ),
        ('get_file_info_by_id', ('file-id',), {}, 'b2_get_file_info', {'fileId': 'file-id'}),
        ('get_upload_url', ('bucket-id',), {}, 'b2_get_upload_url', {'bucketId': 'bucket-id'}),
        ('get_upload_part_url', ('file-id',), {}, 'b2_get_upload_part_url', {'fileId': 'file-id'}),
        (
            'hide_file',
            ('bucket-id', 'dir/a.txt'),
            {},
            'b2_hide_file',
            {'bucketId': 'bucket-id', 'fileName': 'dir/a.txt'},
        ),
        (
            'list_buckets',
            ('account-id',),
            {'bucket_id': 'bucket-id'},
            'b2_list_buckets',
            {
                'accountId': 'account-id',
                'bucketId': 'bucket-id',
                'bucketName': None,
                'bucketTypes': ['all'],
            },
        ),
        (
            'list_file_names',
            ('bucket-id',),
            {'start_file_name': 'dir/a.txt', 'max_file_count': 2, 'prefix': 'dir/'},
            'b2_list_file_names',
            {
                'bucketId': 'bucket-id',
                'startFileName': 'dir/a.txt',
                'maxFileCount': 2,
                'prefix': 'dir/',
            },
        ),
        (
            'list_file_versions',
            ('bucket-id',),
            {
                'start_file_name': 'dir/a.txt',
                'start_file_id': 'file-id',
                'max_file_count': 2,
                'prefix': 'dir/',
            },
            'b2_list_file_versions',
            {
                'bucketId': 'bucket-id',
                'startFileName': 'dir/a.txt',
                'startFileId': 'file-id',
                'maxFileCount': 2,
                'prefix': 'dir/',
            },
        ),
        (
            'list_keys',
            ('account-id',),
            {'max_key_count': 2, 'start_application_key_id': 'key-id'},
            'b2_list_keys',
            {'accountId': 'account-id', 'maxKeyCount': 2, 'startApplicationKeyId': 'key-id'},
        ),
        (
            'list_parts',
            ('file-id', 2, 3),
            {},
            'b2_list_parts',
            {'fileId': 'file-id', 'startPartNumber': 2, 'maxPartCount': 3},
        ),
        (
            'list_unfinished_large_files',
            ('bucket-id',),
            {'start_file_id': 'file-id', 'max_file_count': 2, 'prefix': 'dir/'},
            'b2_list_unfinished_large_files',
            {
                'bucketId': 'bucket-id',
                'startFileId': 'file-id',
                'maxFileCount': 2,
                'namePrefix': 'dir/',
            },
        ),
    ],
)
def test_json_endpoint_and_payload(raw_api, api_version, method, args, kwargs, endpoint, payload):
    getattr(raw_api, method)(API, TOKEN, *args, **kwargs)
    assert_json_request(raw_api, api_version, endpoint, payload)


def test_authorization_uses_basic_header_not_body_credentials(raw_api, api_version):
    raw_api.authorize_account(API, 'id', 'key')
    raw_api.b2_http.post_json_return_json.assert_called_once_with(
        f'{API}/b2api/{api_version}/b2_authorize_account',
        {'Authorization': 'Basic aWQ6a2V5'},
        {},
    )


@pytest.mark.parametrize('operation', ['create', 'update'])
def test_bucket_settings_preserve_explicit_false_and_empty_values(raw_api, api_version, operation):
    options = dict(
        bucket_info={},
        cors_rules=[],
        lifecycle_rules=[],
        default_server_side_encryption=EncryptionSetting(
            EncryptionMode.SSE_B2, EncryptionAlgorithm.AES256
        ),
        is_file_lock_enabled=False,
    )
    payload = {
        'accountId': 'account-id',
        'bucketInfo': {},
        'corsRules': [],
        'lifecycleRules': [],
        'defaultServerSideEncryption': {'mode': 'SSE-B2', 'algorithm': 'AES256'},
        'fileLockEnabled': False,
    }
    if operation == 'create':
        raw_api.create_bucket(API, TOKEN, 'account-id', 'bucket-name', 'allPrivate', **options)
        payload.update(bucketName='bucket-name', bucketType='allPrivate')
    else:
        raw_api.update_bucket(
            API,
            TOKEN,
            'account-id',
            'bucket-id',
            default_retention=BucketRetentionSetting(
                RetentionMode.GOVERNANCE, RetentionPeriod(days=7)
            ),
            if_revision_is=0,
            **options,
        )
        payload.update(
            bucketId='bucket-id',
            ifRevisionIs=0,
            defaultRetention={'mode': 'governance', 'period': {'duration': 7, 'unit': 'days'}},
        )
    assert_json_request(raw_api, api_version, 'b2_' + operation + '_bucket', payload)


@pytest.mark.parametrize('operation', ['create', 'update'])
def test_customer_encryption_cannot_be_a_bucket_default(raw_api, operation):
    with pytest.raises(WrongEncryptionModeForBucketDefault):
        if operation == 'create':
            raw_api.create_bucket(
                API,
                TOKEN,
                'account-id',
                'bucket-name',
                'allPrivate',
                default_server_side_encryption=customer_setting(),
            )
        else:
            raw_api.update_bucket(
                API,
                TOKEN,
                'account-id',
                'bucket-id',
                default_server_side_encryption=customer_setting(),
            )
    assert raw_api.b2_http.mock_calls == []


def test_start_large_file_sends_encryption_retention_and_legal_hold(raw_api, api_version):
    raw_api.start_large_file(
        API,
        TOKEN,
        'bucket-id',
        'dir/a.txt',
        'text/plain',
        {},
        server_side_encryption=customer_setting(),
        file_retention=FileRetentionSetting(RetentionMode.GOVERNANCE, UNTIL),
        legal_hold=LegalHold.ON,
    )
    assert_json_request(
        raw_api,
        api_version,
        'b2_start_large_file',
        {
            'bucketId': 'bucket-id',
            'fileName': 'dir/a.txt',
            'contentType': 'text/plain',
            'fileInfo': {'sse_c_key_id': 'public-key-id'},
            'serverSideEncryption': SSE_JSON,
            'fileRetention': {'mode': 'governance', 'retainUntilTimestamp': UNTIL},
            'legalHold': 'on',
        },
    )


@pytest.mark.parametrize('directive', [MetadataDirectiveMode.COPY, MetadataDirectiveMode.REPLACE])
def test_copy_serializes_encryption_range_and_lock_settings(raw_api, api_version, directive):
    kwargs = (
        {}
        if directive == MetadataDirectiveMode.COPY
        else {'content_type': 'text/plain', 'file_info': {'color': 'blue'}}
    )
    raw_api.copy_file(
        API,
        TOKEN,
        'source-id',
        'dir/copied.txt',
        bytes_range=(0, 0),
        metadata_directive=directive,
        destination_bucket_id='destination-id',
        destination_server_side_encryption=customer_setting('destination'),
        source_server_side_encryption=customer_setting('source'),
        file_retention=FileRetentionSetting(RetentionMode.COMPLIANCE, UNTIL),
        legal_hold=LegalHold.OFF,
        **kwargs,
    )
    payload = {
        'sourceFileId': 'source-id',
        'fileName': 'dir/copied.txt',
        'range': 'bytes=0-0',
        'metadataDirective': directive.name,
        'destinationBucketId': 'destination-id',
        'destinationServerSideEncryption': SSE_JSON,
        'sourceServerSideEncryption': SSE_JSON,
        'fileRetention': {'mode': 'compliance', 'retainUntilTimestamp': UNTIL},
        'legalHold': 'off',
    }
    if directive == MetadataDirectiveMode.REPLACE:
        payload.update(contentType='text/plain', fileInfo={'color': 'blue'})
    assert_json_request(raw_api, api_version, 'b2_copy_file', payload)


@pytest.mark.parametrize(
    'directive,kwargs',
    [
        (MetadataDirectiveMode.COPY, {'content_type': 'text/plain'}),
        (MetadataDirectiveMode.COPY, {'file_info': {}}),
        (MetadataDirectiveMode.REPLACE, {}),
    ],
)
def test_copy_metadata_conflicts_are_rejected_before_transport(raw_api, directive, kwargs):
    with pytest.raises(InvalidMetadataDirective):
        raw_api.copy_file(
            API, TOKEN, 'source-id', 'copied.txt', metadata_directive=directive, **kwargs
        )
    assert raw_api.b2_http.mock_calls == []


def test_copy_part_preserves_range_and_both_customer_keys(raw_api, api_version):
    raw_api.copy_part(
        API,
        TOKEN,
        'source-id',
        'large-id',
        2,
        bytes_range=(4, 9),
        destination_server_side_encryption=customer_setting('destination'),
        source_server_side_encryption=customer_setting('source'),
    )
    assert_json_request(
        raw_api,
        api_version,
        'b2_copy_part',
        {
            'sourceFileId': 'source-id',
            'largeFileId': 'large-id',
            'partNumber': 2,
            'range': 'bytes=4-9',
            'destinationServerSideEncryption': SSE_JSON,
            'sourceServerSideEncryption': SSE_JSON,
        },
    )


@pytest.mark.parametrize('operation', ['upload_file', 'upload_part'])
@pytest.mark.parametrize('mode', [EncryptionMode.SSE_B2, EncryptionMode.SSE_C])
def test_upload_preserves_body_and_exact_headers(raw_api, operation, mode):
    data = io.BytesIO(b'body')
    encryption = (
        customer_setting()
        if mode == EncryptionMode.SSE_C
        else EncryptionSetting(mode, EncryptionAlgorithm.AES256)
    )
    expected = {
        'Authorization': 'upload-token',
        'Content-Length': '4',
        'X-Bz-Content-Sha1': 'test-sha1',
    }
    if mode == EncryptionMode.SSE_C:
        expected.update(SSE_HEADERS)
        expected['X-Bz-Info-sse_c_key_id'] = 'public-key-id'
    else:
        expected['X-Bz-Server-Side-Encryption'] = 'AES256'
    if operation == 'upload_file':
        raw_api.upload_file(
            'https://upload.example.invalid',
            'upload-token',
            'dir/Δ +.txt',
            4,
            'text/plain',
            'test-sha1',
            {'note': 'Δ +'},
            data,
            server_side_encryption=encryption,
            file_retention=FileRetentionSetting(RetentionMode.GOVERNANCE, UNTIL),
            legal_hold=LegalHold.ON,
            custom_upload_timestamp=0,
        )
        expected.update(
            {
                'X-Bz-File-Name': 'dir/%CE%94%20%2B.txt',
                'Content-Type': 'text/plain',
                'X-Bz-Info-note': '%CE%94%20%2B',
                'X-Bz-File-Legal-Hold': 'on',
                'X-Bz-File-Retention-Mode': 'governance',
                'X-Bz-File-Retention-Retain-Until-Timestamp': str(UNTIL),
                'X-Bz-Custom-Upload-Timestamp': '0',
            }
        )
    else:
        raw_api.upload_part(
            'https://upload.example.invalid',
            'upload-token',
            2,
            4,
            'test-sha1',
            data,
            server_side_encryption=encryption,
        )
        expected['X-Bz-Part-Number'] = '2'
    raw_api.b2_http.post_content_return_json.assert_called_once_with(
        'https://upload.example.invalid',
        expected,
        data,
        try_count=1,
    )
    assert data.tell() == 0


@pytest.mark.parametrize('token', [None, TOKEN])
@pytest.mark.parametrize('mode', [EncryptionMode.NONE, EncryptionMode.SSE_B2, EncryptionMode.SSE_C])
def test_download_authorization_range_and_encryption_headers(raw_api, token, mode):
    setting = (
        EncryptionSetting(mode)
        if mode == EncryptionMode.NONE
        else customer_setting()
        if mode == EncryptionMode.SSE_C
        else EncryptionSetting(mode, EncryptionAlgorithm.AES256)
    )
    raw_api.download_file_from_url(
        token, 'https://download.example.invalid/file', range_=(0, 0), encryption=setting
    )
    headers = {'Range': 'bytes=0-0'}
    if token is not None:
        headers['Authorization'] = token
    if mode == EncryptionMode.SSE_C:
        headers.update(SSE_HEADERS)
    elif mode == EncryptionMode.SSE_B2:
        headers['X-Bz-Server-Side-Encryption'] = 'AES256'
    raw_api.b2_http.get_content.assert_called_once_with(
        'https://download.example.invalid/file', headers
    )


@pytest.mark.parametrize('operation', ['download', 'copy', 'copy_part'])
@pytest.mark.parametrize('range_', [(-1, 2), (3, 2), (0,)])
def test_invalid_range_never_reaches_transport(raw_api, operation, range_):
    with pytest.raises(AssertionError):
        if operation == 'download':
            raw_api.download_file_from_url(TOKEN, API, range_=range_)
        elif operation == 'copy':
            raw_api.copy_file(API, TOKEN, 'source-id', 'copy.txt', bytes_range=range_)
        else:
            raw_api.copy_part(API, TOKEN, 'source-id', 'large-id', 1, bytes_range=range_)
    assert raw_api.b2_http.mock_calls == []


@pytest.mark.parametrize('bypass', [False, True])
def test_retention_update_preserves_explicit_governance_bypass(raw_api, api_version, bypass):
    raw_api.update_file_retention(
        API,
        TOKEN,
        'file-id',
        'dir/a.txt',
        FileRetentionSetting(RetentionMode.GOVERNANCE, UNTIL),
        bypass_governance=bypass,
    )
    assert_json_request(
        raw_api,
        api_version,
        'b2_update_file_retention',
        {
            'fileId': 'file-id',
            'fileName': 'dir/a.txt',
            'bypassGovernance': bypass,
            'fileRetention': {'mode': 'governance', 'retainUntilTimestamp': UNTIL},
        },
    )


@pytest.mark.parametrize('hold,wire', [(LegalHold.ON, 'on'), (LegalHold.OFF, 'off')])
def test_legal_hold_update_uses_wire_value(raw_api, api_version, hold, wire):
    raw_api.update_file_legal_hold(API, TOKEN, 'file-id', 'dir/a.txt', hold)
    assert_json_request(
        raw_api,
        api_version,
        'b2_update_file_legal_hold',
        {
            'fileId': 'file-id',
            'fileName': 'dir/a.txt',
            'legalHold': wire,
        },
    )


@pytest.mark.parametrize(
    'operation', ['start', 'copy', 'upload', 'retention_update', 'hold_update', 'bucket_update']
)
def test_unknown_lock_setting_is_rejected_before_transport(raw_api, operation):
    unknown = FileRetentionSetting(RetentionMode.UNKNOWN)
    with pytest.raises(ValueError, match='unknown'):
        if operation == 'start':
            raw_api.start_large_file(
                API, TOKEN, 'bucket-id', 'file.txt', 'text/plain', {}, file_retention=unknown
            )
        elif operation == 'copy':
            raw_api.copy_file(API, TOKEN, 'source-id', 'copy.txt', legal_hold=LegalHold.UNKNOWN)
        elif operation == 'upload':
            raw_api.upload_file(
                API,
                TOKEN,
                'file.txt',
                1,
                'text/plain',
                'sha1',
                {},
                io.BytesIO(b'x'),
                file_retention=unknown,
            )
        elif operation == 'retention_update':
            raw_api.update_file_retention(API, TOKEN, 'file-id', 'file.txt', unknown)
        elif operation == 'hold_update':
            raw_api.update_file_legal_hold(API, TOKEN, 'file-id', 'file.txt', LegalHold.UNKNOWN)
        else:
            raw_api.update_bucket(
                API,
                TOKEN,
                'account-id',
                'bucket-id',
                default_retention=BucketRetentionSetting(RetentionMode.UNKNOWN),
            )
    assert raw_api.b2_http.mock_calls == []


@pytest.mark.parametrize('operation', ['start', 'copy', 'copy_part', 'upload', 'upload_part'])
def test_missing_customer_secret_is_rejected_before_transport(raw_api, operation):
    setting = customer_setting(secret=None)
    with pytest.raises(WrongEncryptionSettingForFileWrite):
        if operation == 'start':
            raw_api.start_large_file(
                API,
                TOKEN,
                'bucket-id',
                'file.txt',
                'text/plain',
                {},
                server_side_encryption=setting,
            )
        elif operation == 'copy':
            raw_api.copy_file(
                API, TOKEN, 'source-id', 'copy.txt', destination_server_side_encryption=setting
            )
        elif operation == 'copy_part':
            raw_api.copy_part(
                API, TOKEN, 'source-id', 'large-id', 1, destination_server_side_encryption=setting
            )
        elif operation == 'upload':
            raw_api.upload_file(
                API,
                TOKEN,
                'file.txt',
                1,
                'text/plain',
                'sha1',
                {},
                io.BytesIO(b'x'),
                server_side_encryption=setting,
            )
        else:
            raw_api.upload_part(
                API, TOKEN, 1, 1, 'sha1', io.BytesIO(b'x'), server_side_encryption=setting
            )
    assert raw_api.b2_http.mock_calls == []


@pytest.mark.parametrize(
    'operation,transport,expected_error',
    [
        ('download', 'get_content', SSECKeyError),
        ('copy', 'post_json_return_json', SSECKeyError),
        ('copy_part', 'post_json_return_json', SSECKeyError),
        ('retention', 'post_json_return_json', RetentionWriteError),
        ('legal_hold', 'post_json_return_json', RetentionWriteError),
    ],
)
def test_access_denied_is_translated_at_the_boundary(raw_api, operation, transport, expected_error):
    getattr(raw_api.b2_http, transport).side_effect = AccessDenied()
    with pytest.raises(expected_error):
        if operation == 'download':
            raw_api.download_file_from_url(TOKEN, API, encryption=customer_setting())
        elif operation == 'copy':
            raw_api.copy_file(
                API,
                TOKEN,
                'source-id',
                'copy.txt',
                source_server_side_encryption=customer_setting(),
            )
        elif operation == 'copy_part':
            raw_api.copy_part(
                API,
                TOKEN,
                'source-id',
                'large-id',
                1,
                source_server_side_encryption=customer_setting(),
            )
        elif operation == 'retention':
            raw_api.update_file_retention(
                API,
                TOKEN,
                'file-id',
                'file.txt',
                FileRetentionSetting(RetentionMode.GOVERNANCE, UNTIL),
            )
        else:
            raw_api.update_file_legal_hold(API, TOKEN, 'file-id', 'file.txt', LegalHold.ON)
    assert getattr(raw_api.b2_http, transport).call_count == 1


@pytest.mark.parametrize('missing', [False, True])
def test_file_info_by_name_encodes_path_and_preserves_not_found_identity(raw_api, missing):
    expected_url = 'https://download.example.invalid/file/bucket-name/dir/%CE%94%20%2B.txt'
    if missing:
        raw_api.b2_http.head_content.side_effect = ResourceNotFound()
        with pytest.raises(FileOrBucketNotFound) as exc:
            raw_api.get_file_info_by_name(
                'https://download.example.invalid', TOKEN, 'bucket-name', 'dir/Δ +.txt'
            )
        assert exc.value.bucket_name == 'bucket-name'
        assert exc.value.file_id_or_name == 'dir/Δ +.txt'
    else:
        raw_api.b2_http.head_content.return_value.headers = {
            'Content-Length': '4',
            'X-Bz-File-Id': 'file-id',
        }
        assert raw_api.get_file_info_by_name(
            'https://download.example.invalid', TOKEN, 'bucket-name', 'dir/Δ +.txt'
        ) == {
            'Content-Length': '4',
            'X-Bz-File-Id': 'file-id',
        }
    raw_api.b2_http.head_content.assert_called_once_with(
        expected_url, headers={'Authorization': TOKEN}
    )


@pytest.mark.parametrize(
    'key_id,expected',
    [
        ('plain-id', 'plain-id'),
        ('key id+Δ', 'key%20id%2B%CE%94'),
        ('literal%20value', 'literal%2520value'),
        ('plus+literal', 'plus%2Bliteral'),
    ],
)
def test_upload_accepts_matching_key_id_from_file_metadata(raw_api, key_id, expected):
    raw_api.upload_file(
        API,
        TOKEN,
        'file.txt',
        1,
        'text/plain',
        'sha1',
        {'sse_c_key_id': key_id},
        io.BytesIO(b'x'),
        server_side_encryption=customer_setting(key_id),
    )
    headers = raw_api.b2_http.post_content_return_json.call_args.args[1]
    assert headers['X-Bz-Info-sse_c_key_id'] == expected
    assert raw_api.b2_http.post_content_return_json.call_count == 1


@pytest.mark.parametrize('metadata_id', ['different', 'key%20id+Δ'])
def test_conflicting_upload_key_id_is_rejected_before_transport(raw_api, metadata_id):
    with pytest.raises(ValueError, match='Ambiguous key id'):
        raw_api.upload_file(
            API,
            TOKEN,
            'file.txt',
            1,
            'text/plain',
            'sha1',
            {'sse_c_key_id': metadata_id},
            io.BytesIO(b'x'),
            server_side_encryption=customer_setting('key id+Δ'),
        )
    assert raw_api.b2_http.mock_calls == []


@pytest.mark.parametrize('operation', ['start', 'upload'])
def test_unknown_customer_key_id_is_rejected_before_metadata_write(raw_api, operation):
    setting = customer_setting(UNKNOWN_KEY_ID)
    with pytest.raises(ValueError, match='unknown key id'):
        if operation == 'start':
            raw_api.start_large_file(
                API,
                TOKEN,
                'bucket-id',
                'file.txt',
                'text/plain',
                {},
                server_side_encryption=setting,
            )
        else:
            raw_api.upload_file(
                API,
                TOKEN,
                'file.txt',
                1,
                'text/plain',
                'sha1',
                {},
                io.BytesIO(b'x'),
                server_side_encryption=setting,
            )
    assert raw_api.b2_http.mock_calls == []
