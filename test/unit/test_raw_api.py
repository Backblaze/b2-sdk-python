######################################################################
#
# File: test/unit/test_raw_api.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import io

import pytest
from apiver_deps import (
    B2RawHTTPApi,
    EncryptionMode,
    EncryptionSetting,
)
from apiver_deps_exception import WrongEncryptionSettingForFileWrite

NO_ENCRYPTION = EncryptionSetting(EncryptionMode.NONE)


@pytest.fixture
def raw_api(mocker):
    return B2RawHTTPApi(mocker.MagicMock())


@pytest.mark.parametrize(
    'write_file',
    [
        pytest.param(
            lambda raw_api: raw_api.upload_file(
                'upload-url',
                'upload-token',
                'file-name',
                1,
                'text/plain',
                'sha1',
                {},
                io.BytesIO(),
                server_side_encryption=NO_ENCRYPTION,
            ),
            id='upload_file',
        ),
        pytest.param(
            lambda raw_api: raw_api.upload_part(
                'upload-url',
                'upload-token',
                1,
                1,
                'sha1',
                io.BytesIO(),
                server_side_encryption=NO_ENCRYPTION,
            ),
            id='upload_part',
        ),
        pytest.param(
            lambda raw_api: raw_api.start_large_file(
                'api-url',
                'account-token',
                'bucket-id',
                'file-name',
                'text/plain',
                {},
                server_side_encryption=NO_ENCRYPTION,
            ),
            id='start_large_file',
        ),
        pytest.param(
            lambda raw_api: raw_api.copy_file(
                'api-url',
                'account-token',
                'source-file-id',
                'new-file-name',
                destination_server_side_encryption=NO_ENCRYPTION,
            ),
            id='copy_file',
        ),
        pytest.param(
            lambda raw_api: raw_api.copy_part(
                'api-url',
                'account-token',
                'source-file-id',
                'large-file-id',
                1,
                destination_server_side_encryption=NO_ENCRYPTION,
            ),
            id='copy_part',
        ),
    ],
)
def test_raw_api_rejects_no_encryption_for_file_writes(raw_api, write_file):
    with pytest.raises(WrongEncryptionSettingForFileWrite):
        write_file(raw_api)


@pytest.mark.parametrize(
    'call, transport_method',
    [
        pytest.param(
            lambda raw_api: raw_api.create_bucket(
                'api-url', 'account-token', 'account-id', 'bucket-name', 'allPrivate'
            ),
            'post_json_return_json',
            id='create_bucket',
        ),
        pytest.param(
            lambda raw_api: raw_api.update_bucket(
                'api-url', 'account-token', 'account-id', 'bucket-id', bucket_type='allPrivate'
            ),
            'post_json_return_json',
            id='update_bucket',
        ),
        pytest.param(
            lambda raw_api: raw_api.upload_file(
                'upload-url',
                'upload-token',
                'file-name',
                1,
                'text/plain',
                'sha1',
                {},
                io.BytesIO(),
            ),
            'post_content_return_json',
            id='upload_file',
        ),
        pytest.param(
            lambda raw_api: raw_api.upload_part(
                'upload-url', 'upload-token', 1, 1, 'sha1', io.BytesIO()
            ),
            'post_content_return_json',
            id='upload_part',
        ),
        pytest.param(
            lambda raw_api: raw_api.start_large_file(
                'api-url', 'account-token', 'bucket-id', 'file-name', 'text/plain', {}
            ),
            'post_json_return_json',
            id='start_large_file',
        ),
        pytest.param(
            lambda raw_api: raw_api.copy_file(
                'api-url', 'account-token', 'source-file-id', 'new-file-name'
            ),
            'post_json_return_json',
            id='copy_file',
        ),
        pytest.param(
            lambda raw_api: raw_api.copy_part(
                'api-url', 'account-token', 'source-file-id', 'large-file-id', 1
            ),
            'post_json_return_json',
            id='copy_part',
        ),
    ],
)
def test_raw_api_omits_encryption_from_request_when_not_given(raw_api, call, transport_method):
    """
    With no encryption setting given, nothing about encryption may appear in the
    request: the server applies the bucket default.
    """
    call(raw_api)

    transport_call = getattr(raw_api.b2_http, transport_method).call_args
    _, headers, payload = transport_call.args[:3]
    sent = {**headers, **(payload if isinstance(payload, dict) else {})}

    assert not [key for key in sent if 'encryption' in key.lower()], sent
