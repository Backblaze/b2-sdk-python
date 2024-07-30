######################################################################
#
# File: test/unit/test_raw_simulator.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import hashlib
from test.helpers import patch_bind_params
from unittest.mock import Mock

import pytest

from b2sdk import _v3 as v3


@pytest.fixture
def dummy_bucket_simulator(apiver_module):
    return apiver_module.BucketSimulator(
        Mock(spec=apiver_module.B2Api), 'account_id', 'bucket_id', 'bucket_name', 'allPublic'
    )


@pytest.fixture
def dummy_raw_simulator(apiver_module):
    return apiver_module.RawSimulator()


@pytest.fixture
def file_sim(apiver_module, dummy_bucket_simulator, file_info):
    data = b'dummy'
    return apiver_module.FileSimulator(
        account_id="account_id",
        bucket=dummy_bucket_simulator,
        file_id="dummy-id",
        action="upload",
        name="dummy.txt",
        content_type="text/plain",
        content_sha1=hashlib.sha1(data).hexdigest(),
        file_info=file_info,
        data_bytes=data,
        upload_timestamp=0,
        server_side_encryption=apiver_module.EncryptionSetting(
            mode=apiver_module.EncryptionMode.SSE_C,
            algorithm=apiver_module.EncryptionAlgorithm.AES256,
            key=apiver_module.EncryptionKey(key_id=None, secret=b"test"),
        )
    )


def test_file_sim__as_download_headers(file_sim):
    assert file_sim.as_download_headers() == {
        'content-length': "5",
        'content-type': 'text/plain',
        'x-bz-content-sha1': '829c3804401b0727f70f73d4415e162400cbe57b',
        'x-bz-upload-timestamp': "0",
        'x-bz-file-id': 'dummy-id',
        'x-bz-file-name': 'dummy.txt',
        'X-Bz-Info-key': 'value',
        'X-Bz-Server-Side-Encryption-Customer-Algorithm': 'AES256',
        'X-Bz-Server-Side-Encryption-Customer-Key-Md5': 'CY9rzUYh03PK3k6DJie09g=='
    }


@pytest.mark.apiver(to_ver=2)
def test_bucket_simulator__upload_file__supports_file_infos(
    apiver_module, dummy_bucket_simulator, file_info
):
    """Test v2.BucketSimulator.upload_file support of deprecated file_infos param"""
    with patch_bind_params(v3.BucketSimulator, 'upload_file') as mock_method,\
        pytest.warns(DeprecationWarning, match=r'deprecated argument'):
        assert dummy_bucket_simulator.upload_file(
            'upload_id',
            'upload_auth_token',
            'file_name',
            123,  # content_length
            'content_type',
            'content_sha1',
            file_infos=file_info,
            data_stream='data_stream',
        )
    assert mock_method.get_bound_call_args()["file_info"] == file_info
    assert 'file_infos' not in mock_method.call_args[1]


@pytest.mark.apiver(to_ver=2)
def test_raw_simulator__get_upload_file_headers__supports_file_infos(apiver_module, file_info):
    """Test v2.RawSimulator.get_upload_file_headers support of deprecated file_infos param"""
    with patch_bind_params(v3.RawSimulator, 'get_upload_file_headers') as mock_method,\
        pytest.warns(DeprecationWarning, match=r'deprecated argument'):
        apiver_module.RawSimulator.get_upload_file_headers(
            upload_auth_token='upload_auth_token',
            file_name='file_name',
            content_type='content_type',
            content_length=123,
            content_sha1='content_sha1',
            server_side_encryption=None,
            file_retention=None,
            legal_hold=None,
            file_infos=file_info,
        )
    assert mock_method.get_bound_call_args()["file_info"] == file_info
    assert 'file_infos' not in mock_method.call_args[1]


@pytest.mark.apiver(to_ver=2)
def test_raw_simulator__upload_file__supports_file_infos(dummy_raw_simulator, file_info):
    """Test v2.RawSimulator.upload_file support of deprecated file_infos param"""
    with patch_bind_params(v3.RawSimulator, 'upload_file') as mock_method,\
        pytest.warns(DeprecationWarning, match=r'deprecated argument'):
        dummy_raw_simulator.upload_file(
            'upload_url',
            'upload_auth_token',
            'file_name',
            123,
            'content_type',
            'content_sha1',
            file_infos=file_info,
            data_stream='data_stream',
        )
    assert mock_method.get_bound_call_args()["file_info"] == file_info
    assert 'file_infos' not in mock_method.call_args[1]
