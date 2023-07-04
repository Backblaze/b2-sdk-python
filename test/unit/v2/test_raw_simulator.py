######################################################################
#
# File: test/unit/v2/test_raw_simulator.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from test.helpers import patch_bind_params
from unittest.mock import Mock

import pytest

from b2sdk import _v3 as v3
from b2sdk.v2 import B2Api, BucketSimulator, RawSimulator


@pytest.fixture
def dummy_bucket_simulator():
    return BucketSimulator(Mock(spec=B2Api), 'account_id', 'bucket_id', 'bucket_name', 'allPublic')


@pytest.fixture
def dummy_raw_simulator():
    return RawSimulator()


def test_bucket_simulator__upload_file__supports_file_infos(dummy_bucket_simulator, file_info):
    """Test v2.BucketSimulator.upload_file support of deprecated file_infos param"""
    with patch_bind_params(v3.BucketSimulator, 'upload_file') as mock_method,\
        pytest.warns(DeprecationWarning, match=r'deprecated argument'):
        dummy_bucket_simulator.upload_file(
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


def test_raw_simulator__get_upload_file_headers__supports_file_infos(file_info):
    """Test v2.RawSimulator.get_upload_file_headers support of deprecated file_infos param"""
    with patch_bind_params(v3.RawSimulator, 'get_upload_file_headers') as mock_method,\
        pytest.warns(DeprecationWarning, match=r'deprecated argument'):
        RawSimulator.get_upload_file_headers(
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
