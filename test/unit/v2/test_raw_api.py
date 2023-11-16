######################################################################
#
# File: test/unit/v2/test_raw_api.py
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
from b2sdk.v2 import B2Http, B2RawHTTPApi


@pytest.fixture
def dummy_b2_raw_http_api():
    return B2RawHTTPApi(Mock(spec=B2Http))


def test_b2_raw_http_api__get_upload_file_headers__supports_file_infos(
    dummy_b2_raw_http_api, file_info
):
    """Test v2.B2RawHTTPApi.get_upload_file_headers support of deprecated file_infos param"""
    with patch_bind_params(v3.B2RawHTTPApi, 'get_upload_file_headers') as mock_method,\
        pytest.warns(DeprecationWarning, match=r'deprecated argument'):
        dummy_b2_raw_http_api.get_upload_file_headers(
            'upload_auth_token',
            'file_name',
            123,  # content_length
            'content_type',
            'content_sha1',
            file_infos=file_info,
            server_side_encryption=None,
            file_retention=None,
            legal_hold=None,
            custom_upload_timestamp=None,
        )
    assert mock_method.get_bound_call_args()["file_info"] == file_info
    assert 'file_infos' not in mock_method.call_args[1]


def test_b2_raw_http_api__upload_file__supports_file_infos(dummy_b2_raw_http_api, file_info):
    """Test v2.B2RawHTTPApi.upload_file support of deprecated file_infos param"""
    with patch_bind_params(v3.B2RawHTTPApi, 'upload_file') as mock_method,\
        pytest.warns(DeprecationWarning, match=r'deprecated argument'):
        dummy_b2_raw_http_api.upload_file(
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
