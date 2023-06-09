######################################################################
#
# File: test/unit/v2/test_raw_api.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from unittest.mock import Mock, patch

import pytest

from b2sdk.v2 import B2RawHTTPApi, B2Http
from b2sdk import _v3 as v3


@pytest.fixture
def dummy_b2_raw_http_api():
    return B2RawHTTPApi(Mock(spec=B2Http))


def test_b2_raw_http_api__upload_file__supports_file_infos(dummy_b2_raw_http_api):
    """Test v2.B2RawHTTPApi.upload_file support of deprecated file_infos param"""
    file_info = {'key': 'value'}
    with patch.object(v3.B2RawHTTPApi, 'upload_file') as mock_method,\
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
    assert mock_method.call_args[1]['file_info'] == file_info
