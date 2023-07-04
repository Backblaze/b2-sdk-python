######################################################################
#
# File: test/unit/v2/test_bucket.py
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
from b2sdk.v2 import B2Api, Bucket


@pytest.fixture
def dummy_bucket():
    return Bucket(Mock(spec=B2Api), 'bucket_id', 'bucket_name')


def test_bucket__upload_file__supports_file_infos(dummy_bucket, file_info):
    """Test v2.Bucket.upload_file support of deprecated file_infos param"""
    with patch_bind_params(v3.Bucket, 'upload_local_file') as mock_method,\
        pytest.warns(DeprecationWarning, match=r'deprecated argument'):
        dummy_bucket.upload_local_file(
            'filename',
            'filename',
            file_infos=file_info,
        )
    assert mock_method.get_bound_call_args()["file_info"] == file_info
    assert 'file_infos' not in mock_method.call_args[1]


def test_bucket__upload_bytes__supports_file_infos(dummy_bucket, file_info):
    """Test v2.Bucket.upload_bytes support of deprecated file_infos param"""
    with patch_bind_params(dummy_bucket, 'upload') as mock_method,\
        pytest.warns(DeprecationWarning, match=r'deprecated argument'):
        dummy_bucket.upload_bytes(
            b'data',
            'filename',
            file_infos=file_info,
        )
    assert mock_method.get_bound_call_args()["file_info"] == file_info
    assert 'file_infos' not in mock_method.call_args[1]
