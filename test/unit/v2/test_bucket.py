######################################################################
#
# File: test/unit/v2/test_bucket.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from unittest.mock import Mock, patch

import pytest

from b2sdk.v2 import Bucket, B2Api
from b2sdk import _v3 as v3


@pytest.fixture
def dummy_bucket():
    return Bucket(Mock(spec=B2Api), 'bucket_id', 'bucket_name')


def test_bucket__upload_file__supports_file_infos(dummy_bucket):
    """Test v2.Bucket.upload_file support of deprecated file_infos param"""
    file_info = {'key': 'value'}
    with patch.object(v3.Bucket, 'upload_local_file') as mock_method,\
        pytest.warns(DeprecationWarning, match=r'deprecated argument'):
        dummy_bucket.upload_local_file(
            'filename',
            'filename',
            file_infos={'key': 'value'},
        )
    assert mock_method.call_args[1]['file_info'] == file_info


def test_bucket__upload_bytes__supports_file_infos(dummy_bucket):
    """Test v2.Bucket.upload_bytes support of deprecated file_infos param"""
    file_info = {'key': 'value'}
    with patch.object(dummy_bucket, 'upload') as mock_method,\
        pytest.warns(DeprecationWarning, match=r'deprecated argument'):
        dummy_bucket.upload_bytes(
            b'data',
            'filename',
            file_infos={'key': 'value'},
        )
    assert mock_method.call_args[1]['file_info'] == file_info
