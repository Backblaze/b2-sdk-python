######################################################################
#
# File: test/unit/v2/test_session.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from unittest.mock import Mock, patch

import pytest

from b2sdk.v2 import B2Session
from b2sdk import _v3 as v3


@pytest.fixture
def dummy_session():
    return B2Session()


def test_session__upload_file__supports_file_infos(dummy_session, file_info):
    """Test v2.B2Session.upload_file support of deprecated file_infos param"""
    with patch.object(v3.B2Session, 'upload_file') as mock_method,\
        pytest.warns(DeprecationWarning, match=r'deprecated argument'):
        dummy_session.upload_file(
            'filename',
            'filename',
            content_type='text/plain',
            content_length=0,
            content_sha1='dummy',
            data_stream=Mock(),
            file_infos=file_info,
        )
    assert mock_method.call_args[1]['file_info'] == file_info
    assert 'file_infos' not in mock_method.call_args[1]
