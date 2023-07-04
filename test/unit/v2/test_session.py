######################################################################
#
# File: test/unit/v2/test_session.py
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
from b2sdk.v2 import B2Session


@pytest.fixture
def dummy_session():
    return B2Session()


def test_session__upload_file__supports_file_infos(dummy_session, file_info):
    """Test v2.B2Session.upload_file support of deprecated file_infos param"""
    with patch_bind_params(v3.B2Session, 'upload_file') as mock_method,\
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
    assert mock_method.get_bound_call_args()["file_info"] == file_info
    assert 'file_infos' not in mock_method.call_args[1]
