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

from unittest.mock import Mock

import pytest
from apiver_deps_exception import Unauthorized

from b2sdk import v3
from b2sdk.v2 import B2Http, B2RawHTTPApi, B2Session
from test.helpers import patch_bind_params

from ..account_info.fixtures import *  # noqa
from ..fixtures import *


class TestSession:
    @pytest.fixture(autouse=True)
    def setup(self, b2_session):
        self.b2_session = b2_session

    def test_app_key_info_no_info(self):
        self.b2_session.account_info.get_allowed.return_value = dict(
            bucketId=None,
            bucketName=None,
            capabilities=ALL_CAPABILITIES,
            namePrefix=None,
        )
        self.b2_session.raw_api.get_file_info_by_id.side_effect = Unauthorized('no_go', 'code')
        with pytest.raises(
            Unauthorized, match=r'no_go for application key with no restrictions \(code\)'
        ):
            self.b2_session.get_file_info_by_id(None)

    def test_app_key_info_no_info_no_message(self):
        self.b2_session.account_info.get_allowed.return_value = dict(
            bucketId=None,
            bucketName=None,
            capabilities=ALL_CAPABILITIES,
            namePrefix=None,
        )
        self.b2_session.raw_api.get_file_info_by_id.side_effect = Unauthorized('', 'code')
        with pytest.raises(
            Unauthorized, match=r'unauthorized for application key with no restrictions \(code\)'
        ):
            self.b2_session.get_file_info_by_id(None)

    def test_app_key_info_all_info(self):
        self.b2_session.account_info.get_allowed.return_value = dict(
            bucketId='123456',
            bucketName='my-bucket',
            capabilities=['readFiles'],
            namePrefix='prefix/',
        )
        self.b2_session.raw_api.get_file_info_by_id.side_effect = Unauthorized('no_go', 'code')

        with pytest.raises(
            Unauthorized,
            match=r"no_go for application key with capabilities 'readFiles', restricted to bucket 'my-bucket', restricted to files that start with 'prefix/' \(code\)",
        ):
            self.b2_session.get_file_info_by_id(None)


@pytest.fixture
def dummy_session():
    return B2Session()


@pytest.mark.xdist_group('dummy_session')
def test_session__default_classes_v2():
    session = B2Session()

    assert isinstance(session.raw_api, B2RawHTTPApi), 'Expected v2.B2RawHTTPApi, got %s' % type(
        session.raw_api
    )

    assert isinstance(session.raw_api.b2_http, B2Http), 'Expected v2.B2Http, got %s' % type(
        session.raw_api.b2_http
    )


@pytest.mark.xdist_group('dummy_session')
def test_session__upload_file__supports_file_infos(dummy_session, file_info):
    """Test v2.B2Session.upload_file support of deprecated file_infos param"""
    with patch_bind_params(v3.B2Session, 'upload_file') as mock_method, pytest.warns(
        DeprecationWarning, match=r'deprecated argument'
    ):
        dummy_session.upload_file(
            'filename',
            'filename',
            content_type='text/plain',
            content_length=0,
            content_sha1='dummy',
            data_stream=Mock(),
            file_infos=file_info,
        )
    assert mock_method.get_bound_call_args()['file_info'] == file_info
    assert 'file_infos' not in mock_method.call_args[1]
