######################################################################
#
# File: test/unit/test_session.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from .account_info.fixtures import *  # noqa
from .fixtures import *  # noqa


class TestAuthorizeAccount:
    @pytest.fixture(autouse=True)
    def setup(self, b2_session):
        self.b2_session = b2_session

    def test_simple_authorization(self, mocker):
        self.b2_session.authorize_account('dev', '123', '456')

        self.b2_session.raw_api.authorize_account.assert_called_once_with(
            'http://api.backblazeb2.xyz:8180', '123', '456'
        )
        assert self.b2_session.cache.clear.called is False
        self.b2_session.account_info.set_auth_data.assert_called_once_with(
            account_id=mocker.ANY,
            auth_token=mocker.ANY,
            api_url=mocker.ANY,
            download_url=mocker.ANY,
            minimum_part_size=mocker.ANY,
            application_key='456',
            realm='dev',
            s3_api_url=mocker.ANY,
            allowed=mocker.ANY,
            application_key_id='123',
        )

    def test_clear_cache(self):
        self.b2_session.account_info.is_same_account.return_value = False

        self.b2_session.authorize_account('dev', '123', '456')

        assert self.b2_session.cache.clear.called is True
