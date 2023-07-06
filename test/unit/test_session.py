######################################################################
#
# File: test/unit/test_session.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from unittest import mock

from .account_info.fixtures import *  # noqa
from .fixtures import *  # noqa


class TestAuthorizeAccount:
    @pytest.fixture(autouse=True)
    def setup(self, b2_session):
        self.b2_session = b2_session

    @pytest.mark.parametrize(
        'authorize_call_kwargs',
        [
            pytest.param(
                dict(
                    account_id=mock.ANY,
                    auth_token=mock.ANY,
                    api_url=mock.ANY,
                    download_url=mock.ANY,
                    recommended_part_size=mock.ANY,
                    absolute_minimum_part_size=mock.ANY,
                    application_key='456',
                    realm='dev',
                    s3_api_url=mock.ANY,
                    allowed=mock.ANY,
                    application_key_id='123',
                ),
                marks=pytest.mark.apiver(from_ver=2)
            ),
            pytest.param(
                dict(
                    account_id=mock.ANY,
                    auth_token=mock.ANY,
                    api_url=mock.ANY,
                    download_url=mock.ANY,
                    minimum_part_size=mock.ANY,
                    application_key='456',
                    realm='dev',
                    s3_api_url=mock.ANY,
                    allowed=mock.ANY,
                    application_key_id='123',
                ),
                marks=pytest.mark.apiver(to_ver=1)
            ),
        ],
    )
    def test_simple_authorization(self, authorize_call_kwargs):
        self.b2_session.authorize_account('dev', '123', '456')

        self.b2_session.raw_api.authorize_account.assert_called_once_with(
            'http://api.backblazeb2.xyz:8180', '123', '456'
        )
        assert self.b2_session.cache.clear.called is False
        self.b2_session.account_info.set_auth_data.assert_called_once_with(**authorize_call_kwargs)

    def test_clear_cache(self):
        self.b2_session.account_info.is_same_account.return_value = False

        self.b2_session.authorize_account('dev', '123', '456')

        assert self.b2_session.cache.clear.called is True
