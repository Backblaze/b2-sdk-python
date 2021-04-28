######################################################################
#
# File: test/unit/account_info/test_account_info.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from .fixtures import *


class TestAccountInfo:
    @pytest.fixture(autouse=True)
    def setup(self, account_info_factory, account_info_default_data):
        self.account_info_factory = account_info_factory
        self.account_info_default_data = account_info_default_data

    @pytest.mark.parametrize(
        'application_key_id,realm,expected',
        (
            ('application_key_id', 'dev', True),
            ('application_key_id', 'test', False),
            ('different_application_key_id', 'dev', False),
            ('different_application_key_id', 'test', False),
        ),
    )
    def test_is_same_key(self, application_key_id, realm, expected):
        account_info = self.account_info_factory()
        account_info.set_auth_data(**self.account_info_default_data)

        assert account_info.is_same_key(application_key_id, realm) is expected

    @pytest.mark.parametrize(
        'account_id,realm,expected',
        (
            ('account_id', 'dev', True),
            ('account_id', 'test', False),
            ('different_account_id', 'dev', False),
            ('different_account_id', 'test', False),
        ),
    )
    def test_is_same_account(self, account_id, realm, expected):
        account_info = self.account_info_factory()
        account_info.set_auth_data(**self.account_info_default_data)

        assert account_info.is_same_account(account_id, realm) is expected

    @pytest.mark.parametrize(
        's3_api_url',
        ('https://s3.us-east-123.backblazeb2.com', 'https://s3.us-west-321.backblazeb2.com')
    )
    def test_s3_api_url(self, s3_api_url):
        account_info = self.account_info_factory()
        account_info_default_data = {
            **self.account_info_default_data,
            's3_api_url': s3_api_url,
        }
        account_info.set_auth_data(**account_info_default_data)
        assert s3_api_url == account_info.get_s3_api_url()
