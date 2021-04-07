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
    def setup(self, account_info_factory):
        self.account_info_factory = account_info_factory

    @pytest.mark.parametrize(
        's3_api_url',
        ('https://s3.us-east-123.backblazeb2.com', 'https://s3.us-west-321.backblazeb2.com')
    )
    def test_s3_api_url(self, s3_api_url):
        account_info = self.account_info_factory(s3_api_url=s3_api_url)
        assert s3_api_url == account_info.get_s3_api_url()
