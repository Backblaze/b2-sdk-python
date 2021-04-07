######################################################################
#
# File: test/unit/account_info/test_sqlite_account_info.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from apiver_deps import AbstractAccountInfo

from .fixtures import *


class TestDatabseMigrations:
    @pytest.fixture(autouse=True)
    def setup(self, sqlite_account_info_factory):
        self.sqlite_account_info_factory = sqlite_account_info_factory

    def test_upgrade_1_default_allowed(self):
        """The 'allowed' field should be the default for upgraded databases."""
        old_account_info = self.sqlite_account_info_factory(schema_0=True)
        new_account_info = self.sqlite_account_info_factory(file_name=old_account_info.filename)

        assert AbstractAccountInfo.DEFAULT_ALLOWED == new_account_info.get_allowed()

    def test_upgrade_2_default_app_key(self):
        """The 'application_key_id' field should default to the account ID."""
        old_account_info = self.sqlite_account_info_factory(schema_0=True)
        new_account_info = self.sqlite_account_info_factory(file_name=old_account_info.filename)

        assert 'account_id' == new_account_info.get_application_key_id()

    def test_upgrade_3_default_s3_api_url(self):
        """The 's3_api_url' field should be set."""
        old_account_info = self.sqlite_account_info_factory(schema_0=True)
        new_account_info = self.sqlite_account_info_factory(file_name=old_account_info.filename)

        assert '' == new_account_info.get_s3_api_url()
