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
    def setup(self, sqlite_account_info_factory, account_info_default_data_schema_0):
        self.sqlite_account_info_factory = sqlite_account_info_factory
        self.account_info_default_data = account_info_default_data_schema_0

    def test_upgrade_1_default_allowed(self):
        """The 'allowed' field should be the default for upgraded databases."""
        old_account_info = self.sqlite_account_info_factory(schema_0=True)
        old_account_info.set_auth_data_with_schema_0_for_test(**self.account_info_default_data)
        new_account_info = self.sqlite_account_info_factory(file_name=old_account_info.filename)

        assert AbstractAccountInfo.DEFAULT_ALLOWED == new_account_info.get_allowed()

    def test_upgrade_2_default_app_key(self):
        """The 'application_key_id' field should default to the account ID."""
        old_account_info = self.sqlite_account_info_factory(schema_0=True)
        old_account_info.set_auth_data_with_schema_0_for_test(**self.account_info_default_data)
        new_account_info = self.sqlite_account_info_factory(file_name=old_account_info.filename)

        assert 'account_id' == new_account_info.get_application_key_id()

    def test_upgrade_3_default_s3_api_url(self):
        """The 's3_api_url' field should be set."""
        old_account_info = self.sqlite_account_info_factory(schema_0=True)
        old_account_info.set_auth_data_with_schema_0_for_test(**self.account_info_default_data)
        new_account_info = self.sqlite_account_info_factory(file_name=old_account_info.filename)

        assert '' == new_account_info.get_s3_api_url()

    def test_migrate_to_4(self):
        old_account_info = self.sqlite_account_info_factory(schema_0=True)
        old_account_info.set_auth_data_with_schema_0_for_test(**self.account_info_default_data)
        new_account_info = self.sqlite_account_info_factory(file_name=old_account_info.filename)

        with new_account_info._get_connection() as conn:
            sizes = conn.execute(
                "SELECT recommended_part_size, absolute_minimum_part_size from account"
            ).fetchone()
        assert (100, 5000000) == sizes
