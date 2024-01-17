######################################################################
#
# File: test/unit/account_info/test_sqlite_account_info.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import os

import pytest
from apiver_deps import (
    B2_ACCOUNT_INFO_DEFAULT_FILE,
    B2_ACCOUNT_INFO_ENV_VAR,
    XDG_CONFIG_HOME_ENV_VAR,
    AbstractAccountInfo,
    SqliteAccountInfo,
)

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


class TestSqliteAccountProfileFileLocation:
    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch, tmpdir):
        monkeypatch.setenv(
            'HOME', str(tmpdir)
        )  # this affects .expanduser() and protects the real HOME folder
        monkeypatch.setenv("USERPROFILE", str(tmpdir))  # same as HOME, but for Windows
        monkeypatch.delenv(B2_ACCOUNT_INFO_ENV_VAR, raising=False)
        monkeypatch.delenv(XDG_CONFIG_HOME_ENV_VAR, raising=False)

    def test_invalid_profile_name(self):
        with pytest.raises(ValueError):
            SqliteAccountInfo._get_user_account_info_path(profile='&@(*$')

    def test_profile_and_file_name_conflict(self):
        with pytest.raises(ValueError):
            SqliteAccountInfo._get_user_account_info_path(file_name='foo', profile='bar')

    def test_profile_and_env_var_conflict(self, monkeypatch):
        monkeypatch.setenv(B2_ACCOUNT_INFO_ENV_VAR, 'foo')
        with pytest.raises(ValueError):
            SqliteAccountInfo._get_user_account_info_path(profile='bar')

    def test_profile_and_xdg_config_env_var(self, monkeypatch):
        monkeypatch.setenv(XDG_CONFIG_HOME_ENV_VAR, os.path.join('~', 'custom'))
        account_info_path = SqliteAccountInfo._get_user_account_info_path(profile='secondary')
        assert account_info_path == os.path.expanduser(
            os.path.join('~', 'custom', 'b2', 'db-secondary.sqlite')
        )

    def test_profile(self, monkeypatch):
        xdg_config_path = SqliteAccountInfo._get_xdg_config_path()
        if xdg_config_path:
            expected_path = (xdg_config_path, 'b2', 'db-foo.sqlite')
        else:
            expected_path = ('~', '.b2db-foo.sqlite')

        account_info_path = SqliteAccountInfo._get_user_account_info_path(profile='foo')
        assert account_info_path == os.path.expanduser(os.path.join(*expected_path))

    def test_file_name(self):
        account_info_path = SqliteAccountInfo._get_user_account_info_path(
            file_name=os.path.join('~', 'foo')
        )
        assert account_info_path == os.path.expanduser(os.path.join('~', 'foo'))

    def test_env_var(self, monkeypatch):
        monkeypatch.setenv(B2_ACCOUNT_INFO_ENV_VAR, os.path.join('~', 'foo'))
        account_info_path = SqliteAccountInfo._get_user_account_info_path()
        assert account_info_path == os.path.expanduser(os.path.join('~', 'foo'))

    def test_default_file_if_exists(self, monkeypatch):
        # ensure that XDG_CONFIG_HOME_ENV_VAR doesn't matter if default file exists
        monkeypatch.setenv(XDG_CONFIG_HOME_ENV_VAR, 'some')
        account_file_path = os.path.expanduser(B2_ACCOUNT_INFO_DEFAULT_FILE)
        parent_dir = os.path.abspath(os.path.join(account_file_path, os.pardir))
        os.makedirs(parent_dir, exist_ok=True)
        with open(account_file_path, 'w') as account_file:
            account_file.write('')
        account_info_path = SqliteAccountInfo._get_user_account_info_path()
        assert account_info_path == os.path.expanduser(B2_ACCOUNT_INFO_DEFAULT_FILE)

    def test_xdg_config_env_var(self, monkeypatch):
        monkeypatch.setenv(XDG_CONFIG_HOME_ENV_VAR, os.path.join('~', 'custom'))
        account_info_path = SqliteAccountInfo._get_user_account_info_path()
        assert account_info_path == os.path.expanduser(
            os.path.join('~', 'custom', 'b2', 'account_info')
        )

    def test_default_file(self):
        xdg_config_path = SqliteAccountInfo._get_xdg_config_path()
        if xdg_config_path:
            expected_path = os.path.join(xdg_config_path, 'b2', 'account_info')
        else:
            expected_path = B2_ACCOUNT_INFO_DEFAULT_FILE

        account_info_path = SqliteAccountInfo._get_user_account_info_path()
        assert account_info_path == os.path.expanduser(expected_path)
