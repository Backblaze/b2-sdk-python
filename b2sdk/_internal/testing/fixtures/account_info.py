######################################################################
#
# File: b2sdk/_internal/testing/fixtures/account_info.py
#
# Copyright 2025 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from b2sdk._internal.account_info.sqlite_account_info import (
    B2_ACCOUNT_INFO_ENV_VAR,
    XDG_CONFIG_HOME_ENV_VAR,
)


@pytest.fixture(scope='session')
def change_account_info_dir(monkeysession, tmp_path_factory):
    """
    For the entire test session:
    1) temporarily remove B2_APPLICATION_KEY and B2_APPLICATION_KEY_ID from the environment
    2) create a temporary directory for storing account info database
    3) set B2_ACCOUNT_INFO_ENV_VAR to point to the temporary account info file
    """

    monkeysession.delenv('B2_APPLICATION_KEY_ID', raising=False)
    monkeysession.delenv('B2_APPLICATION_KEY', raising=False)

    temp_dir = tmp_path_factory.mktemp('b2_config')

    monkeysession.setenv(B2_ACCOUNT_INFO_ENV_VAR, str(temp_dir / ('.b2_account_info')))
    monkeysession.setenv(XDG_CONFIG_HOME_ENV_VAR, str(temp_dir))
