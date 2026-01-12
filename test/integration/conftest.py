######################################################################
#
# File: test/integration/conftest.py
#
# Copyright 2025 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest


@pytest.fixture(scope='session', autouse=True)
def auto_change_account_info_dir(change_account_info_dir):
    pass
