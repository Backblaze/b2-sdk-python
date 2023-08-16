######################################################################
#
# File: test/unit/v_all/test_constants.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import apiver_deps
import pytest


@pytest.mark.apiver(from_ver=2)
def test_public_constants():
    assert set(dir(apiver_deps)) >= {
        "ALL_CAPABILITIES",
        "B2_ACCOUNT_INFO_DEFAULT_FILE",
        "B2_ACCOUNT_INFO_ENV_VAR",
        "B2_ACCOUNT_INFO_PROFILE_FILE",
        "DEFAULT_MIN_PART_SIZE",
        "DEFAULT_RECOMMENDED_UPLOAD_PART_SIZE",
        "LARGE_FILE_SHA1",
        "LIST_FILE_NAMES_MAX_LIMIT",
        "SERVER_DEFAULT_SYNC_ENCRYPTION_SETTINGS_PROVIDER",
        "SRC_LAST_MODIFIED_MILLIS",
        "SSE_B2_AES",
        "SSE_C_KEY_ID_FILE_INFO_KEY_NAME",
        "SSE_NONE",
        "UNKNOWN_KEY_ID",
        "V",
        "VERSION",
        "XDG_CONFIG_HOME_ENV_VAR",
    }
