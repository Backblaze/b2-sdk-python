######################################################################
#
# File: b2sdk/_internal/testing/helpers/api.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import os

from b2sdk._internal.account_info.in_memory import InMemoryAccountInfo
from b2sdk._internal.api import B2Api
from b2sdk._internal.api_config import DEFAULT_HTTP_API_CONFIG


def get_realm() -> str:
    return os.environ.get('B2_TEST_ENVIRONMENT', 'production')


def get_b2_auth_data():
    application_key_id = os.environ.get('B2_TEST_APPLICATION_KEY_ID')
    if application_key_id is None:
        raise ValueError('B2_TEST_APPLICATION_KEY_ID is not set.')

    application_key = os.environ.get('B2_TEST_APPLICATION_KEY')
    if application_key is None:
        raise ValueError('B2_TEST_APPLICATION_KEY is not set.')
    return application_key_id, application_key


def authorize(b2_auth_data, realm, api_config=DEFAULT_HTTP_API_CONFIG):
    info = InMemoryAccountInfo()
    b2_api = B2Api(info, api_config=api_config)
    b2_api.authorize_account(*b2_auth_data, realm=realm)
    return b2_api, info
