######################################################################
#
# File: test/integration/__init__.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations
import os


def get_b2_auth_data():
    application_key_id = os.environ.get('B2_TEST_APPLICATION_KEY_ID')
    if application_key_id is None:
        raise ValueError('B2_TEST_APPLICATION_KEY_ID is not set.')

    application_key = os.environ.get('B2_TEST_APPLICATION_KEY')
    if application_key is None:
        raise ValueError('B2_TEST_APPLICATION_KEY is not set.')
    return application_key_id, application_key
