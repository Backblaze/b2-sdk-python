######################################################################
#
# File: b2sdk/_internal/testing/fixtures/api.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import http
import http.client
import os

import pytest

from b2sdk._internal.testing.helpers.api import authorize, get_b2_auth_data, get_realm


@pytest.fixture(scope='session')
def realm():
    yield get_realm()


@pytest.fixture(autouse=True, scope='session')
def set_http_debug():
    if os.environ.get('B2_DEBUG_HTTP'):
        http.client.HTTPConnection.debuglevel = 1


@pytest.fixture(scope='session')
def b2_auth_data():
    try:
        return get_b2_auth_data()
    except ValueError as ex:
        pytest.fail(ex.args[0])


@pytest.fixture(scope='session')
def _b2_api(b2_auth_data, realm):
    b2_api, _ = authorize(b2_auth_data, realm)
    return b2_api


@pytest.fixture(scope='session')
def b2_api(_b2_api, bucket_manager):
    return _b2_api
