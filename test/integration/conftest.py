######################################################################
#
# File: test/integration/conftest.py
#
# Copyright 2025 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest

from b2sdk._internal.exception import ServiceError

RETRYABLE_SERVICE_ERROR_STATUSES = {500, 503}
INTEGRATION_TEST_RETRY_COUNT = 2


@pytest.fixture(scope='session', autouse=True)
def auto_change_account_info_dir(change_account_info_dir):
    pass


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem):
    testfunction = pyfuncitem.obj
    funcargs = pyfuncitem.funcargs
    testargs = {arg: funcargs[arg] for arg in pyfuncitem._fixtureinfo.argnames}

    for attempt in range(INTEGRATION_TEST_RETRY_COUNT + 1):
        try:
            testfunction(**testargs)
            return True
        except ServiceError as exc:
            if exc._status not in RETRYABLE_SERVICE_ERROR_STATUSES:
                raise
            if attempt >= INTEGRATION_TEST_RETRY_COUNT:
                raise
            print(
                f'Retrying {pyfuncitem.nodeid} after transient service error {exc._status}:'
                f' attempt {attempt + 1} of {INTEGRATION_TEST_RETRY_COUNT}'
            )
