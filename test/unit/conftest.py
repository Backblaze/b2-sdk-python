######################################################################
#
# File: test/unit/conftest.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os
import sys
from pathlib import Path

import pytest

pytest.register_assert_rewrite('test.unit')


@pytest.hookimpl
def pytest_addoption(parser):
    parser.addoption(
        '--api',
        default='v1',
        choices=['v0', 'v1'],
        help='version of the API',
    )


@pytest.hookimpl
def pytest_configure(config):
    sys.path.insert(0, str(Path(__file__).parent / config.getoption('--api') / 'apiver'))


@pytest.hookimpl
def pytest_report_header(config):
    return 'b2sdk apiver: %s' % config.getoption('--api')


@pytest.hookimpl(tryfirst=True)
def pytest_ignore_collect(path, config):
    path = str(path)
    ver = config.getoption('--api')
    if ver == 'v1' and 'v0' + os.sep in path:
        return True
    if ver == 'v0' and 'v1' + os.sep in path:
        return True
    return False


@pytest.fixture(scope='session')
def apiver(request):
    return request.config.getoption('--api')
