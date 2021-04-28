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
from glob import glob
from pathlib import Path

import pytest

pytest.register_assert_rewrite('test.unit')


def get_api_versions():
    return [
        str(Path(p).parent.name) for p in sorted(glob(str(Path(__file__).parent / 'v*/apiver/')))
    ]


API_VERSIONS = get_api_versions()


@pytest.hookimpl
def pytest_addoption(parser):
    parser.addoption(
        '--api',
        default=API_VERSIONS[-1],
        choices=API_VERSIONS,
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
    other_versions = [v for v in API_VERSIONS if v != ver]
    for other_version in other_versions:
        if other_version + os.sep in path:
            return True
    return False


@pytest.fixture(scope='session')
def apiver(request):
    return request.config.getoption('--api')
