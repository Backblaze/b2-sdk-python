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
    config.addinivalue_line(
        'markers',
        'apiver(*args, *, from_ver=0, to_ver=sys.maxsize): mark test to run only for specific apivers'
    )


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


def pytest_runtest_setup(item):
    for mark in item.iter_markers(name='apiver'):
        if mark.args and mark.kwargs:
            raise pytest.UsageError('apiver mark should not have both args and kwargs')

        int_ver = int(item.config.getoption('--api')[1])
        if mark.args:
            if int_ver not in mark.args:
                pytest.skip('test requires apiver to be one of: %s' % mark.args)
        elif mark.kwargs:
            from_ver = mark.kwargs.get('from_ver', 0)
            to_ver = mark.kwargs.get('to_ver', sys.maxsize)
            if not (from_ver <= int_ver <= to_ver):
                pytest.skip('test requires apiver to be in range: [%d, %d]' % (from_ver, to_ver))


@pytest.fixture(scope='session')
def apiver(request):
    return request.config.getoption('--api')
