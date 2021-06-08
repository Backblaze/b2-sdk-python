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
    """Add an argument for running test for given apiver."""
    parser.addoption(
        '--api',
        default=API_VERSIONS[-1],
        choices=API_VERSIONS,
        help='version of the API',
    )


@pytest.hookimpl
def pytest_configure(config):
    """Add apiver test folder to the path and add "apiver" marker used by `pytest_runtest_setup`."""
    sys.path.insert(0, str(Path(__file__).parent / config.getoption('--api') / 'apiver'))
    config.addinivalue_line(
        'markers',
        'apiver(*args, *, from_ver=0, to_ver=sys.maxsize): mark test to run only for specific apivers'
    )


@pytest.hookimpl
def pytest_report_header(config):
    """Print apiver in the header."""
    return 'b2sdk apiver: %s' % config.getoption('--api')


@pytest.hookimpl(tryfirst=True)
def pytest_ignore_collect(path, config):
    """Ignore all tests from subfolders for different apiver."""
    path = str(path)
    ver = config.getoption('--api')
    other_versions = [v for v in API_VERSIONS if v != ver]
    for other_version in other_versions:
        if other_version + os.sep in path:
            return True
    return False


def pytest_runtest_setup(item):
    """
    Skip tests based on "apiver" marker.

    .. code-block:: python

       @pytest.mark.apiver(1)
       def test_only_for_v1(self):
           ...

       @pytest.mark.apiver(1, 3)
       def test_only_for_v1_and_v3(self):
           ...

       @pytest.mark.apiver(from_ver=2)
       def test_for_greater_or_equal_v2(self):
           ...

       @pytest.mark.apiver(to_ver=2)
       def test_for_less_or_equal_v2(self):
           ...

       @pytest.mark.apiver(from_ver=2, to_ver=4)
       def test_for_versions_from_v2_to_v4(self):
           ...

    Both `from_ver` and `to_ver` are inclusive.

    Providing test parameters based on apiver is also possible:

    .. code-block:: python

       @pytest.mark.parametrize(
           'exc_class,exc_msg',
           [
               pytest.param(InvalidArgument, None, marks=pytest.mark.apiver(from_ver=2)),
               pytest.param(re.error, "invalid something", marks=pytest.mark.apiver(to_ver=1)),
           ],
       )
       def test_illegal_regex(self, exc_class, exc_msg):
           with pytest.raises(exc_class, match=exc_msg):
               error_raising_function()

    If a test is merked with "apiver" multiple times, it will be skipped if at least one of the apiver conditions
    cause it to be skipped. E.g. if a test module is marked with

    .. code-block:: python

       pytestmark = [pytest.mark.apiver(from_ver=1)]

    and a test function is marked with

    .. code-block:: python

       @pytest.mark.apiver(to_ver=1)
       def test_function(self):
           ...

    the test function will be run only for apiver=v1
    """
    for mark in item.iter_markers(name='apiver'):
        if mark.args and mark.kwargs:
            raise pytest.UsageError('apiver mark should not have both args and kwargs')

        int_ver = int(item.config.getoption('--api')[1:])
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
    """Get apiver as a v-prefixed string, e.g. "v2"."""
    return request.config.getoption('--api')
