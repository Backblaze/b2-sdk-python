######################################################################
#
# File: test/unit/conftest.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import os
import shutil
import sys
from glob import glob
from pathlib import Path

try:
    import ntsecuritycon
    import win32api
    import win32security
except ImportError:
    ntsecuritycon = win32api = win32security = None

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
def pytest_ignore_collect(collection_path, config):
    """Ignore all tests from subfolders for different apiver."""
    ver = config.getoption('--api')
    other_versions = [v for v in API_VERSIONS if v != ver]
    for other_version in other_versions:
        if other_version in collection_path.parts:
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


@pytest.fixture(scope='session')
def apiver_int(apiver):
    """Get apiver as an int, e.g. `2`."""
    return int(apiver[1:])


@pytest.fixture
def b2api():
    from apiver_deps import (
        B2Api,
        B2HttpApiConfig,
        RawSimulator,
        StubAccountInfo,
    )

    account_info = StubAccountInfo()
    api = B2Api(
        account_info,
        api_config=B2HttpApiConfig(_raw_api_class=RawSimulator),
    )

    simulator = api.session.raw_api
    account_id, master_key = simulator.create_account()
    api.authorize_account(
        application_key_id=account_id,
        application_key=master_key,
        realm='production',
    )
    return api


@pytest.fixture
def b2api_simulator(b2api):
    return b2api.session.raw_api


@pytest.fixture
def bucket(b2api):
    return b2api.create_bucket('test-bucket', 'allPublic')


@pytest.fixture
def file_info():
    return {'key': 'value'}


class PermTool:
    def allow_access(self, path):
        pass

    def deny_access(self, path):
        pass


class UnixPermTool(PermTool):
    def allow_access(self, path):
        path.chmod(0o700)

    def deny_access(self, path):
        path.chmod(0o000)


class WindowsPermTool(PermTool):
    def __init__(self):
        self.user_sid = win32security.GetTokenInformation(
            win32security.OpenProcessToken(win32api.GetCurrentProcess(), win32security.TOKEN_QUERY),
            win32security.TokenUser
        )[0]

    def allow_access(self, path):
        dacl = win32security.ACL()
        dacl.AddAccessAllowedAce(
            win32security.ACL_REVISION, ntsecuritycon.FILE_ALL_ACCESS, self.user_sid
        )

        security_desc = win32security.GetFileSecurity(
            str(path), win32security.DACL_SECURITY_INFORMATION
        )
        security_desc.SetSecurityDescriptorDacl(1, dacl, 0)
        win32security.SetFileSecurity(
            str(path), win32security.DACL_SECURITY_INFORMATION, security_desc
        )

    def deny_access(self, path):
        dacl = win32security.ACL()
        dacl.AddAccessDeniedAce(
            win32security.ACL_REVISION, ntsecuritycon.FILE_ALL_ACCESS, self.user_sid
        )

        security_desc = win32security.GetFileSecurity(
            str(path), win32security.DACL_SECURITY_INFORMATION
        )
        security_desc.SetSecurityDescriptorDacl(1, dacl, 0)
        win32security.SetFileSecurity(
            str(path), win32security.DACL_SECURITY_INFORMATION, security_desc
        )


@pytest.fixture
def fs_perm_tool(tmp_path):
    """
    Ensure tmp_path is delete-able after the test.

    Important for the tests that mess with filesystem permissions.
    """
    if os.name == 'nt':
        if win32api is None:
            pytest.skip('pywin32 is required to run this test')
        perm_tool = WindowsPermTool()
    else:
        perm_tool = UnixPermTool()
    yield perm_tool

    try:
        shutil.rmtree(tmp_path)
    except OSError:
        perm_tool.allow_access(tmp_path)

        for root, dirs, files in os.walk(tmp_path, topdown=True):
            for name in dirs:
                perm_tool.allow_access(Path(root) / name)
            for name in files:
                file_path = Path(root) / name
                perm_tool.allow_access(file_path)
                file_path.unlink()

        for root, dirs, files in os.walk(tmp_path, topdown=False):
            for name in dirs:
                (Path(root) / name).rmdir()

        tmp_path.rmdir()
