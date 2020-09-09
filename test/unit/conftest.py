######################################################################
#
# File: test/unit_new/conftest.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import importlib
from functools import partial

import pytest

pytest.register_assert_rewrite('test.unit')


def get_apiver_modules(version):
    return importlib.import_module('b2sdk.%s' % version), importlib.import_module(
        'b2sdk.%s.exception' % version
    )


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
    pytest.get_apiver_modules = partial(get_apiver_modules, config.getoption('--api'))


@pytest.hookimpl
def pytest_report_header(config):
    return 'b2sdk apiver: %s' % config.getoption('--api')


@pytest.hookimpl(tryfirst=True)
def pytest_ignore_collect(path, config):
    path = str(path)
    ver = config.getoption('--api')
    if ver == 'v1' and 'v0/' in path:
        return True
    if ver == 'v0' and 'v1/' in path:
        return True
    return False


@pytest.fixture(scope='session')
def b2sdk_apiver(request):
    return request.config.getoption('--api')
