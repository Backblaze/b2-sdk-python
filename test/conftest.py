######################################################################
#
# File: test/conftest.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import concurrent.futures

import pytest

pytest_plugins = ['b2sdk.v3.testing']


@pytest.fixture
def bg_executor():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        yield executor


@pytest.fixture
def apiver_module():
    """
    b2sdk apiver module fixture.

    A compatibility function that is to be replaced by `pytest-apiver` plugin in the future.
    """
    import apiver_deps  # noqa: F401

    return apiver_deps
