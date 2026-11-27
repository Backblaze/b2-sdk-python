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

# test.unit.b2_simulator: the opt-in --b2-simulator option for the unit tests.
pytest_plugins = ['b2sdk.v3.testing', 'test.junit_subtests', 'test.unit.b2_simulator']


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'known_defect: test of a defect that is still present, marked as a strict expected failure '
        '(see test/known_defect.py)',
    )


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
