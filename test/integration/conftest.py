######################################################################
#
# File: test/integration/conftest.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest


def pytest_addoption(parser):
    """Add a flag for not cleaning up old buckets"""
    parser.addoption(
        '--dont-cleanup-old-buckets',
        action="store_true",
        default=False,
    )


@pytest.fixture
def dont_cleanup_old_buckets(request):
    return request.config.getoption("--dont-cleanup-old-buckets")
