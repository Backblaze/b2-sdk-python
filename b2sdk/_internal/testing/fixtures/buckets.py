######################################################################
#
# File: b2sdk/_internal/testing/fixtures/buckets.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import secrets

import pytest

from b2sdk._internal.testing.helpers.bucket_manager import BucketManager
from b2sdk._internal.testing.helpers.buckets import (
    GENERAL_BUCKET_NAME_PREFIX,
    get_bucket_name_prefix,
)


def pytest_addoption(parser):
    """Add a flag for not cleaning up old buckets"""
    parser.addoption(
        '--dont-cleanup-old-buckets',
        action='store_true',
        default=False,
    )


@pytest.fixture(scope='session')
def dont_cleanup_old_buckets(request):
    return request.config.getoption('--dont-cleanup-old-buckets')


@pytest.fixture(scope='session')
def bucket_name_prefix():
    return get_bucket_name_prefix(8)


@pytest.fixture(scope='session')
def general_bucket_name_prefix():
    return GENERAL_BUCKET_NAME_PREFIX


@pytest.fixture(scope='session')
def bucket_manager(
    bucket_name_prefix, general_bucket_name_prefix, dont_cleanup_old_buckets, _b2_api
):
    manager = BucketManager(
        dont_cleanup_old_buckets,
        _b2_api,
        current_run_prefix=bucket_name_prefix,
        general_prefix=general_bucket_name_prefix,
    )
    yield manager
    manager.clean_buckets()


@pytest.fixture
def bucket(bucket_name_prefix, bucket_manager):
    bucket = bucket_manager.create_bucket()
    yield bucket
    bucket_manager.clean_bucket(bucket)


@pytest.fixture
def b2_subfolder(bucket, request):
    subfolder_name = f'{request.node.name}_{secrets.token_urlsafe(4)}'
    return f'b2://{bucket.name}/{subfolder_name}'
