######################################################################
#
# File: test/integration/conftest.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations
import secrets

import pytest

from b2sdk._internal.utils import current_time_millis
from b2sdk._internal.testing.helpers.bucket_manager import BucketManager
from b2sdk._internal.testing.helpers.buckets import (
    BUCKET_CREATED_AT_MILLIS,
    get_bucket_name_prefix,
    random_bucket_name,
    GENERAL_BUCKET_NAME_PREFIX,
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
def bucket_manager(bucket_name_prefix, general_bucket_name_prefix, dont_cleanup_old_buckets, _b2_api):
    cleaner = BucketManager(
        dont_cleanup_old_buckets,
        _b2_api,
        current_run_prefix=bucket_name_prefix,
        general_prefix=general_bucket_name_prefix
    )
    yield cleaner
    cleaner.clean_buckets()


@pytest.fixture
def bucket(b2_api, bucket_name_prefix, bucket_manager):
    bucket = b2_api.create_bucket(
        random_bucket_name(bucket_name_prefix),
        'allPrivate',
        bucket_info={
            'created_by': 'b2-sdk integration test',
            BUCKET_CREATED_AT_MILLIS: str(current_time_millis()),
        },
    )
    yield bucket
    bucket_manager.clean_bucket(bucket)


@pytest.fixture
def b2_subfolder(bucket, request):
    subfolder_name = f'{request.node.name}_{secrets.token_urlsafe(4)}'
    return f'b2://{bucket.name}/{subfolder_name}'
