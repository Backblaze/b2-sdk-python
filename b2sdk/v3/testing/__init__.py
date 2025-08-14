######################################################################
#
# File: b2sdk/v3/testing/__init__.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

# testing - it is not imported in v3.__init__ as it depends on pytest and other test dependencies.

from b2sdk._internal.testing.helpers.api import get_b2_auth_data, authorize, get_realm
from b2sdk._internal.testing.helpers.base import IntegrationTestBase
from b2sdk._internal.testing.helpers.buckets import (
    GENERAL_BUCKET_NAME_PREFIX,
    BUCKET_NAME_LENGTH,
    BUCKET_CREATED_AT_MILLIS,
    RNG,
    random_token,
    get_bucket_name_prefix,
)
from b2sdk._internal.testing.helpers.bucket_manager import (
    NODE_DESCRIPTION,
    ONE_HOUR_MILLIS,
    BUCKET_CLEANUP_PERIOD_MILLIS,
    BucketManager,
)
from b2sdk._internal.testing.fixtures.api import (
    set_http_debug,
    b2_auth_data,
    _b2_api,
    b2_api,
    realm,
)
from b2sdk._internal.testing.fixtures.buckets import (
    pytest_addoption,
    dont_cleanup_old_buckets,
    bucket_name_prefix,
    general_bucket_name_prefix,
    bucket_manager,
    bucket,
    b2_subfolder,
)
