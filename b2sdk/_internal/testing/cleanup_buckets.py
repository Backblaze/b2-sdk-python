######################################################################
#
# File: test/integration/cleanup_buckets.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from test.integration.helpers import authorize

from b2sdk._internal.testing.helpers.api import get_b2_auth_data
from b2sdk._internal.testing.helpers.bucket_manager import BucketManager
# from .test_raw_api import cleanup_old_buckets

if __name__ == '__main__':
    # cleanup_old_buckets()
    BucketManager(
        dont_cleanup_old_buckets=False, b2_api=authorize(get_b2_auth_data())[0]
    ).clean_buckets()
