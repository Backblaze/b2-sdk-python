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

from . import get_b2_auth_data
from .bucket_cleaner import BucketCleaner
from .test_raw_api import cleanup_old_buckets

if __name__ == '__main__':
    cleanup_old_buckets()
    BucketCleaner(False, *get_b2_auth_data()).cleanup_buckets()
