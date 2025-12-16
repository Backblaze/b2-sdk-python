######################################################################
#
# File: b2sdk/_internal/testing/helpers/base.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest

from b2sdk._internal.testing.helpers.bucket_manager import BucketManager
from b2sdk.v2 import B2Api


@pytest.mark.usefixtures('cls_setup')
class IntegrationTestBase:
    b2_api: B2Api
    this_run_bucket_name_prefix: str
    bucket_manager: BucketManager

    @pytest.fixture(autouse=True, scope='class')
    def cls_setup(self, request, b2_api, b2_auth_data, bucket_name_prefix, bucket_manager):
        cls = request.cls
        cls.b2_auth_data = b2_auth_data
        cls.this_run_bucket_name_prefix = bucket_name_prefix
        cls.bucket_manager = bucket_manager
        cls.b2_api = b2_api
        cls.info = b2_api.account_info

    @pytest.fixture(autouse=True)
    def setup_method(self):
        self.buckets_created = []
        yield
        for bucket in self.buckets_created:
            self.bucket_manager.clean_bucket(bucket)

    def write_zeros(self, file, number):
        line = b'0' * 1000 + b'\n'
        line_len = len(line)
        written = 0
        while written <= number:
            file.write(line)
            written += line_len

    def create_bucket(self):
        bucket = self.bucket_manager.create_bucket()
        self.buckets_created.append(bucket)
        return bucket
