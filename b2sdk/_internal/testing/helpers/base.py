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
from b2sdk.v2.exception import DuplicateBucketName


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
        bucket_name = self.bucket_manager.new_bucket_name()
        try:
            bucket = self.bucket_manager.create_bucket(name=bucket_name)
        except DuplicateBucketName:
            self._duplicated_bucket_name_debug_info(bucket_name)
            raise
        self.buckets_created.append(bucket)
        return bucket

    def _duplicated_bucket_name_debug_info(self, bucket_name: str) -> None:
        # Trying to obtain as much information as possible about this bucket.
        print(' DUPLICATED BUCKET DEBUG START '.center(60, '='))
        bucket = self.b2_api.get_bucket_by_name(bucket_name)

        print('Bucket metadata:')
        bucket_dict = bucket.as_dict()
        for info_key, info in bucket_dict.items():
            print(f'\t{info_key}: "{info}"')

        print('All files (and their versions) inside the bucket:')
        ls_generator = bucket.ls(recursive=True, latest_only=False)
        for file_version, _directory in ls_generator:
            # as_dict() is bound to have more info than we can use,
            # but maybe some of it will cast some light on the issue.
            print(f'\t{file_version.file_name} ({file_version.as_dict()})')

        print(' DUPLICATED BUCKET DEBUG END '.center(60, '='))
