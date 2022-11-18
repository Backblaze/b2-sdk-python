######################################################################
#
# File: test/integration/base.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import http.client
import os

import pytest

from b2sdk.v2 import current_time_millis
from b2sdk.v2.exception import DuplicateBucketName
from .bucket_cleaner import BucketCleaner
from .helpers import GENERAL_BUCKET_NAME_PREFIX, BUCKET_NAME_LENGTH, BUCKET_CREATED_AT_MILLIS, bucket_name_part, authorize


class IntegrationTestBase:
    @pytest.fixture(autouse=True)
    def set_http_debug(self):
        if os.environ.get('B2_DEBUG_HTTP'):
            http.client.HTTPConnection.debuglevel = 1

    @pytest.fixture(autouse=True)
    def save_settings(self, dont_cleanup_old_buckets, b2_auth_data):
        type(self).dont_cleanup_old_buckets = dont_cleanup_old_buckets
        type(self).b2_auth_data = b2_auth_data

    @classmethod
    def setup_class(cls):
        cls.this_run_bucket_name_prefix = GENERAL_BUCKET_NAME_PREFIX + bucket_name_part(8)

    @classmethod
    def teardown_class(cls):
        BucketCleaner(
            cls.dont_cleanup_old_buckets,
            *cls.b2_auth_data,
            current_run_prefix=cls.this_run_bucket_name_prefix
        ).cleanup_buckets()

    @pytest.fixture(autouse=True)
    def setup_method(self):
        self.b2_api, self.info = authorize(self.b2_auth_data)

    def generate_bucket_name(self):
        return self.this_run_bucket_name_prefix + bucket_name_part(
            BUCKET_NAME_LENGTH - len(self.this_run_bucket_name_prefix)
        )

    def write_zeros(self, file, number):
        line = b'0' * 1000 + b'\n'
        line_len = len(line)
        written = 0
        while written <= number:
            file.write(line)
            written += line_len

    def create_bucket(self):
        bucket_name = self.generate_bucket_name()
        try:
            return self.b2_api.create_bucket(
                bucket_name,
                'allPublic',
                bucket_info={BUCKET_CREATED_AT_MILLIS: str(current_time_millis())}
            )
        except DuplicateBucketName:
            self._duplicated_bucket_name_debug_info(bucket_name)
            raise

    def _duplicated_bucket_name_debug_info(self, bucket_name: str) -> None:
        # Trying to obtain as much information as possible about this bucket.
        print(' DUPLICATED BUCKET DEBUG START '.center(60, '='))
        bucket = self.b2_api.get_bucket_by_name(bucket_name)

        print('Bucket metadata:')
        bucket_dict = bucket.as_dict()
        for info_key, info in bucket_dict.items():
            print('\t%s: "%s"' % (info_key, info))

        print('All files (and their versions) inside the bucket:')
        ls_generator = bucket.ls(recursive=True, latest_only=False)
        for file_version, _directory in ls_generator:
            # as_dict() is bound to have more info than we can use,
            # but maybe some of it will cast some light on the issue.
            print('\t%s (%s)' % (file_version.file_name, file_version.as_dict()))

        print(' DUPLICATED BUCKET DEBUG END '.center(60, '='))
