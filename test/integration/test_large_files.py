######################################################################
#
# File: test/integration/test_large_files.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import hashlib
import io
import pathlib
import random
import string
import time
from unittest import mock

import pytest

from b2sdk._v2 import *

from .fixtures import *  # pyflakes: disable

BUCKET_NAME_CHARS = string.ascii_letters + string.digits + '-'
BUCKET_NAME_LENGTH = 50
ONE_HOUR_MILLIS = 60 * 60 * 1000
BUCKET_CREATED_AT_MILLIS = 'created_at_millis'


def bucket_name_part(length):
    return ''.join(random.choice(BUCKET_NAME_CHARS) for _ in range(length))


def current_time_millis():
    """
    File times are in integer milliseconds, to avoid roundoff errors.
    """
    return int(round(time.time() * 1000))


def hash_file(path_):
    hash_ = hashlib.sha1()
    with open(path_, 'rb') as file_to_hash:
        while True:
            data = file_to_hash.read(65536)
            if not data:
                break
            hash_.update(data)
    return hash_.digest()


class TestLargeFile:

    general_bucket_name_prefix = 'sdktst'

    @pytest.fixture(autouse=True)
    def save_settings(self, b2_auth_data):
        type(self).b2_auth_data = b2_auth_data

    @classmethod
    def setup_class(cls):
        cls.this_run_bucket_name_prefix = cls.general_bucket_name_prefix + bucket_name_part(8)

    @classmethod
    def teardown_class(cls):
        cls.clean_buckets()

    @pytest.fixture(autouse=True)
    def setup_method(self):
        self.b2_api, self.info = self._authorize(self.b2_auth_data)

    @classmethod
    def _authorize(cls, b2_auth_data):
        info = InMemoryAccountInfo()
        b2_api = B2Api(info)
        b2_api.authorize_account("production", *b2_auth_data)
        return b2_api, info

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
        return self.b2_api.create_bucket(
            self.generate_bucket_name(),
            'allPublic',
            bucket_info={BUCKET_CREATED_AT_MILLIS: str(current_time_millis())}
        )

    def test_large_file(self):
        bucket = self.create_bucket()
        with mock.patch.object(
            self.info, '_recommended_part_size', new=self.info.get_absolute_minimum_part_size()
        ):
            bytes_to_write = int(self.info.get_absolute_minimum_part_size() * 2.5)
            download_manager = self.b2_api.services.download_manager
            with mock.patch.object(
                download_manager,
                'strategies',
                new=[
                    ParallelDownloader(
                        max_streams=download_manager.DEFAULT_MAX_STREAMS,
                        min_part_size=self.info.get_absolute_minimum_part_size(),
                        min_chunk_size=download_manager.MIN_CHUNK_SIZE,
                        max_chunk_size=download_manager.MAX_CHUNK_SIZE,
                    )
                ]
            ):

                # let's check that small file downloads fail with these settings
                bucket.upload_bytes(b'0', 'a_single_zero')
                with pytest.raises(ValueError) as exc_info:
                    with io.BytesIO() as io_:
                        bucket.download_file_by_name('a_single_zero').save(io_)
                assert exc_info.value.args == ('no strategy suitable for download was found!',)

                with TempDir() as temp_dir:
                    temp_dir = pathlib.Path(temp_dir)
                    source_large_file = pathlib.Path(temp_dir) / 'source_large_file'
                    with open(source_large_file, 'wb') as large_file:
                        self.write_zeros(large_file, bytes_to_write)
                    bucket.upload_local_file(source_large_file, 'large_file')
                    target_large_file = pathlib.Path(temp_dir) / 'target_large_file'

                    bucket.download_file_by_name('large_file').save_to(target_large_file)
                    assert hash_file(source_large_file) == hash_file(target_large_file)

    @classmethod
    def _should_remove_bucket(cls, bucket):
        if bucket.name.startswith(cls.this_run_bucket_name_prefix):
            return True
        if bucket.name.startswith(cls.general_bucket_name_prefix):
            if BUCKET_CREATED_AT_MILLIS in bucket.bucket_info:
                if int(bucket.bucket_info[BUCKET_CREATED_AT_MILLIS]
                      ) < current_time_millis() - ONE_HOUR_MILLIS:
                    return True
        return False

    @classmethod
    def clean_buckets(cls):
        b2_api, _ = cls._authorize(cls.b2_auth_data)
        buckets = b2_api.list_buckets()
        for bucket in buckets:
            if not cls._should_remove_bucket(bucket):
                print('Skipping bucket removal:', bucket.name)
            else:
                print('Trying to remove bucket:', bucket.name)
                files_leftover = False
                file_versions = bucket.ls(show_versions=True, recursive=True)
                for file_version_info, _ in file_versions:
                    if file_version_info.file_retention:
                        if file_version_info.file_retention.mode == RetentionMode.GOVERNANCE:
                            print('Removing retention from file version:', file_version_info.id_)
                            b2_api.update_file_retention(
                                file_version_info.id_, file_version_info.file_name,
                                NO_RETENTION_FILE_SETTING, True
                            )
                        elif file_version_info.file_retention.mode == RetentionMode.COMPLIANCE:
                            if file_version_info.file_retention.retain_until > current_time_millis():  # yapf: disable
                                print(
                                    'File version: %s cannot be removed due to compliance mode retention'
                                    % (file_version_info.id_,)
                                )
                                files_leftover = True
                                continue
                        elif file_version_info.file_retention.mode == RetentionMode.NONE:
                            pass
                        else:
                            raise ValueError(
                                'Unknown retention mode: %s' %
                                (file_version_info.file_retention.mode,)
                            )
                    if file_version_info.legal_hold.is_on():
                        print('Removing legal hold from file version:', file_version_info.id_)
                        b2_api.update_file_legal_hold(
                            file_version_info.id_, file_version_info.file_name, LegalHold.OFF
                        )
                    print('Removing file version:', file_version_info.id_)
                    b2_api.delete_file_version(file_version_info.id_, file_version_info.file_name)

                if files_leftover:
                    print('Unable to remove bucket because some retained files remain')
                else:
                    print('Removing bucket:', bucket.name)
                    b2_api.delete_bucket(bucket)
