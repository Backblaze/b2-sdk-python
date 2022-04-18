######################################################################
#
# File: test/integration/test_download.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import io
import pathlib
from unittest import mock

import pytest

from b2sdk.v2 import *

from .bucket_cleaner import BucketCleaner
from .fixtures import *  # pyflakes: disable
from .helpers import GENERAL_BUCKET_NAME_PREFIX
from .base import IntegrationTestBase

#class TestDownload(IntegrationTestBase):
#    def test_large_file(self):
#        bucket = self.create_bucket()
#        with mock.patch.object(
#            self.info, '_recommended_part_size', new=self.info.get_absolute_minimum_part_size()
#        ):
#            download_manager = self.b2_api.services.download_manager
#            with mock.patch.object(
#                download_manager,
#                'strategies',
#                new=[
#                    ParallelDownloader(
#                        min_part_size=self.info.get_absolute_minimum_part_size(),
#                        min_chunk_size=download_manager.MIN_CHUNK_SIZE,
#                        max_chunk_size=download_manager.MAX_CHUNK_SIZE,
#                        thread_pool=download_manager._thread_pool,
#                    )
#                ]
#            ):
#
#                # let's check that small file downloads fail with these settings
#                bucket.upload_bytes(b'0', 'a_single_zero')
#                with pytest.raises(ValueError) as exc_info:
#                    with io.BytesIO() as io_:
#                        bucket.download_file_by_name('a_single_zero').save(io_)
#                assert exc_info.value.args == ('no strategy suitable for download was found!',)
#                f = self._file_helper(bucket)
#                assert f.download_version.content_sha1_verified
#
#    def _file_helper(self, bucket, sha1_sum=None):
#        bytes_to_write = int(self.info.get_absolute_minimum_part_size() * 2.5)
#        with TempDir() as temp_dir:
#            temp_dir = pathlib.Path(temp_dir)
#            source_large_file = pathlib.Path(temp_dir) / 'source_large_file'
#            with open(source_large_file, 'wb') as large_file:
#                self.write_zeros(large_file, bytes_to_write)
#            bucket.upload_local_file(
#                source_large_file,
#                'large_file',
#                sha1_sum='do_not_verify',
#            )
#            target_large_file = pathlib.Path(temp_dir) / 'target_large_file'
#
#            f = bucket.download_file_by_name('large_file')
#            f.save_to(target_large_file)
#            assert hex_sha1_of_file(source_large_file) == hex_sha1_of_file(target_large_file)
#        return f
#
#    def test_small(self):
#        bucket = self.create_bucket()
#        f = self._file_helper(bucket)
#        assert not f.download_version.content_sha1_verified
#
#    def test_small_unverified(self):
#        bucket = self.create_bucket()
#        f = self._file_helper(bucket, sha1_sum='do_not_verify')
#        assert not f.download_version.content_sha1_verified
