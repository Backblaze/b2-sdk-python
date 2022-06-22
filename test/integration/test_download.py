######################################################################
#
# File: test/integration/test_download.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import gzip
import io
import pathlib
from unittest import mock

from b2sdk.v2 import *

from .fixtures import *  # pyflakes: disable
from .helpers import authorize
from .base import IntegrationTestBase


class TestDownload(IntegrationTestBase):
    def test_large_file(self):
        bucket = self.create_bucket()
        with mock.patch.object(
            self.info, '_recommended_part_size', new=self.info.get_absolute_minimum_part_size()
        ):
            download_manager = self.b2_api.services.download_manager
            with mock.patch.object(
                download_manager,
                'strategies',
                new=[
                    ParallelDownloader(
                        min_part_size=self.info.get_absolute_minimum_part_size(),
                        min_chunk_size=download_manager.MIN_CHUNK_SIZE,
                        max_chunk_size=download_manager.MAX_CHUNK_SIZE,
                        thread_pool=download_manager._thread_pool,
                    )
                ]
            ):

                # let's check that small file downloads fail with these settings
                bucket.upload_bytes(b'0', 'a_single_zero')
                with pytest.raises(ValueError) as exc_info:
                    with io.BytesIO() as io_:
                        bucket.download_file_by_name('a_single_zero').save(io_)
                assert exc_info.value.args == ('no strategy suitable for download was found!',)
                f = self._file_helper(bucket)
                assert f.download_version.content_sha1_verified

    def _file_helper(self, bucket, sha1_sum=None):
        bytes_to_write = int(self.info.get_absolute_minimum_part_size()) * 2 + 1
        with TempDir() as temp_dir:
            temp_dir = pathlib.Path(temp_dir)
            source_large_file = pathlib.Path(temp_dir) / 'source_large_file'
            with open(source_large_file, 'wb') as large_file:
                self.write_zeros(large_file, bytes_to_write)
            bucket.upload_local_file(
                source_large_file,
                'large_file',
                sha1_sum=sha1_sum,
            )
            target_large_file = pathlib.Path(temp_dir) / 'target_large_file'

            f = bucket.download_file_by_name('large_file')
            f.save_to(target_large_file)
            assert hex_sha1_of_file(source_large_file) == hex_sha1_of_file(target_large_file)
        return f

    def test_small(self):
        bucket = self.create_bucket()
        f = self._file_helper(bucket)
        assert f.download_version.content_sha1_verified

    def test_small_unverified(self):
        bucket = self.create_bucket()
        f = self._file_helper(bucket, sha1_sum='do_not_verify')
        assert not f.download_version.content_sha1_verified

    def test_gzip(self):
        bucket = self.create_bucket()
        with TempDir() as temp_dir:
            temp_dir = pathlib.Path(temp_dir)
            source_file = temp_dir / 'compressed_file.gz'
            downloaded_uncompressed_file = temp_dir / 'downloaded_uncompressed_file'
            downloaded_compressed_file = temp_dir / 'downloaded_compressed_file'

            data_to_write = b"I'm about to be compressed and sent to the cloud, yay!\n" * 100  # too short files failed somehow
            with gzip.open(source_file, 'wb') as gzip_file:
                gzip_file.write(data_to_write)
            file_version = bucket.upload_local_file(
                str(source_file), 'gzipped_file', file_infos={'b2-content-encoding': 'gzip'}
            )
            self.b2_api.download_file_by_id(file_id=file_version.id_).save_to(
                str(downloaded_compressed_file)
            )
            with open(downloaded_compressed_file, 'rb') as dcf:
                downloaded_data = dcf.read()
                with open(source_file, 'rb') as sf:
                    source_data = sf.read()
                    assert downloaded_data == source_data

            decompressing_api, _ = authorize(
                self.b2_auth_data, B2HttpApiConfig(decode_content=True)
            )
            decompressing_api.download_file_by_id(file_id=file_version.id_).save_to(
                str(downloaded_uncompressed_file)
            )
            with open(downloaded_uncompressed_file, 'rb') as duf:
                assert duf.read() == data_to_write
