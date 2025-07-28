######################################################################
#
# File: test/integration/test_download.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import gzip
import io
import os
import pathlib
import platform
import tempfile
from pprint import pprint
from unittest import mock

import pytest

from b2sdk._internal.utils import Sha1HexDigest
from b2sdk._internal.utils.filesystem import _IS_WINDOWS
from b2sdk.v3 import *
from b2sdk._internal.testing.helpers.base import IntegrationTestBase
from b2sdk._internal.testing.helpers.api import authorize


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
                ],
            ):
                # let's check that small file downloads do not fail with these settings
                small_file_version = bucket.upload_bytes(b'0', 'a_single_char')
                with io.BytesIO() as io_:
                    bucket.download_file_by_name('a_single_char').save(io_)
                    assert io_.getvalue() == b'0'

                f, sha1 = self._file_helper(bucket)
                if small_file_version._type() != 'large':
                    # if we are here, that's not the production server!
                    assert (
                        f.download_version.content_sha1_verified
                    )  # large files don't have sha1, lets not check

                file_info = f.download_version.file_info
                assert LARGE_FILE_SHA1 in file_info
                assert file_info[LARGE_FILE_SHA1] == sha1

    def _file_helper(
        self, bucket, sha1_sum=None, bytes_to_write: int | None = None
    ) -> tuple[DownloadVersion, Sha1HexDigest]:
        bytes_to_write = bytes_to_write or int(self.info.get_absolute_minimum_part_size()) * 2 + 1
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = pathlib.Path(temp_dir)
            source_small_file = pathlib.Path(temp_dir) / 'source_small_file'
            with open(source_small_file, 'wb') as small_file:
                self.write_zeros(small_file, bytes_to_write)
            bucket.upload_local_file(
                source_small_file,
                'small_file',
                sha1_sum=sha1_sum,
            )
            target_small_file = pathlib.Path(temp_dir) / 'target_small_file'

            f = bucket.download_file_by_name('small_file')
            f.save_to(target_small_file)

            source_sha1 = hex_sha1_of_file(source_small_file)
            assert source_sha1 == hex_sha1_of_file(target_small_file)
        return f, source_sha1

    def test_small(self):
        bucket = self.create_bucket()
        f, _ = self._file_helper(bucket, bytes_to_write=1)
        assert f.download_version.content_sha1_verified

    def test_small_unverified(self):
        bucket = self.create_bucket()
        f, _ = self._file_helper(bucket, sha1_sum='do_not_verify', bytes_to_write=1)
        if f.download_version.content_sha1_verified:
            pprint(f.download_version._get_args_for_clone())
            assert not f.download_version.content_sha1_verified


@pytest.mark.parametrize('size_multiplier', [1, 100])
def test_gzip(b2_auth_data, bucket, tmp_path, b2_api, size_multiplier):
    """Test downloading gzipped files of varius sizes with and without content-encoding."""
    source_file = tmp_path / 'compressed_file.gz'
    downloaded_uncompressed_file = tmp_path / 'downloaded_uncompressed_file'
    downloaded_compressed_file = tmp_path / 'downloaded_compressed_file'

    data_to_write = b"I'm about to be compressed and sent to the cloud, yay!\n" * size_multiplier
    source_file.write_bytes(gzip.compress(data_to_write))
    file_version = bucket.upload_local_file(
        str(source_file), 'gzipped_file', file_info={'b2-content-encoding': 'gzip'}
    )
    b2_api.download_file_by_id(file_id=file_version.id_).save_to(str(downloaded_compressed_file))
    assert downloaded_compressed_file.read_bytes() == source_file.read_bytes()

    decompressing_api, _ = authorize(b2_auth_data, B2HttpApiConfig(decode_content=True))
    decompressing_api.download_file_by_id(file_id=file_version.id_).save_to(
        str(downloaded_uncompressed_file)
    )
    assert downloaded_uncompressed_file.read_bytes() == data_to_write


@pytest.fixture
def source_file(tmp_path):
    source_file = tmp_path / 'source.txt'
    source_file.write_text('hello world')
    return source_file


@pytest.fixture
def uploaded_source_file_version(bucket, source_file):
    file_version = bucket.upload_local_file(str(source_file), source_file.name)
    return file_version


@pytest.mark.skipif(platform.system() == 'Windows', reason='no os.mkfifo() on Windows')
def test_download_to_fifo(bucket, tmp_path, source_file, uploaded_source_file_version, bg_executor):
    output_file = tmp_path / 'output.txt'
    os.mkfifo(output_file)
    output_string = None

    def reader():
        nonlocal output_string
        output_string = output_file.read_text()

    reader_future = bg_executor.submit(reader)

    bucket.download_file_by_id(file_id=uploaded_source_file_version.id_).save_to(output_file)

    reader_future.result(timeout=1)
    assert source_file.read_text() == output_string


@pytest.fixture
def binary_cap(request):
    """
    Get best suited capture.

    For Windows we need capsys as capfd fails, while on any other (i.e. POSIX systems) we need capfd.
    This is sadly tied directly to how .save_to() is implemented, as Windows required special handling.
    """
    cap = request.getfixturevalue('capsysbinary' if _IS_WINDOWS else 'capfdbinary')
    yield cap


def test_download_to_stdout(bucket, source_file, uploaded_source_file_version, binary_cap):
    output_file = 'CON' if _IS_WINDOWS else '/dev/stdout'

    bucket.download_file_by_id(file_id=uploaded_source_file_version.id_).save_to(output_file)

    assert binary_cap.readouterr().out == source_file.read_bytes()
