######################################################################
#
# File: test/unit/v1/test_download_dest.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import os
import tempfile

from ..test_base import TestBase
from .deps import (
    DownloadDestLocalFile,
    DownloadDestProgressWrapper,
    PreSeekedDownloadDest,
    ProgressListenerForTest,
)


class TestDownloadDestLocalFile(TestBase):
    expected_result = 'hello world'

    def _make_dest(self, temp_dir):
        file_path = os.path.join(temp_dir, "test.txt")
        return DownloadDestLocalFile(file_path), file_path

    def test_write_and_set_mod_time(self):
        """
        Check that the file gets written and that its mod time gets set.
        """
        mod_time = 1500222333000
        with tempfile.TemporaryDirectory() as temp_dir:
            download_dest, file_path = self._make_dest(temp_dir)
            with download_dest.make_file_context(
                "file_id", "file_name", 100, "content_type", "sha1", {}, mod_time
            ) as f:
                f.write(b'hello world')
            with open(file_path, 'rb') as f:
                self.assertEqual(
                    self.expected_result.encode(),
                    f.read(),
                )
            self.assertEqual(mod_time, int(os.path.getmtime(file_path) * 1000))

    def test_failed_write_deletes_partial_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            download_dest, file_path = self._make_dest(temp_dir)
            try:
                with download_dest.make_file_context(
                    "file_id", "file_name", 100, "content_type", "sha1", {}, 1500222333000
                ) as f:
                    f.write(b'hello world')
                    raise Exception('test error')
            except Exception as e:
                self.assertEqual('test error', str(e))
            self.assertFalse(os.path.exists(file_path), msg='failed download should be deleted')


class TestPreSeekedDownloadDest(TestDownloadDestLocalFile):
    expected_result = '123hello world567890'

    def _make_dest(self, temp_dir):
        file_path = os.path.join(temp_dir, "test.txt")
        with open(file_path, 'wb') as f:
            f.write(b'12345678901234567890')
        return PreSeekedDownloadDest(local_file_path=file_path, seek_target=3), file_path


class TestDownloadDestProgressWrapper(TestBase):
    def test_write_and_set_mod_time_and_progress(self):
        """
        Check that the file gets written and that its mod time gets set.
        """
        mod_time = 1500222333000
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "test.txt")
            download_local_file = DownloadDestLocalFile(file_path)
            progress_listener = ProgressListenerForTest()
            download_dest = DownloadDestProgressWrapper(download_local_file, progress_listener)
            with download_dest.make_file_context(
                "file_id", "file_name", 100, "content_type", "sha1", {}, mod_time
            ) as f:
                f.write(b'hello world\n')
            with open(file_path, 'rb') as f:
                self.assertEqual(b'hello world\n', f.read())
            self.assertEqual(mod_time, int(os.path.getmtime(file_path) * 1000))
            self.assertEqual(
                [
                    'set_total_bytes(100)',
                    'bytes_completed(12)',
                    'close()',
                ],
                progress_listener.get_calls(),
            )
