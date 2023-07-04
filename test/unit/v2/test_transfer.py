######################################################################
#
# File: test/unit/v2/test_transfer.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from unittest.mock import Mock

from ..test_base import TestBase
from .apiver.apiver_deps import DownloadManager, UploadManager


class TestDownloadManager(TestBase):
    def test_set_thread_pool_size(self) -> None:
        download_manager = DownloadManager(services=Mock())
        download_manager.set_thread_pool_size(21)
        self.assertEqual(download_manager._thread_pool._max_workers, 21)


class TestUploadManager(TestBase):
    def test_set_thread_pool_size(self) -> None:
        upload_manager = UploadManager(services=Mock())
        upload_manager.set_thread_pool_size(37)
        self.assertEqual(upload_manager._thread_pool._max_workers, 37)
