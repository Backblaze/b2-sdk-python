######################################################################
#
# File: test/unit/v_all/test_transfer.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from unittest.mock import Mock

from apiver_deps import DownloadManager, UploadManager

from ..test_base import TestBase


class TestDownloadManager(TestBase):
    def test_set_thread_pool_size(self) -> None:
        download_manager = DownloadManager(services=Mock())
        assert download_manager.get_thread_pool_size() > 0

        pool_size = 21
        download_manager.set_thread_pool_size(pool_size)
        assert download_manager.get_thread_pool_size() == pool_size


class TestUploadManager(TestBase):
    def test_set_thread_pool_size(self) -> None:
        upload_manager = UploadManager(services=Mock())
        assert upload_manager.get_thread_pool_size() > 0

        pool_size = 37
        upload_manager.set_thread_pool_size(pool_size)
        assert upload_manager.get_thread_pool_size() == pool_size
