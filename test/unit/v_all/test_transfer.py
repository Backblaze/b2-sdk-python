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
        assert download_manager.get_thread_pool_size() is None
        download_manager.set_thread_pool_size(21)
        assert download_manager._thread_pool._max_workers == 21
        assert download_manager.get_thread_pool_size() == 21


class TestUploadManager(TestBase):
    def test_set_thread_pool_size(self) -> None:
        upload_manager = UploadManager(services=Mock())
        assert upload_manager.get_thread_pool_size() is None
        upload_manager.set_thread_pool_size(37)
        assert upload_manager._thread_pool._max_workers == 37
        assert upload_manager.get_thread_pool_size() == 37
