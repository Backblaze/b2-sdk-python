######################################################################
#
# File: b2sdk/v2/transfer.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk import _v3 as v3


class ParallelDownloader(v3.ParallelDownloader):

    # This method is used in CLI even though it doesn't belong to the public API
    def set_thread_pool_size(self, max_workers: int) -> None:
        self._set_thread_pool_size(max_workers)


class DownloadManager(v3.DownloadManager):
    PARALLEL_DOWNLOADER_CLASS = staticmethod(ParallelDownloader)

    # This method is used in CLI even though it doesn't belong to the public API
    def set_thread_pool_size(self, max_workers: int) -> None:
        for strategy in self.strategies:
            if isinstance(strategy, ParallelDownloader):
                strategy.set_thread_pool_size(max_workers)


class UploadManager(v3.UploadManager):

    # This method is used in SDK even though it doesn't belong to the public API
    def set_thread_pool_size(self, max_workers: int) -> None:
        self._set_thread_pool_size(max_workers)
