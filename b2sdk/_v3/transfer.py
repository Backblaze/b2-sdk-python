######################################################################
#
# File: b2sdk/_v3/transfer.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk import transfer
from b2sdk.transfer.inbound.downloader import parallel


class ParallelDownloader(parallel.ParallelDownloader):

    # This method is used in SDK even though it doesn't belong to the public API
    def set_thread_pool_size(self, max_workers: int) -> None:
        self._set_thread_pool_size(max_workers)


class DownloadManager(transfer.DownloadManager):
    PARALLEL_DOWNLOADER_CLASS = staticmethod(ParallelDownloader)

    # This method is used in SDK even though it doesn't belong to the public API
    def set_thread_pool_size(self, max_workers: int) -> None:
        for strategy in self.strategies:
            if isinstance(strategy, ParallelDownloader):
                strategy.set_thread_pool_size(max_workers)


class UploadManager(transfer.UploadManager):

    # This method is used in SDK even though it doesn't belong to the public API
    def set_thread_pool_size(self, max_workers: int) -> None:
        self._set_thread_pool_size(max_workers)
