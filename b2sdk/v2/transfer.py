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


class LazyThreadPoolMixin(v3.LazyThreadPoolMixin):
    # This method is used in CLI even though it doesn't belong to the public API
    def set_thread_pool_size(self, max_workers: int) -> None:
        with self._lock:
            if self._thread_pool is not None:
                raise RuntimeError('Thread pool already created')
            self._max_workers = max_workers


class DownloadManager(v3.DownloadManager, LazyThreadPoolMixin):
    pass


class UploadManager(v3.UploadManager, LazyThreadPoolMixin):
    pass
