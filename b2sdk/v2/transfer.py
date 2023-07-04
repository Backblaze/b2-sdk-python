######################################################################
#
# File: b2sdk/v2/transfer.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable

from b2sdk import _v3 as v3


class LazyThreadPool:
    """
    Lazily initialized thread pool.
    """

    def __init__(self, max_workers: int | None = None, **kwargs):
        self._max_workers = max_workers
        self._thread_pool = None  # type: 'Optional[ThreadPoolExecutor]'
        super().__init__(**kwargs)

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        if self._thread_pool is None:
            self._thread_pool = ThreadPoolExecutor(self._max_workers)
        return self._thread_pool.submit(fn, *args, **kwargs)

    def set_size(self, max_workers: int) -> None:
        if self._max_workers == max_workers:
            return
        if self._thread_pool is not None:
            raise RuntimeError('Thread pool already created')
        self._max_workers = max_workers


class ThreadPoolMixin(v3.ThreadPoolMixin):
    DEFAULT_THREAD_POOL_CLASS = staticmethod(LazyThreadPool)

    # This method is used in CLI even though it doesn't belong to the public API
    def set_thread_pool_size(self, max_workers: int) -> None:
        self._thread_pool.set_size(max_workers)


class DownloadManager(v3.DownloadManager, ThreadPoolMixin):
    pass


class UploadManager(v3.UploadManager, ThreadPoolMixin):
    pass
