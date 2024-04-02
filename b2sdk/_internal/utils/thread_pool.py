######################################################################
#
# File: b2sdk/_internal/utils/thread_pool.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable

try:
    from typing_extensions import Protocol
except ImportError:
    from typing import Protocol

from b2sdk._internal.utils import B2TraceMetaAbstract


class DynamicThreadPoolExecutorProtocol(Protocol):
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        ...

    def set_size(self, max_workers: int) -> None:
        """Set the size of the thread pool."""

    def get_size(self) -> int:
        """Return the current size of the thread pool."""


class LazyThreadPool:
    """
    Lazily initialized thread pool.
    """

    _THREAD_POOL_FACTORY = ThreadPoolExecutor

    def __init__(self, max_workers: int | None = None, **kwargs):
        if max_workers is None:
            max_workers = min(
                32, (os.cpu_count() or 1) + 4
            )  # same default as in ThreadPoolExecutor
        self._max_workers = max_workers
        self._thread_pool: ThreadPoolExecutor | None = None
        super().__init__(**kwargs)

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        if self._thread_pool is None:
            self._thread_pool = self._THREAD_POOL_FACTORY(self._max_workers)
        return self._thread_pool.submit(fn, *args, **kwargs)

    def set_size(self, max_workers: int) -> None:
        """
        Set the size of the thread pool.

        This operation will block until all tasks in the current thread pool are completed.

        :param max_workers: New size of the thread pool
        :return: None
        """
        if self._max_workers == max_workers:
            return
        old_thread_pool = self._thread_pool
        self._thread_pool = self._THREAD_POOL_FACTORY(max_workers=max_workers)
        if old_thread_pool is not None:
            old_thread_pool.shutdown(wait=True)
        self._max_workers = max_workers

    def get_size(self) -> int:
        """Return the current size of the thread pool."""
        return self._max_workers


class ThreadPoolMixin(metaclass=B2TraceMetaAbstract):
    """
    Mixin class with ThreadPoolExecutor.
    """

    DEFAULT_THREAD_POOL_CLASS = LazyThreadPool

    def __init__(
        self,
        thread_pool: DynamicThreadPoolExecutorProtocol | None = None,
        max_workers: int | None = None,
        **kwargs,
    ):
        """
        :param thread_pool: thread pool to be used
        :param max_workers: maximum number of worker threads (ignored if thread_pool is not None)
        """
        self._thread_pool = (
            thread_pool
            if thread_pool is not None else self.DEFAULT_THREAD_POOL_CLASS(max_workers=max_workers)
        )
        self._max_workers = max_workers
        super().__init__(**kwargs)

    def set_thread_pool_size(self, max_workers: int) -> None:
        """
        Set the size of the thread pool.

        This operation will block until all tasks in the current thread pool are completed.

        :param max_workers: New size of the thread pool
        :return: None
        """
        return self._thread_pool.set_size(max_workers)

    def get_thread_pool_size(self) -> int:
        return self._thread_pool.get_size()
