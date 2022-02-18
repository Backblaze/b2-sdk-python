######################################################################
#
# File: b2sdk/utils/thread_pool.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from concurrent.futures import ThreadPoolExecutor, Future
from threading import Lock
from typing import Optional, Callable

from b2sdk.utils import B2TraceMetaAbstract


class LazyThreadPool(metaclass=B2TraceMetaAbstract):
    """
    Lazily initialized thread pool.
    Can be safely used between threads.
    """

    def __init__(self, max_workers: 'Optional[int]' = None, **kwargs):
        self._lock = Lock()
        self._max_workers = max_workers
        self._thread_pool = None  # type: 'Optional[ThreadPoolExecutor]'
        super().__init__(**kwargs)

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        with self._lock:
            if self._thread_pool is None:
                self._thread_pool = ThreadPoolExecutor(self._max_workers)
            return self._thread_pool.submit(fn, *args, **kwargs)


class LazyThreadPoolMixin(metaclass=B2TraceMetaAbstract):
    """
    Mixin class with lazily initialized ThreadPoolExecutor.
    Can be safely used between threads.
    """
    THREAD_POOL_CLASS = staticmethod(LazyThreadPool)

    def __init__(self, max_workers: 'Optional[int]' = None, **kwargs):
        """
        :param Optional[int] max_workers: maximum number of worker threads
        """

        self._thread_pool = self.THREAD_POOL_CLASS(max_workers=max_workers)
        super().__init__(**kwargs)
