######################################################################
#
# File: b2sdk/utils/thread_pool.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from concurrent.futures import Future, ThreadPoolExecutor
from threading import Lock
from typing import Callable

from b2sdk.utils import B2TraceMetaAbstract


class ThreadPoolMixin(metaclass=B2TraceMetaAbstract):
    """
    Mixin class with ThreadPoolExecutor. Threadsafe.
    """
    THREAD_POOL_CLASS = staticmethod(ThreadPoolExecutor)

    def __init__(self, max_workers: 'Optional[int]' = None, **kwargs):
        """
        :param Optional[int] max_workers: maximum number of worker threads
        """
        self._lock = Lock()
        self._thread_pool = self.THREAD_POOL_CLASS(max_workers=max_workers)
        super().__init__(**kwargs)

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        with self._lock:
            return self._thread_pool.submit(fn, *args, **kwargs)
