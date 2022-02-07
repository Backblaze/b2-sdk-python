######################################################################
#
# File: b2sdk/utils/thread_pool.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Optional

from b2sdk.utils import B2TraceMetaAbstract


class LazyThreadPoolMixin(metaclass=B2TraceMetaAbstract):
    """
    Mixin class with lazily initialized ThreadPoolExecutor.
    Can be safely used between threads.
    """

    def __init__(self, max_workers: 'Optional[int]' = None, **kwargs):
        """
        :param Optional[int] max_workers: maximum number of worker threads
        """

        self._lock = Lock()
        self._thread_pool = None  # type: 'Optional[ThreadPoolExecutor]'
        self._max_workers = max_workers
        super().__init__(**kwargs)

    def _get_thread_pool(self) -> ThreadPoolExecutor:
        with self._lock:
            if self._thread_pool is None:
                self._thread_pool = ThreadPoolExecutor(max_workers=self._max_workers)
        return self._thread_pool
