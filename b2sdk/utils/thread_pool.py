from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Optional

from b2sdk.utils import B2TraceMetaAbstract


class LazyThreadPoolMixin(metaclass=B2TraceMetaAbstract):
    """
    TODO: Add docstring
    """

    def __init__(self, max_workers: 'Optional[int]' = None) -> None:
        """
        :param Optional[int] max_workers: maximum number of worker threads
        """

        self._lock = Lock()
        self._thread_pool: 'Optional[ThreadPoolExecutor]' = None
        self._max_workers = max_workers

    def set_thread_pool_size(self, max_workers: int) -> None:
        """
        Set the size of the thread pool.

        Must be called before any work starts, or the thread pool will get
        the default size.

        :param int max_workers: maximum number of worker threads
        """
        with self._lock:
            if self._thread_pool is not None:
                raise RuntimeError('Thread pool already created')
            self._max_workers = max_workers

    def get_thread_pool(self) -> ThreadPoolExecutor:
        """
        Return the thread pool executor.
        """
        with self._lock:
            if self._thread_pool is None:
                self._thread_pool = ThreadPoolExecutor(max_workers=self._max_workers)
        return self._thread_pool
