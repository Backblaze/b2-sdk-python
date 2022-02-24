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
from typing import Optional

from b2sdk.utils import B2TraceMetaAbstract


class ThreadPoolMixin(metaclass=B2TraceMetaAbstract):
    """
    Mixin class with ThreadPoolExecutor.
    """
    DEFAULT_THREAD_POOL_CLASS = staticmethod(ThreadPoolExecutor)

    def __init__(
        self,
        thread_pool: Optional[ThreadPoolExecutor] = None,
        max_workers: Optional[int] = None,
        **kwargs
    ):
        """
        :param thread_pool: thread pool to be used
        :param max_workers: maximum number of worker threads (ignored if thread_pool is not None)
        """
        self._thread_pool = thread_pool if thread_pool is not None \
            else self.DEFAULT_THREAD_POOL_CLASS(max_workers=max_workers)
        super().__init__(**kwargs)
