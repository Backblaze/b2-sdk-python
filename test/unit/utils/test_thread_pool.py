######################################################################
#
# File: test/unit/utils/test_thread_pool.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from concurrent.futures import Future

import pytest

from b2sdk._internal.utils.thread_pool import LazyThreadPool


class TestLazyThreadPool:
    @pytest.fixture
    def thread_pool(self):
        return LazyThreadPool()

    def test_submit(self, thread_pool):

        future = thread_pool.submit(sum, (1, 2))
        assert isinstance(future, Future)
        assert future.result() == 3

    def test_set_size(self, thread_pool):
        thread_pool.set_size(10)
        assert thread_pool.get_size() == 10

    def test_get_size(self, thread_pool):
        assert thread_pool.get_size() > 0

    def test_set_size__after_submit(self, thread_pool):
        future = thread_pool.submit(sum, (1, 2))

        thread_pool.set_size(7)
        assert thread_pool.get_size() == 7

        assert future.result() == 3

        assert thread_pool.submit(sum, (1,)).result() == 1
