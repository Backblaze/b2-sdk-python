######################################################################
#
# File: test/unit/v1/test_bounded_queue_executor.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import concurrent.futures as futures
import threading
import time

from ..test_base import TestBase
from .deps import BoundedQueueExecutor


class TestBoundedQueueExecutor(TestBase):
    def setUp(self):
        unbounded_executor = futures.ThreadPoolExecutor(max_workers=1)
        self.executor = BoundedQueueExecutor(unbounded_executor, 1)

    def tearDown(self):
        self.executor.shutdown()

    def test_return_future(self):
        future_1 = self.executor.submit(lambda: 1)
        print(future_1)
        self.assertEqual(1, future_1.result())

    def test_blocking(self):
        # This doesn't actually test that it waits, but it does exercise the code.

        # Make some futures using a function that takes a little time.
        def sleep_and_return_fcn(n):
            def fcn():
                time.sleep(0.01)
                return n

            return fcn

        futures = [self.executor.submit(sleep_and_return_fcn(i)) for i in range(10)]

        # Check the answers
        answers = list(map(lambda f: f.result(), futures))
        self.assertEqual(list(range(10)), answers)

    def test_submit_blocks_until_a_slot_is_free(self):
        """With a queue limit of 1, a second submit() must wait for the first job to finish."""
        release_first_job = threading.Event()
        first_job_started = threading.Event()

        def first_job():
            first_job_started.set()
            release_first_job.wait(timeout=10)

        self.executor.submit(first_job)
        assert first_job_started.wait(timeout=10)

        second_submit_returned = threading.Event()

        def submit_second_job():
            self.executor.submit(lambda: None)
            second_submit_returned.set()

        # daemon thread: it must not keep the test process alive if submit() blocks for good
        submitter = threading.Thread(target=submit_second_job, daemon=True)
        submitter.start()
        try:
            self.assertFalse(second_submit_returned.wait(timeout=0.3), 'submit() did not block')
        finally:
            release_first_job.set()
        self.assertTrue(second_submit_returned.wait(timeout=10), 'submit() never returned')

    def test_no_exceptions(self):
        f = self.executor.submit(lambda: 1)
        self.executor.shutdown()
        self.assertEqual(0, self.executor.get_num_exceptions())
        self.assertTrue(f.exception() is None)

    def test_two_exceptions(self):
        def thrower():
            raise Exception('test_exception')

        f1 = self.executor.submit(thrower)
        f2 = self.executor.submit(thrower)
        self.executor.shutdown()
        self.assertEqual(2, self.executor.get_num_exceptions())
        self.assertFalse(f1.exception() is None)
        self.assertEqual('test_exception', str(f2.exception()))
