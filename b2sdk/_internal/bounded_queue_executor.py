######################################################################
#
# File: b2sdk/_internal/bounded_queue_executor.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import threading


class BoundedQueueExecutor:
    """
    Wrap a concurrent.futures.Executor and limits the number of requests that
    can be queued at once.  Requests to submit() tasks block until
    there is room in the queue.

    The number of available slots in the queue is tracked with a
    semaphore that is acquired before queueing an action, and
    released when an action finishes.

    Counts the number of exceptions thrown by tasks, and makes them
    available from get_num_exceptions() after shutting down.
    """

    def __init__(self, executor, queue_limit):
        """
        :param executor: an executor to be wrapped
        :type executor: concurrent.futures.Executor
        :param queue_limit: a queue limit
        :type queue_limit: int
        """
        self.executor = executor
        self.semaphore = threading.Semaphore(queue_limit)
        self.num_exceptions = 0

    def submit(self, fcn, *args, **kwargs):
        """
        Start execution of a callable with the given optional and positional arguments

        :param fcn: a callable object
        :type fcn: callable
        :return: a future object
        :rtype: concurrent.futures.Future
        """
        # Wait until there is room in the queue.
        self.semaphore.acquire()

        # Wrap the action in a function that will release
        # the semaphore after it runs.
        def run_it():
            try:
                return fcn(*args, **kwargs)
            except Exception:
                self.num_exceptions += 1
                raise
            finally:
                self.semaphore.release()

        # Submit the wrapped action.
        return self.executor.submit(run_it)

    def shutdown(self):
        """
        Shut an executor down.
        """
        self.executor.shutdown()

    def get_num_exceptions(self):
        """
        Return a number of exceptions.

        :rtype: int
        """
        return self.num_exceptions
