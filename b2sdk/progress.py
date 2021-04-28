######################################################################
#
# File: b2sdk/progress.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import ABCMeta, abstractmethod
import time

try:
    from tqdm import tqdm  # displays a nice progress bar
except ImportError:
    tqdm = None  # noqa


class AbstractProgressListener(metaclass=ABCMeta):
    """
    Interface expected by B2Api upload and download methods to report
    on progress.

    This interface just accepts the number of bytes transferred so far.
    Subclasses will need to know the total size if they want to report
    a percent done.
    """

    def __init__(self):
        self._closed = False

    @abstractmethod
    def set_total_bytes(self, total_byte_count):
        """
        Always called before __enter__ to set the expected total number of bytes.

        May be called more than once if an upload is retried.

        :param int total_byte_count: expected total number of bytes
        """

    @abstractmethod
    def bytes_completed(self, byte_count):
        """
        Report the given number of bytes that have been transferred
        so far.  This is not a delta, it is the total number of bytes
        transferred so far.

        Transfer can fail and restart from beginning so byte count can
        decrease between calls.

        :param int byte_count: number of bytes have been transferred
        """

    def close(self):
        """
        Must be called when you're done with the listener.
        In well-structured code, should be called only once.
        """
        #import traceback, sys; traceback.print_stack(file=sys.stdout)
        assert self._closed is False, 'progress listener was closed twice! uncomment the line above to debug this'
        self._closed = True

    def __enter__(self):
        """
        A standard context manager method.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        A standard context manager method.
        """
        self.close()


class TqdmProgressListener(AbstractProgressListener):
    """
    Progress listener based on tqdm library.
    """

    def __init__(self, description, *args, **kwargs):
        self.description = description
        self.tqdm = None  # set in set_total_bytes()
        self.prev_value = 0
        super(TqdmProgressListener, self).__init__(*args, **kwargs)

    def set_total_bytes(self, total_byte_count):
        if self.tqdm is None:
            self.tqdm = tqdm(
                desc=self.description,
                total=total_byte_count,
                unit='B',
                unit_scale=True,
                leave=True,
                miniters=1,
                smoothing=0.1,
                mininterval=0.2,
            )

    def bytes_completed(self, byte_count):
        # tqdm doesn't support running the progress bar backwards,
        # so on an upload retry, it just won't move until it gets
        # past the point where it failed.
        if self.prev_value < byte_count:
            self.tqdm.update(byte_count - self.prev_value)
            self.prev_value = byte_count

    def close(self):
        if self.tqdm is not None:
            self.tqdm.close()
        super(TqdmProgressListener, self).close()


class SimpleProgressListener(AbstractProgressListener):
    """
    Just a simple progress listener which prints info on a console.
    """

    def __init__(self, description, *args, **kwargs):
        self.desc = description
        self.complete = 0
        self.last_time = time.time()
        self.any_printed = False
        super(SimpleProgressListener, self).__init__(*args, **kwargs)

    def set_total_bytes(self, total_byte_count):
        self.total = total_byte_count

    def bytes_completed(self, byte_count):
        now = time.time()
        elapsed = now - self.last_time
        if 3 <= elapsed and self.total != 0:
            if not self.any_printed:
                print(self.desc)
            print('     %d%%' % int(100.0 * byte_count / self.total))
            self.last_time = now
            self.any_printed = True

    def close(self):
        if self.any_printed:
            print('    DONE.')
        super(SimpleProgressListener, self).close()


class DoNothingProgressListener(AbstractProgressListener):
    """
    This listener gives no output whatsoever.
    """

    def set_total_bytes(self, total_byte_count):
        pass

    def bytes_completed(self, byte_count):
        pass


class ProgressListenerForTest(AbstractProgressListener):
    """
    Capture all of the calls so they can be checked.
    """

    def __init__(self, *args, **kwargs):
        self.calls = []
        super(ProgressListenerForTest, self).__init__(*args, **kwargs)

    def set_total_bytes(self, total_byte_count):
        self.calls.append('set_total_bytes(%d)' % (total_byte_count,))

    def bytes_completed(self, byte_count):
        self.calls.append('bytes_completed(%d)' % (byte_count,))

    def close(self):
        self.calls.append('close()')
        super(ProgressListenerForTest, self).close()

    def get_calls(self):
        return self.calls


def make_progress_listener(description, quiet):
    """
    Return a progress listener object depending on some conditions.

    :param str description: listener description
    :param bool quiet: if ``True``, do not output anything
    :return: a listener object
    """
    if quiet:
        return DoNothingProgressListener()
    elif tqdm is not None:
        return TqdmProgressListener(description)
    else:
        return SimpleProgressListener(description)
