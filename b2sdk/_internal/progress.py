######################################################################
#
# File: b2sdk/_internal/progress.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import time
from abc import ABCMeta, abstractmethod

from .utils.escape import escape_control_chars

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

    def __init__(self, description: str = ''):
        self.description = description
        self._closed = False

    @abstractmethod
    def set_total_bytes(self, total_byte_count: int) -> None:
        """
        Always called before __enter__ to set the expected total number of bytes.

        May be called more than once if an upload is retried.

        :param total_byte_count: expected total number of bytes
        """

    @abstractmethod
    def bytes_completed(self, byte_count: int) -> None:
        """
        Report the given number of bytes that have been transferred
        so far.  This is not a delta, it is the total number of bytes
        transferred so far.

        Transfer can fail and restart from the beginning, so byte count can
        decrease between calls.

        :param byte_count: number of bytes have been transferred
        """

    def _can_change_description(self) -> bool:
        """
        Determines, on a per-implementation basis, whether the description can be changed at this time.
        """
        return True

    def change_description(self, new_description: str) -> bool:
        """
        Ability to change the description after the listener is started.

        Note: whether the change of description is allowed depends on the implementation.
        The safest option is to change the description before setting the total bytes.

        :param new_description: the new description to be used
        :return: information whether the description was changed
        """
        if not self._can_change_description():
            return False

        self.description = new_description
        return True

    def close(self) -> None:
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

    This listener displays a nice progress bar, but requires `tqdm` package to be installed.
    """

    def __init__(self, *args, **kwargs):
        if tqdm is None:
            raise ModuleNotFoundError("No module named 'tqdm' found")
        self.tqdm = None  # set in set_total_bytes()
        self.prev_value = 0
        super().__init__(*args, **kwargs)

    def set_total_bytes(self, total_byte_count: int) -> None:
        if self.tqdm is None:
            self.tqdm = tqdm(
                desc=escape_control_chars(self.description),
                total=total_byte_count,
                unit='B',
                unit_scale=True,
                leave=True,
                miniters=1,
                smoothing=0.1,
                mininterval=0.2,
            )

    def bytes_completed(self, byte_count: int) -> None:
        # tqdm doesn't support running the progress bar backwards,
        # so on an upload retry, it just won't move until it gets
        # past the point where it failed.
        if self.prev_value < byte_count:
            self.tqdm.update(byte_count - self.prev_value)
            self.prev_value = byte_count

    def _can_change_description(self) -> bool:
        return self.tqdm is None

    def close(self) -> None:
        if self.tqdm is not None:
            self.tqdm.close()
        super().close()


class SimpleProgressListener(AbstractProgressListener):
    """
    Just a simple progress listener which prints info on a console.
    """

    def __init__(self, *args, **kwargs):
        self.complete = 0
        self.last_time = time.time()
        self.any_printed = False
        self.total = 0  # set in set_total_bytes()
        super().__init__(*args, **kwargs)

    def set_total_bytes(self, total_byte_count: int) -> None:
        self.total = total_byte_count

    def bytes_completed(self, byte_count: int) -> None:
        now = time.time()
        elapsed = now - self.last_time
        if 3 <= elapsed and self.total != 0:
            if not self.any_printed:
                print(escape_control_chars(self.description))
            print('     %d%%' % int(100.0 * byte_count / self.total))
            self.last_time = now
            self.any_printed = True

    def _can_change_description(self) -> bool:
        return not self.any_printed

    def close(self) -> None:
        if self.any_printed:
            print('    DONE.')
        super().close()


class DoNothingProgressListener(AbstractProgressListener):
    """
    This listener gives no output whatsoever.
    """

    def set_total_bytes(self, total_byte_count: int) -> None:
        pass

    def bytes_completed(self, byte_count: int) -> None:
        pass


class ProgressListenerForTest(AbstractProgressListener):
    """
    Capture all the calls so they can be checked.
    """

    def __init__(self, *args, **kwargs):
        self.calls = []
        super().__init__(*args, **kwargs)

    def set_total_bytes(self, total_byte_count: int) -> None:
        self.calls.append('set_total_bytes(%d)' % (total_byte_count,))

    def bytes_completed(self, byte_count: int) -> None:
        self.calls.append('bytes_completed(%d)' % (byte_count,))

    def close(self) -> None:
        self.calls.append('close()')
        super().close()

    def get_calls(self) -> list[str]:
        return self.calls


def make_progress_listener(description: str, quiet: bool) -> AbstractProgressListener:
    """
    Produce the best progress listener available for the given parameters.

    :param description: listener description
    :param quiet: if ``True``, do not output anything
    :return: a listener object
    """
    if quiet:
        return DoNothingProgressListener()
    elif tqdm is not None:
        return TqdmProgressListener(description)
    else:
        return SimpleProgressListener(description)
